use memchr::memchr;
use std::{fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- CopyFailFrameObserver -------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct CopyFailFrameObserver<'a> {
    _frame: &'a [u8],

    message: &'a str,
}

// -----------------------------------------------------------------------------
// ----- CopyFailFrameObserver: Static -----------------------------------------

impl<'a> CopyFailFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'f').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewCopyFailObserverError> {
        let meta = match parse_tagged_frame(frame, b'f') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewCopyFailObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewCopyFailObserverError::UnexpectedLength);
            }
        };

        let mut pos = 5;

        // message
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewCopyFailObserverError::UnexpectedEof)?;
        let message = str::from_utf8(&frame[pos..pos + rel])
            .map_err(NewCopyFailObserverError::InvalidUtf8)?;
        pos += rel + 1;

        if pos != meta.total_len {
            return Err(NewCopyFailObserverError::UnexpectedLength);
        }

        Ok(Self {
            _frame: frame,
            message,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- CopyFailFrameObserver: Public -----------------------------------------

impl<'a> CopyFailFrameObserver<'a> {
    #[inline]
    pub fn message(&self) -> &'a str {
        self.message
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewCopyFailObserverError {
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewCopyFailObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewCopyFailObserverError::*;
        match self {
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewCopyFailObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};
    fn build_frame(message: &str) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(message.as_bytes());
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'f');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty_message() {
        let frame = build_frame("");
        let len = CopyFailFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = CopyFailFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.message(), "");
    }

    #[test]
    fn peek_then_new_with_message() {
        let message = "error occurred";
        let frame = build_frame(message);
        let len = CopyFailFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = CopyFailFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.message(), message);
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'f');
        frame.put_u32(6);
        frame.extend_from_slice(&[0xFF, 0xFE]);
        frame.put_u8(0);
        let frame = frame.to_vec();
        let err = CopyFailFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCopyFailObserverError::InvalidUtf8(_));
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'f');
        frame.put_u32(6);
        frame.extend_from_slice(b"ab");
        let frame = frame.to_vec();
        let err = CopyFailFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCopyFailObserverError::UnexpectedLength);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame("error");
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(CopyFailFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = CopyFailFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewCopyFailObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_copy_fail() {
        let bogus = vec![b'X', 0, 0, 0, 5, 0];
        assert!(CopyFailFrameObserver::peek(&bogus).is_none());
        let err = CopyFailFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewCopyFailObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn non_ascii_message() {
        let message = "エラー";
        let frame = build_frame(message);
        let total = CopyFailFrameObserver::peek(&frame).unwrap();
        let obs = CopyFailFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.message(), message);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("error1");
        let f2 = build_frame("error2");
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = CopyFailFrameObserver::peek(&stream).unwrap();
        let obs1 = CopyFailFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.message(), "error1");
        // frame 2
        let t2 = CopyFailFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = CopyFailFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.message(), "error2");
    }

    #[test]
    fn zero_copy_message_aliases_frame_memory() {
        let frame = build_frame("error");
        let total = CopyFailFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = CopyFailFrameObserver::new(frame_slice).unwrap();
        let m = obs.message();
        let base = frame_slice.as_ptr() as usize;
        let ptr = m.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
