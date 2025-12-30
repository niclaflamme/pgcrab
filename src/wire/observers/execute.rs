use memchr::memchr;
use std::{fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- ExecuteFrameObserver --------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct ExecuteFrameObserver<'a> {
    _frame: &'a [u8],
    portal: &'a str,
    max_rows: i32,
}

// -----------------------------------------------------------------------------
// ----- ExecuteFrameObserver: Static ------------------------------------------

impl<'a> ExecuteFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'E').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewExecuteObserverError> {
        let meta = match parse_tagged_frame(frame, b'E') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewExecuteObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewExecuteObserverError::UnexpectedLength);
            }
        };
        let mut pos = 5;
        // portal
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewExecuteObserverError::UnexpectedEof)?;
        let portal =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewExecuteObserverError::InvalidUtf8)?;
        pos += rel + 1;
        // max_rows
        if pos + 4 > meta.total_len {
            return Err(NewExecuteObserverError::UnexpectedEof);
        }
        let max_rows = be_i32(&frame[pos..]);
        pos += 4;
        if pos != meta.total_len {
            return Err(NewExecuteObserverError::UnexpectedLength);
        }
        Ok(Self {
            _frame: frame,
            portal,
            max_rows,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- ExecuteFrameObserver: Public ------------------------------------------

impl<'a> ExecuteFrameObserver<'a> {
    #[inline]
    pub fn portal(&self) -> &'a str {
        self.portal
    }

    #[inline]
    pub fn max_rows(&self) -> i32 {
        self.max_rows
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewExecuteObserverError {
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewExecuteObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewExecuteObserverError::*;
        match self {
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewExecuteObserverError {}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn be_i32(x: &[u8]) -> i32 {
    i32::from_be_bytes([x[0], x[1], x[2], x[3]])
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(portal: &str, max_rows: i32) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(portal.as_bytes());
        body.put_u8(0);
        body.put_i32(max_rows);
        let mut frame = BytesMut::new();
        frame.put_u8(b'E');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_minimal() {
        let frame = build_frame("", 0);
        let len = ExecuteFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = ExecuteFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.portal(), "");
        assert_eq!(obs.max_rows(), 0);
    }

    #[test]
    fn peek_then_new_with_portal_and_max_rows() {
        let frame = build_frame("p1", 100);
        let len = ExecuteFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = ExecuteFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.portal(), "p1");
        assert_eq!(obs.max_rows(), 100);
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'E');
        frame.put_u32(10);
        frame.extend_from_slice(&[0xFF, 0xFE]);
        frame.put_u8(0);
        frame.put_i32(0);
        let frame = frame.to_vec();
        let err = ExecuteFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewExecuteObserverError::InvalidUtf8(_));
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'E');
        frame.put_u32(10);
        frame.extend_from_slice(b"p1");
        // no nul, incomplete
        let frame = frame.to_vec();
        let err = ExecuteFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewExecuteObserverError::UnexpectedLength);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame("p1", 100);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(ExecuteFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = ExecuteFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewExecuteObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_execute() {
        let bogus = vec![b'X', 0, 0, 0, 9, 0, 0, 0, 0, 0];
        assert!(ExecuteFrameObserver::peek(&bogus).is_none());
        let err = ExecuteFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewExecuteObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn non_ascii_portal() {
        let portal = "ポータル";
        let frame = build_frame(portal, 0);
        let total = ExecuteFrameObserver::peek(&frame).unwrap();
        let obs = ExecuteFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.portal(), portal);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("p1", 100);
        let f2 = build_frame("p2", 200);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = ExecuteFrameObserver::peek(&stream).unwrap();
        let obs1 = ExecuteFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.portal(), "p1");
        assert_eq!(obs1.max_rows(), 100);
        // frame 2
        let t2 = ExecuteFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = ExecuteFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.portal(), "p2");
        assert_eq!(obs2.max_rows(), 200);
    }

    #[test]
    fn zero_copy_portal_aliases_frame_memory() {
        let frame = build_frame("p1", 100);
        let total = ExecuteFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = ExecuteFrameObserver::new(frame_slice).unwrap();
        let p = obs.portal();
        let base = frame_slice.as_ptr() as usize;
        let ptr = p.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
