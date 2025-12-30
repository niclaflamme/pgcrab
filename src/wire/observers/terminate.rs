use std::error::Error as StdError;
use std::fmt;

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};
// -----------------------------------------------------------------------------
// ----- TerminateFrameObserver ------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct TerminateFrameObserver<'a> {
    _frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- TerminateFrameObserver: Static ----------------------------------------

impl<'a> TerminateFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        let meta = peek_tagged_frame(buf, b'X')?;
        if meta.len != 4 {
            return None;
        }
        Some(meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewTerminateObserverError> {
        let meta = match parse_tagged_frame(frame, b'X') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewTerminateObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewTerminateObserverError::UnexpectedLength);
            }
        };

        if meta.len != 4 {
            return Err(NewTerminateObserverError::UnexpectedLength);
        }

        Ok(Self { _frame: frame })
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewTerminateObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewTerminateObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewTerminateObserverError::*;
        match self {
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewTerminateObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame() -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'X');
        frame.put_u32(4);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame();
        let len = TerminateFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let _obs = TerminateFrameObserver::new(&frame[..len]).unwrap();
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame();
        frame.pop();
        assert!(TerminateFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame();
        with_junk.push(0);
        let err = TerminateFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewTerminateObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'X');
        frame.put_u32(5); // wrong length
        frame.to_vec();
        let err = TerminateFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewTerminateObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag() {
        let mut frame = build_frame();
        frame[0] = b'Y';
        let err = TerminateFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewTerminateObserverError::UnexpectedTag(b'Y'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame();
        let f2 = build_frame();
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = TerminateFrameObserver::peek(&stream).unwrap();
        let _obs1 = TerminateFrameObserver::new(&stream[..t1]).unwrap();
        // frame 2
        let t2 = TerminateFrameObserver::peek(&stream[t1..]).unwrap();
        let _obs2 = TerminateFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame();
        let total = TerminateFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _obs = TerminateFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let tag_ptr = &frame_slice[0] as *const u8 as usize;
        assert!(tag_ptr >= base && tag_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
