use std::error::Error as StdError;
use std::fmt;

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};
// -----------------------------------------------------------------------------
// ----- FlushFrameObserver ----------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct FlushFrameObserver<'a> {
    _frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- FlushFrameObserver: Static --------------------------------------------

impl<'a> FlushFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        let meta = peek_tagged_frame(buf, b'H')?;
        if meta.len != 4 {
            return None;
        }
        Some(meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewFlushObserverError> {
        let meta = match parse_tagged_frame(frame, b'H') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewFlushObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewFlushObserverError::UnexpectedLength);
            }
        };

        if meta.len != 4 {
            return Err(NewFlushObserverError::UnexpectedLength);
        }

        Ok(Self { _frame: frame })
    }
}
// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewFlushObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewFlushObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewFlushObserverError::*;
        match self {
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewFlushObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame() -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'H');
        frame.put_u32(4);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame();
        let len = FlushFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let _obs = FlushFrameObserver::new(&frame[..len]).unwrap();
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame();
        frame.pop();
        assert!(FlushFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame();
        with_junk.push(0);
        let err = FlushFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewFlushObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'H');
        frame.put_u32(5); // wrong length
        frame.to_vec();
        let err = FlushFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewFlushObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag() {
        let mut frame = build_frame();
        frame[0] = b'X';
        let err = FlushFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewFlushObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame();
        let f2 = build_frame();
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = FlushFrameObserver::peek(&stream).unwrap();
        let _obs1 = FlushFrameObserver::new(&stream[..t1]).unwrap();
        // frame 2
        let t2 = FlushFrameObserver::peek(&stream[t1..]).unwrap();
        let _obs2 = FlushFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame();
        let total = FlushFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _obs = FlushFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let tag_ptr = &frame_slice[0] as *const u8 as usize;
        assert!(tag_ptr >= base && tag_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
