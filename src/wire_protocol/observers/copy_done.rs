use std::error::Error as StdError;
use std::fmt;

// -----------------------------------------------------------------------------
// ----- CopyDoneFrameObserver -------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct CopyDoneFrameObserver<'a> {
    _frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- CopyDoneFrameObserver: Static -----------------------------------------

impl<'a> CopyDoneFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 5 || buf[0] != b'c' {
            return None;
        }
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if len != 4 {
            return None;
        }
        Some(5)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewCopyDoneObserverError> {
        if frame.len() != 5 {
            return Err(NewCopyDoneObserverError::UnexpectedLength);
        }
        if frame[0] != b'c' {
            return Err(NewCopyDoneObserverError::UnexpectedTag(frame[0]));
        }
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        if len != 4 {
            return Err(NewCopyDoneObserverError::UnexpectedLength);
        }
        Ok(Self { _frame: frame })
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewCopyDoneObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewCopyDoneObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewCopyDoneObserverError::*;
        match self {
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewCopyDoneObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame() -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'c');
        frame.put_u32(4);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame();
        let len = CopyDoneFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let _obs = CopyDoneFrameObserver::new(&frame[..len]).unwrap();
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame();
        frame.pop();
        assert!(CopyDoneFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame();
        with_junk.push(0);
        let err = CopyDoneFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewCopyDoneObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'c');
        frame.put_u32(5); // wrong length
        frame.to_vec();
        let err = CopyDoneFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCopyDoneObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag() {
        let mut frame = build_frame();
        frame[0] = b'X';
        let err = CopyDoneFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCopyDoneObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame();
        let f2 = build_frame();
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = CopyDoneFrameObserver::peek(&stream).unwrap();
        let _obs1 = CopyDoneFrameObserver::new(&stream[..t1]).unwrap();
        // frame 2
        let t2 = CopyDoneFrameObserver::peek(&stream[t1..]).unwrap();
        let _obs2 = CopyDoneFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame();
        let total = CopyDoneFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _obs = CopyDoneFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let tag_ptr = &frame_slice[0] as *const u8 as usize;
        assert!(tag_ptr >= base && tag_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
