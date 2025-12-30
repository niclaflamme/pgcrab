// src/wire_protocol/frontend/frames/gssenc_request.rs
use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MESSAGE_CODE: i32 = 80877104;

// -----------------------------------------------------------------------------
// ----- GSSENCRequestFrameObserver --------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct GSSENCRequestFrameObserver<'a> {
    _frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- GSSENCRequestFrameObserver: Static ------------------------------------

impl<'a> GSSENCRequestFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 8 {
            return None;
        }
        let len = be_i32(&buf[0..]) as usize;
        if len != 8 {
            return None;
        }
        let code = be_i32(&buf[4..]);
        if code != MESSAGE_CODE {
            return None;
        }
        Some(8)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewGSSENCRequestObserverError> {
        if frame.len() != 8 {
            return Err(NewGSSENCRequestObserverError::UnexpectedLength(frame.len()));
        }

        let len = be_i32(&frame[0..]);
        if len != 8 {
            return Err(NewGSSENCRequestObserverError::UnexpectedLength(
                len as usize,
            ));
        }

        let code = be_i32(&frame[4..]);
        if code != MESSAGE_CODE {
            return Err(NewGSSENCRequestObserverError::UnexpectedCode(code));
        }

        Ok(Self { _frame: frame })
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewGSSENCRequestObserverError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for NewGSSENCRequestObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewGSSENCRequestObserverError::*;
        match self {
            UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for NewGSSENCRequestObserverError {}

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

    fn build_frame() -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_i32(8);
        frame.put_i32(MESSAGE_CODE);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame();
        let len = GSSENCRequestFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let _ = GSSENCRequestFrameObserver::new(&frame[..len]).unwrap();
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame();
        frame.pop();
        assert!(GSSENCRequestFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame();
        with_junk.push(0);
        let err = GSSENCRequestFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewGSSENCRequestObserverError::UnexpectedLength(9));
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_i32(12); // wrong length
        frame.put_i32(MESSAGE_CODE);
        let frame = frame.to_vec();
        let err = GSSENCRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewGSSENCRequestObserverError::UnexpectedLength(12));
    }

    #[test]
    fn new_rejects_unexpected_code() {
        let mut frame = BytesMut::new();
        frame.put_i32(8);
        frame.put_i32(999999);
        let frame = frame.to_vec();
        let err = GSSENCRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewGSSENCRequestObserverError::UnexpectedCode(999999));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame();
        let f2 = build_frame();
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);

        // frame 1
        let t1 = GSSENCRequestFrameObserver::peek(&stream).unwrap();
        let _obs1 = GSSENCRequestFrameObserver::new(&stream[..t1]).unwrap();

        // frame 2
        let t2 = GSSENCRequestFrameObserver::peek(&stream[t1..]).unwrap();
        let _obs2 = GSSENCRequestFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame();
        let total = GSSENCRequestFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _ = GSSENCRequestFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let code_ptr = &frame_slice[4] as *const u8 as usize;
        assert!(code_ptr >= base && code_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
