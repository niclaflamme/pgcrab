// src/wire_protocol/frontend/frames/cancel_request.rs
use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MESSAGE_CODE: i32 = 80877102;

// -----------------------------------------------------------------------------
// ----- CancelRequestFrameObserver --------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct CancelRequestFrameObserver<'a> {
    frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- CancelRequestFrameObserver: Static ------------------------------------

impl<'a> CancelRequestFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 16 {
            return None;
        }
        let len = be_i32(&buf[0..]) as usize;
        if len != 16 {
            return None;
        }
        let code = be_i32(&buf[4..]);
        if code != MESSAGE_CODE {
            return None;
        }
        Some(16)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewCacnelObserverError> {
        if frame.len() != 16 {
            return Err(NewCacnelObserverError::UnexpectedLength(frame.len()));
        }

        let len = be_i32(&frame[0..]);
        if len != 16 {
            return Err(NewCacnelObserverError::UnexpectedLength(len as usize));
        }

        let code = be_i32(&frame[4..]);
        if code != MESSAGE_CODE {
            return Err(NewCacnelObserverError::UnexpectedCode(code));
        }

        Ok(Self { frame })
    }
}

// -----------------------------------------------------------------------------
// ----- CancelRequestFrameObserver: Public ------------------------------------

impl<'a> CancelRequestFrameObserver<'a> {
    #[inline]
    pub fn pid(&self) -> i32 {
        be_i32(&self.frame[8..])
    }

    #[inline]
    pub fn secret(&self) -> i32 {
        be_i32(&self.frame[12..])
    }
}
// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewCacnelObserverError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for NewCacnelObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewCacnelObserverError::*;
        match self {
            UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for NewCacnelObserverError {}

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

    fn build_frame(pid: i32, secret: i32) -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_i32(16);
        frame.put_i32(MESSAGE_CODE);
        frame.put_i32(pid);
        frame.put_i32(secret);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame(1234, 5678);
        let len = CancelRequestFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = CancelRequestFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.pid(), 1234);
        assert_eq!(obs.secret(), 5678);
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame(1234, 5678);
        frame.pop();
        assert!(CancelRequestFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame(1234, 5678);
        with_junk.push(0);
        let err = CancelRequestFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewCacnelObserverError::UnexpectedLength(17));
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_i32(20); // wrong length
        frame.put_i32(MESSAGE_CODE);
        frame.put_i32(1234);
        frame.put_i32(5678);
        let frame = frame.to_vec();
        let err = CancelRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCacnelObserverError::UnexpectedLength(20));
    }

    #[test]
    fn new_rejects_unexpected_code() {
        let mut frame = BytesMut::new();
        frame.put_i32(16);
        frame.put_i32(999999);
        frame.put_i32(1234);
        frame.put_i32(5678);
        let frame = frame.to_vec();
        let err = CancelRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCacnelObserverError::UnexpectedCode(999999));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(1234, 5678);
        let f2 = build_frame(9012, 3456);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);

        // frame 1
        let t1 = CancelRequestFrameObserver::peek(&stream).unwrap();
        let obs1 = CancelRequestFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.pid(), 1234);
        assert_eq!(obs1.secret(), 5678);

        // frame 2
        let t2 = CancelRequestFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = CancelRequestFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.pid(), 9012);
        assert_eq!(obs2.secret(), 3456);
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame(1234, 5678);
        let total = CancelRequestFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _ = CancelRequestFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let pid_ptr = &frame_slice[8] as *const u8 as usize;
        assert!(pid_ptr >= base && pid_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
