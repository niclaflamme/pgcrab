use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MESSAGE_CODE: u32 = 80877103;

// -----------------------------------------------------------------------------
// ----- SSLRequestFrameObserver -----------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct SSLRequestFrameObserver<'a> {
    #[allow(dead_code)]
    frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- SSLRequestFrameObserver: Static ---------------------------------------

impl<'a> SSLRequestFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 8 {
            return None;
        }

        let len = be_u32(&buf[0..]) as usize;
        if len != 8 {
            return None;
        }

        let code = be_u32(&buf[4..]);
        if code != MESSAGE_CODE {
            return None;
        }

        Some(8)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewSSLRequestObserverError> {
        if frame.len() != 8 {
            return Err(NewSSLRequestObserverError::UnexpectedLength(frame.len()));
        }

        let len = be_u32(&frame[0..]) as usize;
        if len != 8 {
            return Err(NewSSLRequestObserverError::UnexpectedLength(len));
        }

        let code = be_u32(&frame[4..]);
        if code != MESSAGE_CODE {
            return Err(NewSSLRequestObserverError::UnexpectedCode(code));
        }

        Ok(Self { frame })
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewSSLRequestObserverError {
    UnexpectedLength(usize),
    UnexpectedCode(u32),
}

impl fmt::Display for NewSSLRequestObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewSSLRequestObserverError::*;
        match self {
            UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for NewSSLRequestObserverError {}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn be_u32(x: &[u8]) -> u32 {
    u32::from_be_bytes([x[0], x[1], x[2], x[3]])
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame() -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u32(8);
        frame.put_u32(MESSAGE_CODE);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let frame = build_frame();
        let len = SSLRequestFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let _ = SSLRequestFrameObserver::new(&frame[..len]).unwrap();
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame();
        frame.pop();
        assert!(SSLRequestFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame();
        with_junk.push(0);
        let err = SSLRequestFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewSSLRequestObserverError::UnexpectedLength(9));
    }

    #[test]
    fn new_rejects_unexpected_length_in_message() {
        let mut frame = BytesMut::new();
        frame.put_u32(12); // wrong length
        frame.put_u32(MESSAGE_CODE);
        let frame = frame.to_vec();
        let err = SSLRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSSLRequestObserverError::UnexpectedLength(12));
    }

    #[test]
    fn new_rejects_unexpected_code() {
        let mut frame = BytesMut::new();
        frame.put_u32(8);
        frame.put_u32(999999);
        let frame = frame.to_vec();
        let err = SSLRequestFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSSLRequestObserverError::UnexpectedCode(999999));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame();
        let f2 = build_frame();
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);

        // frame 1
        let t1 = SSLRequestFrameObserver::peek(&stream).unwrap();
        let _obs1 = SSLRequestFrameObserver::new(&stream[..t1]).unwrap();

        // frame 2
        let t2 = SSLRequestFrameObserver::peek(&stream[t1..]).unwrap();
        let _obs2 = SSLRequestFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
    }

    #[test]
    fn zero_copy_aliases_frame_memory() {
        let frame = build_frame();
        let total = SSLRequestFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let _ = SSLRequestFrameObserver::new(frame_slice).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let code_ptr = &frame_slice[4] as *const u8 as usize;
        assert!(code_ptr >= base && code_ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
