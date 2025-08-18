//! Module: wire_protocol::frontend::cancel_request
//!
//! Provides parsing and serialization for the CancelRequest message in the protocol.
//!
//! Note: Unlike regular protocol messages, CancelRequest has no tag byte and is typically
//! sent over a separate connection to interrupt a running query.
//!
//! - `CancelRequestFrame`: represents a CancelRequest message with backend PID and secret key.
//! - `CancelRequestError`: error types for parsing and encoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MESSAGE_CODE: i32 = 80877102;

// -----------------------------------------------------------------------------
// ----- CancelRequestFrame ----------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CancelRequestFrame {
    pub pid: i32,
    pub secret: i32,
}

// -----------------------------------------------------------------------------
// ----- CancelRequestFrame: Static --------------------------------------------

impl CancelRequestFrame {
    pub fn peek(bytes: &Bytes) -> Option<usize> {
        if bytes.len() < 16 {
            return None;
        }

        let len = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        if len != 16 {
            return None;
        }

        let code = i32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        if code != MESSAGE_CODE {
            return None;
        }

        Some(16)
    }

    pub fn new(pid: i32, secret: i32) -> Self {
        CancelRequestFrame { pid, secret }
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Self, CancelRequestError> {
        if bytes.len() != 16 {
            return Err(CancelRequestError::UnexpectedLength(bytes.len()));
        }

        let mut buf = bytes;

        let len = buf.get_i32();
        if len != 16 {
            return Err(CancelRequestError::UnexpectedLength(len as usize));
        }

        let code = buf.get_i32();
        if code != MESSAGE_CODE {
            return Err(CancelRequestError::UnexpectedCode(code));
        }

        let pid = buf.get_i32();
        let secret = buf.get_i32();

        Ok(CancelRequestFrame { pid, secret })
    }
}

// -----------------------------------------------------------------------------
// ----- CancelRequestFrame: Public --------------------------------------------

impl CancelRequestFrame {
    pub fn to_bytes(&self) -> Result<Bytes, CancelRequestError> {
        let mut buf = BytesMut::with_capacity(16);
        buf.put_i32(16);
        buf.put_i32(MESSAGE_CODE);
        buf.put_i32(self.pid);
        buf.put_i32(self.secret);
        Ok(buf.freeze())
    }

    pub fn header_size(&self) -> usize {
        4
    }

    pub fn body_size(&self) -> usize {
        12
    }
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum CancelRequestError {
    UnexpectedLength(usize),
    UnexpectedCode(i32),
}

impl fmt::Display for CancelRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CancelRequestError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            CancelRequestError::UnexpectedCode(code) => write!(f, "unexpected code: {code}"),
        }
    }
}

impl StdError for CancelRequestError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame() -> CancelRequestFrame {
        CancelRequestFrame {
            pid: 1234,
            secret: 5678,
        }
    }

    #[test]
    fn roundtrip() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = CancelRequestFrame::from_bytes(encoded).unwrap();
        assert_eq!(decoded.pid, frame.pid);
        assert_eq!(decoded.secret, frame.secret);
    }

    #[test]
    fn unexpected_length() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(MESSAGE_CODE);
        buf.put_i32(1234);
        let buf = buf.freeze();
        let err = CancelRequestFrame::from_bytes(buf).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedLength(12));
    }

    #[test]
    fn unexpected_code() {
        let mut buf = BytesMut::new();
        buf.put_i32(16);
        buf.put_i32(999999);
        buf.put_i32(1234);
        buf.put_i32(5678);
        let buf = buf.freeze();
        let err = CancelRequestFrame::from_bytes(buf).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedCode(999999));
    }

    #[test]
    fn unexpected_length_in_message() {
        let mut buf = BytesMut::new();
        buf.put_i32(20); // wrong length
        buf.put_i32(MESSAGE_CODE);
        buf.put_i32(1234);
        buf.put_i32(5678);
        let buf = buf.freeze();
        let err = CancelRequestFrame::from_bytes(buf).unwrap_err();
        matches!(err, CancelRequestError::UnexpectedLength(20));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
