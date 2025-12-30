use memchr::memchr;
use std::{error::Error as StdError, fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- PasswordMessageFrameObserver ------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct PasswordMessageFrameObserver<'a> {
    frame: &'a [u8],
    password_start: usize,
}

// -----------------------------------------------------------------------------
// ----- PasswordMessageFrameObserver: Static ----------------------------------

impl<'a> PasswordMessageFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        let meta = peek_tagged_frame(buf, b'p')?;
        if meta.len < 5 {
            return None;
        }
        Some(meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewPasswordMessageObserverError> {
        let meta = match parse_tagged_frame(frame, b'p') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewPasswordMessageObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength) => {
                return Err(NewPasswordMessageObserverError::UnexpectedLength);
            }
            Err(TaggedFrameError::InvalidLength(len)) => {
                return Err(NewPasswordMessageObserverError::InvalidLength(len));
            }
        };

        if meta.len < 5 {
            return Err(NewPasswordMessageObserverError::InvalidLength(meta.len));
        }

        let nul_pos = memchr(0, &frame[5..meta.total_len])
            .ok_or(NewPasswordMessageObserverError::UnexpectedEof)?;
        if 5 + nul_pos + 1 != meta.total_len {
            return Err(NewPasswordMessageObserverError::UnexpectedLength);
        }

        let password_start = 5;
        let password_bytes = &frame[password_start..password_start + nul_pos];
        str::from_utf8(password_bytes).map_err(NewPasswordMessageObserverError::InvalidUtf8)?;

        Ok(Self {
            frame,
            password_start,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- PasswordMessageFrameObserver: Public ----------------------------------

impl<'a> PasswordMessageFrameObserver<'a> {
    pub fn password(&self) -> &'a str {
        let nul_pos = memchr(0, &self.frame[self.password_start..]).unwrap(); // validated
        let bytes = &self.frame[self.password_start..self.password_start + nul_pos];
        unsafe { str::from_utf8_unchecked(bytes) }
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewPasswordMessageObserverError {
    InvalidLength(usize),
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewPasswordMessageObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewPasswordMessageObserverError::*;
        match self {
            InvalidLength(l) => write!(f, "invalid length: {l}"),
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewPasswordMessageObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(password: &str) -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + password.len() + 1) as u32);
        frame.extend_from_slice(password.as_bytes());
        frame.put_u8(0);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_valid() {
        let password = "hunter2";
        let frame = build_frame(password);
        let len = PasswordMessageFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = PasswordMessageFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.password(), password);
    }

    #[test]
    fn peek_then_new_empty_password() {
        let frame = build_frame("");
        let len = PasswordMessageFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = PasswordMessageFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.password(), "");
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame("pwd");
        frame.pop();
        assert!(PasswordMessageFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame("pwd");
        with_junk.push(0);
        let err = PasswordMessageFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewPasswordMessageObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_no_nul() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32(8);
        frame.extend_from_slice(b"nonulxx"); // length matches but no nul
        let frame = frame.to_vec();
        let err = PasswordMessageFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewPasswordMessageObserverError::UnexpectedEof);
    }

    #[test]
    fn new_rejects_invalid_utf8() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32(7);
        frame.extend_from_slice(&[0xFF, 0xFE]);
        frame.put_u8(0);
        let frame = frame.to_vec();
        let err = PasswordMessageFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewPasswordMessageObserverError::InvalidUtf8(_));
    }

    #[test]
    fn new_rejects_wrong_tag() {
        let mut frame = build_frame("pwd");
        frame[0] = b'X';
        let err = PasswordMessageFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewPasswordMessageObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn new_rejects_invalid_length() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32(4); // invalid <5
        let frame = frame.to_vec();
        let err = PasswordMessageFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewPasswordMessageObserverError::InvalidLength(4));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("pwd1");
        let f2 = build_frame("pwd2");
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = PasswordMessageFrameObserver::peek(&stream).unwrap();
        let obs1 = PasswordMessageFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.password(), "pwd1");
        // frame 2
        let t2 = PasswordMessageFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = PasswordMessageFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.password(), "pwd2");
    }

    #[test]
    fn zero_copy_password_aliases_frame_memory() {
        let frame = build_frame("secret");
        let total = PasswordMessageFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = PasswordMessageFrameObserver::new(frame_slice).unwrap();
        let pwd = obs.password();
        let base = frame_slice.as_ptr() as usize;
        let ptr = pwd.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
