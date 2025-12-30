use memchr::memchr;
use std::{fmt, str};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const PROTOCOL_VERSION: i32 = 196608; // 3.0

// -----------------------------------------------------------------------------
// ----- StartupFrameObserver --------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct StartupFrameObserver<'a> {
    frame: &'a [u8],
    params_start: usize,
}

// -----------------------------------------------------------------------------
// ----- StartupFrameObserver: Static ------------------------------------------

impl<'a> StartupFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 8 {
            return None;
        }

        let len = be_i32(&buf[0..]) as usize;
        if buf.len() < len {
            return None;
        }

        let version = be_i32(&buf[4..]);
        if version != PROTOCOL_VERSION {
            return None;
        }

        Some(len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewStartupObserverError> {
        if frame.len() < 8 {
            return Err(NewStartupObserverError::UnexpectedLength);
        }

        let len = be_i32(&frame[0..]) as usize;
        if frame.len() != len {
            return Err(NewStartupObserverError::UnexpectedLength);
        }

        let version = be_i32(&frame[4..]);
        if version != PROTOCOL_VERSION {
            return Err(NewStartupObserverError::UnexpectedVersion(version));
        }

        let mut pos = 8;

        loop {
            // key
            let rel = memchr(0, &frame[pos..]).ok_or(NewStartupObserverError::UnexpectedEof)?;
            let _key = str::from_utf8(&frame[pos..pos + rel])
                .map_err(NewStartupObserverError::InvalidUtf8)?;
            pos += rel + 1;
            if rel == 0 {
                // terminating nul
                break;
            }
            // value
            let rel = memchr(0, &frame[pos..]).ok_or(NewStartupObserverError::UnexpectedEof)?;
            let _value = str::from_utf8(&frame[pos..pos + rel])
                .map_err(NewStartupObserverError::InvalidUtf8)?;
            pos += rel + 1;
        }

        if pos != len {
            return Err(NewStartupObserverError::UnexpectedLength);
        }

        Ok(Self {
            frame,
            params_start: 8,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- StartupFrameObserver: Public ------------------------------------------

impl<'a> StartupFrameObserver<'a> {
    #[inline]
    pub fn protocol_version(&self) -> i32 {
        be_i32(&self.frame[4..])
    }

    pub fn param(&self, key: &str) -> Option<&'a str> {
        let mut pos = self.params_start;
        loop {
            let key_start = pos;
            let rel = memchr(0, &self.frame[pos..]).unwrap(); // validated
            let this_key = unsafe { str::from_utf8_unchecked(&self.frame[key_start..pos + rel]) };
            pos += rel + 1;
            if this_key.is_empty() {
                return None;
            }
            let val_start = pos;
            let rel = memchr(0, &self.frame[pos..]).unwrap(); // validated
            let this_val = unsafe { str::from_utf8_unchecked(&self.frame[val_start..pos + rel]) };
            pos += rel + 1;
            if this_key == key {
                return Some(this_val);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewStartupObserverError {
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedVersion(i32),
}

impl fmt::Display for NewStartupObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewStartupObserverError::*;
        match self {
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedVersion(v) => write!(f, "unexpected version: {v}"),
        }
    }
}

impl std::error::Error for NewStartupObserverError {}

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

    fn build_frame(params: &[(&str, &str)]) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_VERSION);
        for &(k, v) in params {
            body.extend_from_slice(k.as_bytes());
            body.put_u8(0);
            body.extend_from_slice(v.as_bytes());
            body.put_u8(0);
        }
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_i32((4 + body.len()) as i32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty_params() {
        let frame = build_frame(&[]);
        let len = StartupFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = StartupFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.protocol_version(), PROTOCOL_VERSION);
        assert_eq!(obs.param("user"), None);
    }

    #[test]
    fn peek_then_new_with_params() {
        let frame = build_frame(&[("user", "postgres"), ("database", "mydb")]);
        let len = StartupFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = StartupFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.param("user"), Some("postgres"));
        assert_eq!(obs.param("database"), Some("mydb"));
        assert_eq!(obs.param("nonexistent"), None);
    }

    #[test]
    fn invalid_utf8_key_rejected() {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_VERSION);
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_u8(0);
        body.extend_from_slice(b"value");
        body.put_u8(0);
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_i32((4 + body.len()) as i32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = StartupFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewStartupObserverError::InvalidUtf8(_));
    }

    #[test]
    fn invalid_utf8_value_rejected() {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_VERSION);
        body.extend_from_slice(b"key");
        body.put_u8(0);
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_u8(0);
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_i32((4 + body.len()) as i32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = StartupFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewStartupObserverError::InvalidUtf8(_));
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let mut body = BytesMut::new();
        body.put_i32(PROTOCOL_VERSION);
        body.extend_from_slice(b"key");
        // no nul
        let mut frame = BytesMut::new();
        frame.put_i32((4 + body.len()) as i32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = StartupFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewStartupObserverError::UnexpectedEof);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame(&[]);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(StartupFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = StartupFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewStartupObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_unexpected_version() {
        let mut body = BytesMut::new();
        body.put_i32(12345); // wrong version
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_i32((4 + body.len()) as i32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = StartupFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewStartupObserverError::UnexpectedVersion(12345));
    }

    #[test]
    fn non_ascii_param() {
        let frame = build_frame(&[("user", "ã��ã�¼ã�¿ã�«")]);
        let total = StartupFrameObserver::peek(&frame).unwrap();
        let obs = StartupFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param("user"), Some("ã��ã�¼ã�¿ã�«"));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(&[("user", "u1")]);
        let f2 = build_frame(&[("user", "u2")]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = StartupFrameObserver::peek(&stream).unwrap();
        let obs1 = StartupFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.param("user"), Some("u1"));
        // frame 2
        let t2 = StartupFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = StartupFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.param("user"), Some("u2"));
    }

    #[test]
    fn zero_copy_param_aliases_frame_memory() {
        let frame = build_frame(&[("user", "postgres")]);
        let total = StartupFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = StartupFrameObserver::new(frame_slice).unwrap();
        let p = obs.param("user").unwrap();
        let base = frame_slice.as_ptr() as usize;
        let ptr = p.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
