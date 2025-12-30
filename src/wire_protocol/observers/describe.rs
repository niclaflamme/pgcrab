use memchr::memchr;
use std::{fmt, str};

// -----------------------------------------------------------------------------
// ----- DescribeFrameObserver -------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct DescribeFrameObserver<'a> {
    #[allow(dead_code)]
    frame: &'a [u8],
    target: DescribeTarget,
    name: &'a str,
}

// -----------------------------------------------------------------------------
// ----- DescribeFrameObserver: Sub Structs ------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DescribeTarget {
    Portal,
    Statement,
}

// -----------------------------------------------------------------------------
// ----- DescribeFrameObserver: Static -----------------------------------------

impl<'a> DescribeFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 5 || buf[0] != b'D' {
            return None;
        }
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if len < 4 {
            return None;
        }
        let total = 1 + len;
        if buf.len() < total {
            return None;
        }
        Some(total)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewDescribeObserverError> {
        if frame.len() < 5 || frame[0] != b'D' {
            return Err(NewDescribeObserverError::UnexpectedTag(
                *frame.get(0).unwrap_or(&0),
            ));
        }
        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        let total = 1 + len;
        if frame.len() != total {
            return Err(NewDescribeObserverError::UnexpectedLength);
        }
        let mut pos = 5;
        // target
        if pos + 1 > total {
            return Err(NewDescribeObserverError::UnexpectedEof);
        }
        let target_byte = frame[pos];
        let target = match target_byte {
            b'P' => DescribeTarget::Portal,
            b'S' => DescribeTarget::Statement,
            _ => return Err(NewDescribeObserverError::InvalidTarget(target_byte)),
        };
        pos += 1;
        // name
        let rel = memchr(0, &frame[pos..]).ok_or(NewDescribeObserverError::UnexpectedEof)?;
        let name = str::from_utf8(&frame[pos..pos + rel])
            .map_err(NewDescribeObserverError::InvalidUtf8)?;
        pos += rel + 1;
        if pos != total {
            return Err(NewDescribeObserverError::UnexpectedLength);
        }
        Ok(Self {
            frame,
            target,
            name,
        })
    }
}
// -----------------------------------------------------------------------------
// ----- DescribeFrameObserver: Public -----------------------------------------

impl<'a> DescribeFrameObserver<'a> {
    #[inline]
    pub fn target(&self) -> DescribeTarget {
        self.target
    }

    #[inline]
    pub fn name(&self) -> &'a str {
        self.name
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewDescribeObserverError {
    InvalidTarget(u8),
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewDescribeObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewDescribeObserverError::*;
        match self {
            InvalidTarget(t) => write!(f, "invalid target: {t:#X}"),
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewDescribeObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(target: DescribeTarget, name: &str) -> Vec<u8> {
        let body_size = 1 + name.len() + 1;
        let len = 4 + body_size;
        let mut frame = BytesMut::with_capacity(1 + len);
        frame.put_u8(b'D');
        frame.put_u32(len as u32);
        let target_byte = match target {
            DescribeTarget::Portal => b'P',
            DescribeTarget::Statement => b'S',
        };
        frame.put_u8(target_byte);
        frame.extend_from_slice(name.as_bytes());
        frame.put_u8(0);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_portal() {
        let frame = build_frame(DescribeTarget::Portal, "my_portal");
        let len = DescribeFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = DescribeFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.target(), DescribeTarget::Portal);
        assert_eq!(obs.name(), "my_portal");
    }

    #[test]
    fn peek_then_new_statement_empty_name() {
        let frame = build_frame(DescribeTarget::Statement, "");
        let len = DescribeFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = DescribeFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.target(), DescribeTarget::Statement);
        assert_eq!(obs.name(), "");
    }

    #[test]
    fn invalid_target_rejected() {
        let mut frame = build_frame(DescribeTarget::Portal, "my_portal");
        frame[5] = b'X'; // corrupt target
        let len = DescribeFrameObserver::peek(&frame).unwrap();
        let err = DescribeFrameObserver::new(&frame[..len]).unwrap_err();
        matches!(err, NewDescribeObserverError::InvalidTarget(b'X'));
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'D');
        frame.put_u32(7); // len=7: target + 2 invalid utf8 + nul
        frame.put_u8(b'P');
        frame.extend_from_slice(&[0xFF, 0xFE]);
        frame.put_u8(0);
        let frame = frame.to_vec();
        let err = DescribeFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewDescribeObserverError::InvalidUtf8(_));
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'D');
        frame.put_u32(7); // claims len=7 but no nul
        frame.put_u8(b'P');
        frame.extend_from_slice(b"my");
        // no nul, and length mismatch anyway
        let frame = frame.to_vec();
        let err = DescribeFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewDescribeObserverError::UnexpectedLength); // first detects len mismatch
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame(DescribeTarget::Portal, "my_portal");
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(DescribeFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = DescribeFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewDescribeObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_describe() {
        let bogus = vec![b'X', 0, 0, 0, 6, b'P', 0];
        assert!(DescribeFrameObserver::peek(&bogus).is_none());
        let err = DescribeFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewDescribeObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn non_ascii_name() {
        let name = "ポータル";
        let frame = build_frame(DescribeTarget::Portal, name);
        let total = DescribeFrameObserver::peek(&frame).unwrap();
        let obs = DescribeFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.name(), name);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(DescribeTarget::Portal, "p1");
        let f2 = build_frame(DescribeTarget::Statement, "s2");
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = DescribeFrameObserver::peek(&stream).unwrap();
        let obs1 = DescribeFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.target(), DescribeTarget::Portal);
        assert_eq!(obs1.name(), "p1");
        // frame 2
        let t2 = DescribeFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = DescribeFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.target(), DescribeTarget::Statement);
        assert_eq!(obs2.name(), "s2");
    }

    #[test]
    fn zero_copy_name_aliases_frame_memory() {
        let frame = build_frame(DescribeTarget::Portal, "my_portal");
        let total = DescribeFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = DescribeFrameObserver::new(frame_slice).unwrap();
        let n = obs.name();
        let base = frame_slice.as_ptr() as usize;
        let ptr = n.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
