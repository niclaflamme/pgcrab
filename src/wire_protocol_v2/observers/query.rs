use memchr::memchr;
use std::{fmt, str};

// -----------------------------------------------------------------------------
// ----- QueryFrameObserver ----------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct QueryFrameObserver<'a> {
    #[allow(dead_code)]
    frame: &'a [u8],
    query: &'a str,
}

// -----------------------------------------------------------------------------
// ----- QueryFrameObserver: Static --------------------------------------------

impl<'a> QueryFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        if buf.len() < 5 || buf[0] != b'Q' {
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
    pub fn new(frame: &'a [u8]) -> Result<Self, NewQueryObserverError> {
        if frame.len() < 5 || frame[0] != b'Q' {
            return Err(NewQueryObserverError::UnexpectedTag(
                *frame.get(0).unwrap_or(&0),
            ));
        }

        let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
        let total = 1 + len;
        if frame.len() != total {
            return Err(NewQueryObserverError::UnexpectedLength);
        }

        let mut pos = 5;

        // query
        let rel = memchr(0, &frame[pos..]).ok_or(NewQueryObserverError::UnexpectedEof)?;
        let query =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewQueryObserverError::InvalidUtf8)?;
        pos += rel + 1;

        if pos != total {
            return Err(NewQueryObserverError::UnexpectedLength);
        }

        Ok(Self { frame, query })
    }
}

// -----------------------------------------------------------------------------
// ----- QueryFrameObserver: Public --------------------------------------------

impl<'a> QueryFrameObserver<'a> {
    #[inline]
    pub fn query(&self) -> &'a str {
        self.query
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewQueryObserverError {
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewQueryObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewQueryObserverError::*;
        match self {
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewQueryObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(query: &str) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(query.as_bytes());
        body.put_u8(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'Q');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty_query() {
        let frame = build_frame("");
        let len = QueryFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = QueryFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.query(), "");
    }

    #[test]
    fn peek_then_new_with_query() {
        let query = "SELECT 1";
        let frame = build_frame(query);
        let len = QueryFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = QueryFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.query(), query);
    }

    #[test]
    fn invalid_utf8_rejected() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'Q');
        frame.put_u32(6);
        frame.extend_from_slice(&[0xFF, 0xFE]);
        frame.put_u8(0);
        let frame = frame.to_vec();
        let err = QueryFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewQueryObserverError::InvalidUtf8(_));
    }

    #[test]
    fn unexpected_eof_no_nul() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'Q');
        frame.put_u32(6);
        frame.extend_from_slice(b"ab");
        let frame = frame.to_vec();
        let err = QueryFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewQueryObserverError::UnexpectedLength);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame("SELECT 1");
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(QueryFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = QueryFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewQueryObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_query() {
        let bogus = vec![b'X', 0, 0, 0, 5, 0];
        assert!(QueryFrameObserver::peek(&bogus).is_none());
        let err = QueryFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewQueryObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn non_ascii_query() {
        let query = "SELECT 'ã��ã�¼ã�¿ã�«'";
        let frame = build_frame(query);
        let total = QueryFrameObserver::peek(&frame).unwrap();
        let obs = QueryFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.query(), query);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("SELECT 1");
        let f2 = build_frame("SELECT 2");
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = QueryFrameObserver::peek(&stream).unwrap();
        let obs1 = QueryFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.query(), "SELECT 1");
        // frame 2
        let t2 = QueryFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = QueryFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.query(), "SELECT 2");
    }

    #[test]
    fn zero_copy_query_aliases_frame_memory() {
        let frame = build_frame("SELECT 1");
        let total = QueryFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = QueryFrameObserver::new(frame_slice).unwrap();
        let q = obs.query();
        let base = frame_slice.as_ptr() as usize;
        let ptr = q.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
