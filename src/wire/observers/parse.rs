use memchr::memchr;
use std::{fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- ParseFrameObserver ----------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct ParseFrameObserver<'a> {
    frame: &'a [u8],

    statement: &'a str,
    query: &'a str,

    param_type_count: usize,
    param_type_oids_start: usize,
}

// -----------------------------------------------------------------------------
// ----- ParseFrameObserver: Static --------------------------------------------

impl<'a> ParseFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'P').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewParseObserverError> {
        let meta = match parse_tagged_frame(frame, b'P') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewParseObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewParseObserverError::UnexpectedLength);
            }
        };

        let total = meta.total_len;
        let mut pos = 5;

        // statement
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewParseObserverError::UnexpectedEof)?;
        let statement =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewParseObserverError::InvalidUtf8)?;
        pos += rel + 1;

        // query
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewParseObserverError::UnexpectedEof)?;
        let query =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewParseObserverError::InvalidUtf8)?;
        pos += rel + 1;

        // param type count
        if pos + 2 > meta.total_len {
            return Err(NewParseObserverError::UnexpectedEof);
        }
        let signed_param_type_count = be_i16(&frame[pos..]);
        if signed_param_type_count < 0 {
            return Err(NewParseObserverError::InvalidCount(signed_param_type_count));
        }
        let param_type_count = signed_param_type_count as usize;
        pos += 2;

        // param type oids
        let param_type_oids_start = pos;
        let need = pos + 4 * param_type_count;
        if need > total {
            return Err(NewParseObserverError::UnexpectedEof);
        }
        // OIDs can be 0 (unspecified) or positive; no further validation needed
        pos = need;

        if pos != meta.total_len {
            return Err(NewParseObserverError::UnexpectedLength);
        }

        Ok(Self {
            frame,
            statement,
            query,
            param_type_count,
            param_type_oids_start,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- ParseFrameObserver: Public --------------------------------------------

impl<'a> ParseFrameObserver<'a> {
    #[inline]
    pub fn statement(&self) -> &'a str {
        self.statement
    }

    #[inline]
    pub fn query(&self) -> &'a str {
        self.query
    }

    #[inline]
    pub fn param_type_count(&self) -> usize {
        self.param_type_count
    }

    #[inline]
    pub fn param_type_oid(&self, index: usize) -> i32 {
        debug_assert!(index < self.param_type_count);
        let off = self.param_type_oids_start + 4 * index;
        be_i32(&self.frame[off..])
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewParseObserverError {
    InvalidCount(i16),
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewParseObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewParseObserverError::*;
        match self {
            InvalidCount(c) => write!(f, "invalid count: {c}"),
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewParseObserverError {}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn be_i16(x: &[u8]) -> i16 {
    i16::from_be_bytes([x[0], x[1]])
}

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

    fn build_frame(statement: &str, query: &str, param_oids: &[i32]) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(statement.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(query.as_bytes());
        body.put_u8(0);
        body.put_i16(param_oids.len() as i16);
        for &oid in param_oids {
            body.put_i32(oid);
        }
        let mut frame = BytesMut::new();
        frame.put_u8(b'P');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_minimal() {
        let frame = build_frame("", "SELECT 1", &[]);
        let len = ParseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = ParseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.statement(), "");
        assert_eq!(obs.query(), "SELECT 1");
        assert_eq!(obs.param_type_count(), 0);
    }

    #[test]
    fn with_params() {
        let frame = build_frame("s1", "SELECT $1, $2", &[23, 25]);
        let total = ParseFrameObserver::peek(&frame).unwrap();
        let obs = ParseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.statement(), "s1");
        assert_eq!(obs.query(), "SELECT $1, $2");
        assert_eq!(obs.param_type_count(), 2);
        assert_eq!(obs.param_type_oid(0), 23);
        assert_eq!(obs.param_type_oid(1), 25);
    }

    #[test]
    fn unspecified_params() {
        let frame = build_frame("", "SELECT $1", &[0]);
        let total = ParseFrameObserver::peek(&frame).unwrap();
        let obs = ParseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_type_oid(0), 0);
    }

    #[test]
    fn invalid_utf8_statement_rejected() {
        let mut body = BytesMut::new();
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_u8(0);
        body.extend_from_slice(b"SELECT 1");
        body.put_u8(0);
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'P');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = ParseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewParseObserverError::InvalidUtf8(_));
    }

    #[test]
    fn invalid_utf8_query_rejected() {
        let mut body = BytesMut::new();
        body.put_u8(0);
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_u8(0);
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'P');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = ParseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewParseObserverError::InvalidUtf8(_));
    }

    #[test]
    fn invalid_negative_param_count() {
        let frame = build_frame("", "SELECT 1", &[]);
        let mut frame = frame;
        // corrupt param count to -1
        let pos = 5 + 1 + 9; // after statement nul + query "SELECT 1" + nul
        frame[pos] = 255;
        frame[pos + 1] = 255;
        let err = ParseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewParseObserverError::InvalidCount(-1));
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame("", "SELECT 1", &[]);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(ParseFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = ParseFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewParseObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_parse() {
        let bogus = vec![b'X', 0, 0, 0, 4];
        assert!(ParseFrameObserver::peek(&bogus).is_none());
        let err = ParseFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewParseObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn non_ascii_query() {
        let query = "SELECT 'ã��ã�¼ã�¿ã�«'";
        let frame = build_frame("", query, &[]);
        let total = ParseFrameObserver::peek(&frame).unwrap();
        let obs = ParseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.query(), query);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("s1", "SELECT 1", &[]);
        let f2 = build_frame("s2", "SELECT 2", &[]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = ParseFrameObserver::peek(&stream).unwrap();
        let obs1 = ParseFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.query(), "SELECT 1");
        // frame 2
        let t2 = ParseFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = ParseFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.query(), "SELECT 2");
    }

    #[test]
    fn zero_copy_query_aliases_frame_memory() {
        let frame = build_frame("", "SELECT 1", &[]);
        let total = ParseFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = ParseFrameObserver::new(frame_slice).unwrap();
        let q = obs.query();
        let base = frame_slice.as_ptr() as usize;
        let ptr = q.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }

    #[test]
    fn large_param_set_stress() {
        let count = 64usize;
        let mut oids = Vec::with_capacity(count);
        for i in 0..count {
            oids.push(i as i32);
        }
        let frame = build_frame("", "SELECT", &oids);
        let total = ParseFrameObserver::peek(&frame).unwrap();
        let obs = ParseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_type_count(), count);
        assert_eq!(obs.param_type_oid(0), 0);
        assert_eq!(obs.param_type_oid(63), 63);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
