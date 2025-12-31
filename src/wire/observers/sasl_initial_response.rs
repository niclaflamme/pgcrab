use memchr::memchr;
use std::{fmt, str};

use crate::wire::utils::{TaggedFrameError, parse_tagged_frame, peek_tagged_frame};

// -----------------------------------------------------------------------------
// ----- SASLInitialResponseFrameObserver --------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct SASLInitialResponseFrameObserver<'a> {
    frame: &'a [u8],
    mechanism: &'a str,
    initial_response_start: usize,
    initial_response_len: i32,
}

// -----------------------------------------------------------------------------
// ----- SASLInitialResponseFrameObserver: Static ------------------------------

impl<'a> SASLInitialResponseFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'p').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewSASLInitialResponseObserverError> {
        let meta = match parse_tagged_frame(frame, b'p') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewSASLInitialResponseObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewSASLInitialResponseObserverError::UnexpectedLength);
            }
        };

        let total = meta.total_len;
        let mut pos = 5;

        // mechanism
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewSASLInitialResponseObserverError::UnexpectedEof)?;
        let mechanism = str::from_utf8(&frame[pos..pos + rel])
            .map_err(NewSASLInitialResponseObserverError::InvalidUtf8)?;
        pos += rel + 1;

        // initial response len
        if pos + 4 > total {
            return Err(NewSASLInitialResponseObserverError::UnexpectedEof);
        }
        let initial_response_len = be_i32(&frame[pos..]);
        pos += 4;

        let initial_response_start = pos;
        if initial_response_len == -1 {
            if pos != meta.total_len {
                return Err(NewSASLInitialResponseObserverError::UnexpectedLength);
            }
        } else if initial_response_len < 0 {
            return Err(NewSASLInitialResponseObserverError::InvalidLength(
                initial_response_len,
            ));
        } else {
            let n = initial_response_len as usize;
            if pos + n != total {
                return Err(NewSASLInitialResponseObserverError::UnexpectedLength);
            }
        }

        Ok(Self {
            frame,
            mechanism,
            initial_response_start,
            initial_response_len,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- SASLInitialResponseFrameObserver: Public ------------------------------

impl<'a> SASLInitialResponseFrameObserver<'a> {
    #[inline]
    pub fn mechanism(&self) -> &'a str {
        self.mechanism
    }

    pub fn initial_response(&self) -> Option<&'a [u8]> {
        if self.initial_response_len < 0 {
            None
        } else {
            Some(&self.frame[self.initial_response_start..])
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewSASLInitialResponseObserverError {
    InvalidLength(i32),
    InvalidUtf8(str::Utf8Error),
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewSASLInitialResponseObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewSASLInitialResponseObserverError::*;
        match self {
            InvalidLength(l) => write!(f, "invalid length: {l}"),
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewSASLInitialResponseObserverError {}

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

    fn build_frame(mechanism: &str, initial_response: Option<&[u8]>) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.extend_from_slice(mechanism.as_bytes());
        body.put_u8(0);
        match initial_response {
            Some(data) => {
                body.put_i32(data.len() as i32);
                body.extend_from_slice(data);
            }
            None => {
                body.put_i32(-1);
            }
        }
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_no_initial_response() {
        let frame = build_frame("SCRAM-SHA-256", None);
        let len = SASLInitialResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SASLInitialResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.mechanism(), "SCRAM-SHA-256");
        assert_eq!(obs.initial_response(), None);
    }

    #[test]
    fn peek_then_new_with_initial_response() {
        let data: &[u8] = &[1, 2, 3];
        let frame = build_frame("SCRAM-SHA-256", Some(data));
        let len = SASLInitialResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SASLInitialResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.mechanism(), "SCRAM-SHA-256");
        assert_eq!(obs.initial_response(), Some(data));
    }

    #[test]
    fn empty_mechanism() {
        let frame = build_frame("", None);
        let total = SASLInitialResponseFrameObserver::peek(&frame).unwrap();
        let obs = SASLInitialResponseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.mechanism(), "");
        assert_eq!(obs.initial_response(), None);
    }

    #[test]
    fn invalid_utf8_mechanism_rejected() {
        let mut body = BytesMut::new();
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_u8(0);
        body.put_i32(-1);
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = SASLInitialResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSASLInitialResponseObserverError::InvalidUtf8(_));
    }

    #[test]
    fn invalid_negative_length_not_minus_one() {
        let mut body = BytesMut::new();
        body.put_u8(0);
        body.put_i32(-2); // invalid
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = SASLInitialResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSASLInitialResponseObserverError::InvalidLength(-2));
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame("SCRAM-SHA-256", None);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(SASLInitialResponseFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = SASLInitialResponseFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewSASLInitialResponseObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_sasl_initial_response() {
        let bogus = vec![b'X', 0, 0, 0, 4];
        assert!(SASLInitialResponseFrameObserver::peek(&bogus).is_none());
        let err = SASLInitialResponseFrameObserver::new(&bogus).unwrap_err();
        matches!(
            err,
            NewSASLInitialResponseObserverError::UnexpectedTag(b'X')
        );
    }

    #[test]
    fn non_ascii_mechanism() {
        let mechanism = "ã��ã�¼ã�¿ã�«";
        let frame = build_frame(mechanism, None);
        let total = SASLInitialResponseFrameObserver::peek(&frame).unwrap();
        let obs = SASLInitialResponseFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.mechanism(), mechanism);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame("mech1", Some(&[1]));
        let f2 = build_frame("mech2", None);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);

        // frame 1
        let t1 = SASLInitialResponseFrameObserver::peek(&stream).unwrap();
        let obs1 = SASLInitialResponseFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.mechanism(), "mech1");
        let expected: &[u8] = &[1];
        assert_eq!(obs1.initial_response(), Some(expected));

        // frame 2
        let t2 = SASLInitialResponseFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = SASLInitialResponseFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.mechanism(), "mech2");
        assert_eq!(obs2.initial_response(), None);
    }

    #[test]
    fn zero_copy_initial_response_aliases_frame_memory() {
        let data = &[1, 2, 3];
        let frame = build_frame("mech", Some(data));
        let total = SASLInitialResponseFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = SASLInitialResponseFrameObserver::new(frame_slice).unwrap();
        let resp = obs.initial_response().unwrap();
        let base = frame_slice.as_ptr() as usize;
        let ptr = resp.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
