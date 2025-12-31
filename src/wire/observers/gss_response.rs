use std::{error::Error as StdError, fmt};

use crate::wire::utils::{TaggedFrameError, parse_tagged_frame, peek_tagged_frame};

// -----------------------------------------------------------------------------
// ----- GSSResponseFrameObserver ----------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct GSSResponseFrameObserver<'a> {
    frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- GSSResponseFrameObserver: Static --------------------------------------

impl<'a> GSSResponseFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'p').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewGSSResponseObserverError> {
        match parse_tagged_frame(frame, b'p') {
            Ok(_) => {}
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewGSSResponseObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewGSSResponseObserverError::UnexpectedLength);
            }
        }

        Ok(Self { frame })
    }
}

// -----------------------------------------------------------------------------
// ----- GSSResponseFrameObserver: Public --------------------------------------

impl<'a> GSSResponseFrameObserver<'a> {
    #[inline]
    pub fn gss_token(&self) -> &'a [u8] {
        &self.frame[5..]
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewGSSResponseObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewGSSResponseObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewGSSResponseObserverError::*;
        match self {
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewGSSResponseObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(data: &[u8]) -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32((4 + data.len()) as u32);
        frame.extend_from_slice(data);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty_data() {
        let frame = build_frame(&[]);
        let len = GSSResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = GSSResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.gss_token(), &b""[..]);
    }

    #[test]
    fn peek_then_new_with_data() {
        let data = &[1, 2, 3];
        let frame = build_frame(data);
        let len = GSSResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = GSSResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.gss_token(), data);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame(&[1, 2, 3]);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(GSSResponseFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = GSSResponseFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewGSSResponseObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_gss_response() {
        let bogus = vec![b'X', 0, 0, 0, 7, 1, 2, 3];
        assert!(GSSResponseFrameObserver::peek(&bogus).is_none());
        let err = GSSResponseFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewGSSResponseObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(&[1, 2]);
        let f2 = build_frame(&[3, 4, 5]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = GSSResponseFrameObserver::peek(&stream).unwrap();
        let obs1 = GSSResponseFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.gss_token(), &[1, 2]);
        // frame 2
        let t2 = GSSResponseFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = GSSResponseFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.gss_token(), &[3, 4, 5]);
    }

    #[test]
    fn zero_copy_gss_token_aliases_frame_memory() {
        let frame = build_frame(&[1, 2, 3]);
        let total = GSSResponseFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = GSSResponseFrameObserver::new(frame_slice).unwrap();
        let token = obs.gss_token();
        let base = frame_slice.as_ptr() as usize;
        let ptr = token.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
