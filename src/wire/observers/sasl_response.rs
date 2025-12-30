use std::{error::Error as StdError, fmt};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- SASLResponseFrameObserver ---------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct SASLResponseFrameObserver<'a> {
    frame: &'a [u8],
    data_start: usize,
}

// -----------------------------------------------------------------------------
// ----- SASLResponseFrameObserver: Static -------------------------------------

impl<'a> SASLResponseFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'p').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewSASLResponseObserverError> {
        match parse_tagged_frame(frame, b'p') {
            Ok(_) => {}
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewSASLResponseObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength) => {
                return Err(NewSASLResponseObserverError::UnexpectedLength);
            }
            Err(TaggedFrameError::InvalidLength(len)) => {
                return Err(NewSASLResponseObserverError::InvalidLength(len));
            }
        }

        Ok(Self {
            frame,
            data_start: 5,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- SASLResponseFrameObserver: Public -------------------------------------

impl<'a> SASLResponseFrameObserver<'a> {
    #[inline]
    pub fn data(&self) -> &'a [u8] {
        &self.frame[self.data_start..]
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewSASLResponseObserverError {
    InvalidLength(usize),
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewSASLResponseObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewSASLResponseObserverError::*;
        match self {
            InvalidLength(l) => write!(f, "invalid length: {l}"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewSASLResponseObserverError {}

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
        let len = SASLResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SASLResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.data(), &b""[..]);
    }

    #[test]
    fn peek_then_new_with_data() {
        let data: &[u8] = &[1, 2, 3];
        let frame = build_frame(data);
        let len = SASLResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SASLResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.data(), data);
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame(&[1]);
        frame.pop();
        assert!(SASLResponseFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_wrong_length() {
        let mut with_junk = build_frame(&[]);
        with_junk.push(0);
        let err = SASLResponseFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewSASLResponseObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_invalid_length() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32(3); // invalid <4
        let frame = frame.to_vec();
        let err = SASLResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSASLResponseObserverError::InvalidLength(3));
    }

    #[test]
    fn new_rejects_wrong_tag() {
        let mut frame = build_frame(&[]);
        frame[0] = b'X';
        let err = SASLResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSASLResponseObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(&[1]);
        let f2 = build_frame(&[2, 3]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = SASLResponseFrameObserver::peek(&stream).unwrap();
        let obs1 = SASLResponseFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.data(), &[1]);
        // frame 2
        let t2 = SASLResponseFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = SASLResponseFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.data(), &[2, 3]);
    }

    #[test]
    fn zero_copy_data_aliases_frame_memory() {
        let data = &[1, 2, 3];
        let frame = build_frame(data);
        let total = SASLResponseFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = SASLResponseFrameObserver::new(frame_slice).unwrap();
        let resp = obs.data();
        let base = frame_slice.as_ptr() as usize;
        let ptr = resp.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
