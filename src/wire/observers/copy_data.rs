use std::{error::Error as StdError, fmt};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- CopyDataFrameObserver -------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct CopyDataFrameObserver<'a> {
    frame: &'a [u8],
}

// -----------------------------------------------------------------------------
// ----- CopyDataFrameObserver: Static -----------------------------------------

impl<'a> CopyDataFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'd').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewCopyDataObserverError> {
        match parse_tagged_frame(frame, b'd') {
            Ok(_) => {}
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewCopyDataObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewCopyDataObserverError::UnexpectedLength);
            }
        }
        Ok(Self { frame })
    }
}

// -----------------------------------------------------------------------------
// ----- CopyDataFrameObserver: Public -----------------------------------------

impl<'a> CopyDataFrameObserver<'a> {
    #[inline]
    pub fn data(&self) -> &'a [u8] {
        &self.frame[5..]
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewCopyDataObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewCopyDataObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewCopyDataObserverError::*;
        match self {
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for NewCopyDataObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(data: &[u8]) -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'd');
        frame.put_u32((4 + data.len()) as u32);
        frame.extend_from_slice(data);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty_data() {
        let frame = build_frame(&[]);
        let len = CopyDataFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = CopyDataFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.data(), &b""[..]);
    }

    #[test]
    fn peek_then_new_with_data() {
        let data = &[1, 2, 3];
        let frame = build_frame(data);
        let len = CopyDataFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = CopyDataFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.data(), data);
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame(&[1, 2, 3]);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(CopyDataFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = CopyDataFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewCopyDataObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_copy_data() {
        let bogus = vec![b'X', 0, 0, 0, 4];
        assert!(CopyDataFrameObserver::peek(&bogus).is_none());
        let err = CopyDataFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewCopyDataObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn data_with_zeros() {
        let data = &[0, 1, 0, 255];
        let frame = build_frame(data);
        let total = CopyDataFrameObserver::peek(&frame).unwrap();
        let obs = CopyDataFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.data(), data);
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(&[1, 2]);
        let f2 = build_frame(&[3, 4, 5]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = CopyDataFrameObserver::peek(&stream).unwrap();
        let obs1 = CopyDataFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.data(), &[1, 2]);
        // frame 2
        let t2 = CopyDataFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = CopyDataFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.data(), &[3, 4, 5]);
    }

    #[test]
    fn zero_copy_data_aliases_frame_memory() {
        let frame = build_frame(&[1, 2, 3]);
        let total = CopyDataFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = CopyDataFrameObserver::new(frame_slice).unwrap();
        let d = obs.data();
        let base = frame_slice.as_ptr() as usize;
        let ptr = d.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }

    #[test]
    fn reject_invalid_short_length() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'd');
        frame.put_u32(3); // invalid <4
        let frame = frame.to_vec();
        let err = CopyDataFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewCopyDataObserverError::UnexpectedLength);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
