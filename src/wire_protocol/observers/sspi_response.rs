use crate::wire_protocol::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- SSPIResponseFrameObserver ---------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct SSPIResponseFrameObserver<'a> {
    frame: &'a [u8],
    payload_start: usize,
}

// -----------------------------------------------------------------------------
// ----- SSPIResponseFrameObserver: Static -------------------------------------

impl<'a> SSPIResponseFrameObserver<'a> {
    /// Cheap header peek. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'p').map(|meta| meta.total_len)
    }

    /// Strict validator. Accepts any binary payload (including empty).
    #[inline]
    pub fn new(frame: &'a [u8]) -> Result<Self, NewSSPIResponseObserverError> {
        let meta = match parse_tagged_frame(frame, b'p') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewSSPIResponseObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength) => {
                return Err(NewSSPIResponseObserverError::UnexpectedLength);
            }
            Err(TaggedFrameError::InvalidLength(len)) => {
                return Err(NewSSPIResponseObserverError::InvalidLength(len as u32));
            }
        };
        let _ = meta;
        // SSPI token is opaque; zero or more bytes are allowed.
        Ok(Self {
            frame,
            payload_start: 5,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- SSPIResponseFrameObserver: Public -------------------------------------

impl<'a> SSPIResponseFrameObserver<'a> {
    /// Full frame slice (tag + length + payload).
    #[inline]
    pub fn frame(&self) -> &'a [u8] {
        self.frame
    }

    /// Opaque SSPI token (may be empty).
    #[inline]
    pub fn payload(&self) -> &'a [u8] {
        &self.frame[self.payload_start..]
    }

    /// Length of the SSPI token.
    #[inline]
    pub fn payload_len(&self) -> usize {
        self.frame.len() - self.payload_start
    }
}
// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum NewSSPIResponseObserverError {
    UnexpectedLength,
    UnexpectedTag(u8),
    InvalidLength(u32),
}

impl std::fmt::Display for NewSSPIResponseObserverError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NewSSPIResponseObserverError::UnexpectedLength => write!(f, "unexpected length"),
            NewSSPIResponseObserverError::UnexpectedTag(tag) => {
                write!(f, "unexpected tag: {:#X}", tag)
            }
            NewSSPIResponseObserverError::InvalidLength(len) => {
                write!(f, "invalid length: {}", len)
            }
        }
    }
}

impl std::error::Error for NewSSPIResponseObserverError {}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn build_frame(payload: &[u8]) -> Vec<u8> {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        let len = 4u32 + payload.len() as u32;
        frame.put_u32(len);
        frame.extend_from_slice(payload);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_empty() {
        let frame = build_frame(&[]);
        let len = SSPIResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SSPIResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.payload_len(), 0);
    }

    #[test]
    fn peek_then_new_with_payload() {
        let frame = build_frame(&[1, 2, 3, 4]);
        let len = SSPIResponseFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = SSPIResponseFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.payload(), &[1, 2, 3, 4]);
    }

    #[test]
    fn rejects_wrong_tag() {
        let mut frame = build_frame(&[9]);
        frame[0] = b'X';
        assert!(SSPIResponseFrameObserver::peek(&frame).is_none());
        let err = SSPIResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSSPIResponseObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn peek_rejects_incomplete() {
        let mut frame = build_frame(&[1, 2, 3]);
        frame.pop();
        assert!(SSPIResponseFrameObserver::peek(&frame).is_none());
    }

    #[test]
    fn new_rejects_unexpected_length() {
        let mut with_junk = build_frame(&[]);
        with_junk.push(0);
        let err = SSPIResponseFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewSSPIResponseObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_invalid_length() {
        let mut frame = BytesMut::new();
        frame.put_u8(b'p');
        frame.put_u32(3); // <4
        let frame = frame.to_vec();
        let err = SSPIResponseFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewSSPIResponseObserverError::InvalidLength(3));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(&[1]);
        let f2 = build_frame(&[2, 3]);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = SSPIResponseFrameObserver::peek(&stream).unwrap();
        let obs1 = SSPIResponseFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.payload(), &[1]);
        // frame 2
        let t2 = SSPIResponseFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = SSPIResponseFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.payload(), &[2, 3]);
    }

    #[test]
    fn zero_copy_payload_aliases_frame_memory() {
        let payload = &[1, 2, 3];
        let frame = build_frame(payload);
        let total = SSPIResponseFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = SSPIResponseFrameObserver::new(frame_slice).unwrap();
        let got = obs.payload();
        let base = frame_slice.as_ptr() as usize;
        let ptr = got.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
