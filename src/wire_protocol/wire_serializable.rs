use bytes::{Bytes, BytesMut};
use std::error::Error as StdError;

pub trait WireSerializable<'a>: Sized {
    type Error: StdError + Send + Sync + 'static;

    /// Look ahead at the buffer to determine if the message is complete,
    /// returning the number of bytes needed to complete the message.
    fn peek(buf: &BytesMut) -> Option<usize>;

    /// Serialize the object into bytes for wire transmission.
    fn to_bytes(&self) -> Result<Bytes, Self::Error>;

    /// Deserialize from bytes into the object.
    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error>;

    /// Size of the body of the message.
    fn body_size(&self) -> usize;
}
