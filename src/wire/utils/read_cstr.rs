//! Read a NUL-terminated UTF-8 string from the front of a byte slice.

use memchr::memchr;
use std::{error::Error as StdError, fmt, str};

// -----------------------------------------------------------------------------
// ----- read_cstr (mutates input) ---------------------------------------------

/// Read a NUL-terminated UTF-8 string from the front of `input_bytes`.
///
/// Returns a `&str` borrowed from `input_bytes` and advances `input_bytes` to
/// start **after** the NUL terminator.
#[inline]
pub fn read_cstr<'a>(input_bytes: &mut &'a [u8]) -> Result<&'a str, ReadCStrError> {
    let unread_bytes = *input_bytes;

    let nul_index = memchr(0, unread_bytes).ok_or(ReadCStrError::UnexpectedEof)?;

    let (bytes_before_nul, bytes_from_nul) = unread_bytes.split_at(nul_index);

    // advance past the NUL
    *input_bytes = &bytes_from_nul[1..];

    str::from_utf8(bytes_before_nul).map_err(ReadCStrError::Utf8Error)
}

// -----------------------------------------------------------------------------
// ----- read_cstr_take (returns remainder) ------------------------------------

/// Read a NUL-terminated UTF-8 string and also return the remainder slice.
///
/// This version does **not** mutate the caller’s slice; it returns `(value, remainder)`.
#[inline]
pub fn read_cstr_take<'a>(input_bytes: &'a [u8]) -> Result<(&'a str, &'a [u8]), ReadCStrError> {
    let nul_index = memchr(0, input_bytes).ok_or(ReadCStrError::UnexpectedEof)?;

    let (bytes_before_nul, bytes_from_nul) = input_bytes.split_at(nul_index);

    let parsed = str::from_utf8(bytes_before_nul).map_err(ReadCStrError::Utf8Error)?;

    Ok((parsed, &bytes_from_nul[1..]))
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum ReadCStrError {
    UnexpectedEof,
    Utf8Error(str::Utf8Error),
}

impl fmt::Display for ReadCStrError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ReadCStrError::UnexpectedEof => write!(f, "unexpected EOF"),
            ReadCStrError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
        }
    }
}

impl StdError for ReadCStrError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            ReadCStrError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use bytes::{Bytes, BytesMut};
    use std::ffi::CString;

    // ----- CString-based sanity checks -----

    #[test]
    fn reads_and_advances_mut() {
        let c_hello = CString::new("hello").unwrap();
        let mut buffer = Vec::from(c_hello.as_bytes_with_nul()); // "hello\0"
        buffer.extend_from_slice(b"world"); // "hello\0world"

        let mut input: &[u8] = &buffer;
        let got = read_cstr(&mut input).unwrap();

        assert_eq!(got, "hello");
        assert_eq!(input, b"world");
    }

    #[test]
    fn reads_and_returns_remainder() {
        let c_hello = CString::new("hello").unwrap();
        let mut buffer = Vec::from(c_hello.as_bytes_with_nul());
        buffer.extend_from_slice(b"world");

        let (got, rest) = read_cstr_take(&buffer).unwrap();

        assert_eq!(got, "hello");
        assert_eq!(rest, b"world");
    }

    #[test]
    fn eof_without_nul() {
        let c = CString::new("no-nul").unwrap();
        let mut input: &[u8] = c.as_bytes(); // no trailing NUL
        let err = read_cstr(&mut input).unwrap_err();

        assert!(matches!(err, ReadCStrError::UnexpectedEof));
    }

    #[test]
    fn invalid_utf8_before_nul() {
        let bytes = vec![0xFF, 0xFE]; // invalid UTF-8
        let c = CString::new(bytes).unwrap(); // interior NULs not allowed; invalid UTF-8 is fine
        let mut input: &[u8] = c.as_bytes_with_nul(); // includes trailing NUL
        let err = read_cstr(&mut input).unwrap_err();

        assert!(matches!(err, ReadCStrError::Utf8Error(_)));
    }

    // ----- Bytes / BytesMut integration -----

    #[test]
    fn bytes_read_cstr_mut() {
        let frozen = Bytes::from_static(b"hello\0world");
        let mut buffer: &[u8] = frozen.as_ref();
        let parsed = read_cstr(&mut buffer).unwrap();

        assert_eq!(&frozen[..], b"hello\0world");
        assert_eq!(parsed, "hello");
        assert_eq!(buffer, b"world");
    }

    #[test]
    fn bytes_read_cstr_take() {
        let frozen = Bytes::from_static(b"hello\0world");
        let (parsed, remaining) = read_cstr_take(frozen.as_ref()).unwrap();

        assert_eq!(&frozen[..], b"hello\0world");
        assert_eq!(parsed, "hello");
        assert_eq!(remaining, b"world");
    }

    #[test]
    fn bytesmut_read_cstr_mut() {
        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(b"hello\0world");

        let mut input: &[u8] = &buffer[..]; // borrow as slice
        let parsed = read_cstr(&mut input).unwrap();

        assert_eq!(&buffer[..], b"hello\0world");
        assert_eq!(parsed, "hello");
        assert_eq!(input, b"world");
    }

    #[test]
    fn bytesmut_read_cstr_take() {
        let mut buffer = BytesMut::with_capacity(16);
        buffer.extend_from_slice(b"hello\0world");

        let (parsed, remaining) = read_cstr_take(&buffer[..]).unwrap();

        assert_eq!(&buffer[..], b"hello\0world");
        assert_eq!(parsed, "hello");
        assert_eq!(remaining, b"world");
    }

    #[test]
    fn bytesmut_build_then_freeze_and_read() {
        let mut buffer = BytesMut::with_capacity(16);
        buffer.extend_from_slice(b"hello\0world");
        let frozen: Bytes = buffer.freeze(); // O(1)

        let (parsed, remaining) = read_cstr_take(frozen.as_ref()).unwrap();

        assert_eq!(&frozen[..], b"hello\0world");
        assert_eq!(parsed, "hello");
        assert_eq!(remaining, b"world");
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
