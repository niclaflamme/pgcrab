//! Module: wire_protocol::frontend::bind
//!
//! Provides parsing and serialization for the Bind message ('B') in the extended protocol.
//!
//! - `BindFrame`: represents a Bind message with portal, statement, parameters, and result formats.
//! - `Parameter`: enum distinguishes between text and binary parameter payloads.
//! - `ResultFormat`: indicates text or binary format for results.
//! - `BindFrameError`: error types for parsing and encoding.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::types::{Parameter, ResultFormat};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MESSAGE_TAG: u8 = b'B';

// -----------------------------------------------------------------------------
// ----- BindFrame -------------------------------------------------------------

#[derive(Debug)]
pub struct BindFrame<'a> {
    pub portal: &'a str,
    pub statement: &'a str,
    pub params: Vec<Parameter<'a>>,
    pub result_formats: Vec<ResultFormat>,
}

// -----------------------------------------------------------------------------
// ----- BindFrame: Static -----------------------------------------------------

impl<'a> BindFrame<'a> {
    pub fn peek(bytes: &Bytes) -> Option<usize> {
        if bytes.len() < 5 {
            return None;
        }

        if bytes[0] != MESSAGE_TAG {
            return None;
        }

        let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;

        let total_len = 1 + len;
        if bytes.len() < total_len {
            return None;
        }

        Some(total_len)
    }

    pub fn new(
        portal: &'a str,
        statement: &'a str,
        params: Vec<Parameter<'a>>,
        result_formats: Vec<ResultFormat>,
    ) -> Self {
        BindFrame {
            portal,
            statement,
            params,
            result_formats,
        }
    }

    pub fn from_bytes(bytes: &'a Bytes) -> Result<Self, BindFrameError> {
        let mut buf = bytes.as_ref();
        if buf.len() < 5 {
            return Err(BindFrameError::UnexpectedEof);
        }

        let tag = buf.get_u8();
        if tag != MESSAGE_TAG {
            return Err(BindFrameError::UnexpectedTag(tag));
        }

        let len = buf.get_u32() as usize;
        if buf.remaining() != len - 4 {
            return Err(BindFrameError::UnexpectedLength(buf.remaining() + 4));
        }

        let portal = read_cstr(&mut buf)?;
        let statement = read_cstr(&mut buf)?;

        // 1) number of parameter format codes
        let format_count = buf.get_u16() as usize;

        // 2) that many i16 format codes
        let mut param_fmts = Vec::with_capacity(format_count);
        for _ in 0..format_count {
            param_fmts.push(decode_format_code(buf.get_i16())?);
        }

        // 3) number of parameters
        let params_count = buf.get_u16() as usize;

        // 4) parameter values
        let mut params = Vec::with_capacity(params_count);
        for idx in 0..params_count {
            let val_len = buf.get_i32();

            let is_binary = if format_count == 0 {
                false
            } else if format_count == 1 {
                param_fmts[0]
            } else {
                *param_fmts.get(idx).unwrap_or(&false)
            };

            if val_len == -1 {
                let to_push = if is_binary {
                    Parameter::Binary(&[])
                } else {
                    Parameter::Text("")
                };

                params.push(to_push);
                continue;
            }

            let len = val_len as usize;
            if buf.remaining() < len {
                return Err(BindFrameError::UnexpectedEof);
            }

            let slice = &buf[..len];
            buf.advance(len);

            let mut param = Parameter::Binary(slice);
            if !is_binary {
                param = Parameter::Text(str::from_utf8(slice).map_err(BindFrameError::Utf8Error)?);
            }

            params.push(param);
        }

        // 5) result formats
        let res_count = buf.get_u16() as usize;
        let mut result_formats = Vec::with_capacity(res_count);
        for _ in 0..res_count {
            let is_bin = decode_format_code(buf.get_i16())?;
            result_formats.push(if is_bin {
                ResultFormat::Binary
            } else {
                ResultFormat::Text
            });
        }

        if buf.remaining() != 0 {
            return Err(BindFrameError::UnexpectedLength(buf.remaining()));
        }

        Ok(BindFrame {
            portal,
            statement,
            params,
            result_formats,
        })
    }
}
// -----------------------------------------------------------------------------
// ----- BindFrame: Public -----------------------------------------------------

impl<'a> BindFrame<'a> {
    pub fn to_bytes(&self) -> Result<Bytes, BindFrameError> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.extend_from_slice(self.portal.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(self.statement.as_bytes());
        body.put_u8(0);

        // param format codes (always per-param for simplicity)
        body.put_u16(self.params.len() as u16);

        for p in &self.params {
            let is_binary = matches!(p, Parameter::Binary(_));
            encode_format_code(&mut body, is_binary);
        }

        // parameter values
        body.put_u16(self.params.len() as u16);
        for p in &self.params {
            match p {
                Parameter::Text(s) => {
                    body.put_i32(s.len() as i32);
                    body.extend_from_slice(s.as_bytes());
                }
                Parameter::Binary(b) => {
                    if b.is_empty() {
                        body.put_i32(0);
                    } else {
                        body.put_i32(b.len() as i32);
                        body.extend_from_slice(b);
                    }
                }
            }
        }

        // result formats
        body.put_u16(self.result_formats.len() as u16);

        for fmt in &self.result_formats {
            encode_format_code(&mut body, matches!(fmt, ResultFormat::Binary));
        }

        // wrap
        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(MESSAGE_TAG);
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    pub fn body_size(&self) -> usize {
        let mut n = 0;

        n += self.portal.len() + 1;
        n += self.statement.len() + 1;
        n += 2 + self.params.len() * 2; // formats
        n += 2; // params count

        for p in &self.params {
            n += 4;
            match p {
                Parameter::Text(s) => n += s.len(),
                Parameter::Binary(b) => n += b.len(),
            }
        }

        n += 2 + self.result_formats.len() * 2;

        n
    }
}
// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum BindFrameError {
    InvalidFormatCode(i16),
    UnexpectedEof,
    UnexpectedLength(usize),
    UnexpectedTag(u8),
    Utf8Error(str::Utf8Error),
}

impl fmt::Display for BindFrameError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindFrameError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            BindFrameError::UnexpectedEof => write!(f, "unexpected EOF"),
            BindFrameError::UnexpectedLength(len) => write!(f, "unexpected length: {len}"),
            BindFrameError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
            BindFrameError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
        }
    }
}

impl StdError for BindFrameError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            BindFrameError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn encode_format_code(buf: &mut BytesMut, is_binary: bool) {
    buf.put_i16(if is_binary { 1 } else { 0 });
}

#[inline]
fn decode_format_code(code: i16) -> Result<bool, BindFrameError> {
    match code {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(BindFrameError::InvalidFormatCode(other)),
    }
}

/// Read a NUL-terminated UTF-8 string from the front of `input_bytes`.
/// - Returns a `&str` that borrows from the original slice (zero-copy).
/// - Advances `input_bytes` to start **after** the NUL terminator.
/// - Errors if no NUL is found before EOF or if the bytes before NUL aren’t valid UTF-8.
#[inline]
fn read_cstr<'a>(input_bytes: &mut &'a [u8]) -> Result<&'a str, BindFrameError> {
    // Snapshot the unread portion so we can split it.
    let unread_bytes = *input_bytes;

    // Find the first NUL; memchr is SIMD-accelerated and faster than iter().position().
    let nul_index = memchr::memchr(0, unread_bytes).ok_or(BindFrameError::UnexpectedEof)?;

    // Split at the NUL: left = bytes for the string, right = starts at the NUL.
    let (bytes_before_nul, bytes_from_nul) = unread_bytes.split_at(nul_index);

    // Advance caller's slice to the byte AFTER the NUL terminator.
    // Safe because split_at(nul_index) guarantees bytes_from_nul[0] is the NUL.
    *input_bytes = &bytes_from_nul[1..];

    // Validate UTF-8 and yield a borrowed &str (no copy).
    let parsed = std::str::from_utf8(bytes_before_nul).map_err(BindFrameError::Utf8Error)?;

    Ok(parsed)
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> BindFrame<'a> {
        let mut params = Vec::with_capacity(1);
        params.push(Parameter::Text("42"));

        let mut result_formats = Vec::with_capacity(1);
        result_formats.push(ResultFormat::Text);

        BindFrame {
            portal: "",
            statement: "stmt",
            params,
            result_formats,
        }
    }

    fn make_binary_email_frame(email: &str) -> Bytes {
        let mut body = BytesMut::new();
        // portal\0
        body.extend_from_slice("".as_bytes());
        body.put_u8(0);
        // statement\0
        body.extend_from_slice("stmt".as_bytes());
        body.put_u8(0);
        // one binary param
        body.put_u16(1); // param format count
        body.put_i16(1); // format code = binary
        body.put_u16(1); // param count
        body.put_i32(email.len() as i32);
        body.extend_from_slice(email.as_bytes());
        // no result formats
        body.put_u16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'B');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);
        frame.freeze()
    }

    #[test]
    fn roundtrip_text_param() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = BindFrame::from_bytes(&encoded).unwrap();
        assert_eq!(decoded.portal, frame.portal);
        assert_eq!(decoded.statement, frame.statement);
        if let Parameter::Text(t) = decoded.params[0] {
            assert_eq!(t, "42");
        } else {
            panic!("expected text param");
        }
        assert!(matches!(decoded.result_formats[0], ResultFormat::Text));
    }

    #[test]
    fn roundtrip_null_param_binary_format() {
        let mut params = Vec::with_capacity(1);
        params.push(Parameter::Binary(&[]));

        let mut result_formats = Vec::with_capacity(1);
        result_formats.push(ResultFormat::Binary);

        let frame = BindFrame {
            portal: "super_cool_mega_portal",
            statement: "super_cool_mega_statement",
            params,
            result_formats,
        };

        let encoded = frame.to_bytes().unwrap();
        let decoded = BindFrame::from_bytes(&encoded).unwrap();
        assert!(matches!(decoded.params[0], Parameter::Binary(b) if b.is_empty()));
        assert!(matches!(decoded.result_formats[0], ResultFormat::Binary));
    }

    #[test]
    fn roundtrip_binary_email_param() {
        let email = "person@example.com";
        let buf1 = make_binary_email_frame(email);
        let frame1 = BindFrame::from_bytes(&buf1).unwrap();
        let raw = if let Parameter::Binary(bytes) = frame1.params[0] {
            bytes
        } else {
            &[]
        };
        assert_eq!(raw, email.as_bytes());
        let buf2 = frame1.to_bytes().unwrap();
        let frame2 = BindFrame::from_bytes(&buf2).unwrap();
        let raw2 = if let Parameter::Binary(b) = frame2.params[0] {
            b
        } else {
            &[]
        };
        assert_eq!(raw2, email.as_bytes());
    }

    #[test]
    fn invalid_tag() {
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        bytes[0] = b'Q'; // corrupt the tag
        let bytes = Bytes::from(bytes);
        let err = BindFrame::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, BindFrameError::UnexpectedTag(_)));
    }

    #[test]
    fn invalid_format_code() {
        // produce a good frame then flip the first format code to 2
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();
        let mut offset = 5; // header
        offset += 1; // portal "" + \0
        offset += 5; // "stmt" + \0
        offset += 2; // format count u16=1
        bytes[offset] = 0;
        bytes[offset + 1] = 2; // invalid code 2
        let bytes = Bytes::from(bytes);
        let err = BindFrame::from_bytes(&bytes).unwrap_err();
        assert!(matches!(err, BindFrameError::InvalidFormatCode(2)));
    }

    #[test]
    fn roundtrip_empty_binary_param() {
        let mut params = Vec::with_capacity(1);
        params.push(Parameter::Binary(&[]));

        let frame = BindFrame {
            portal: "",
            statement: "",
            params,
            result_formats: Vec::with_capacity(0),
        };

        let encoded = frame.to_bytes().unwrap();
        let decoded = BindFrame::from_bytes(&encoded).unwrap();

        assert_eq!(decoded.params.len(), 1);
        assert!(matches!(decoded.params[0], Parameter::Binary(b) if b.is_empty()));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
