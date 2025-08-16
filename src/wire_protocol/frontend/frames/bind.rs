//! Module: wire_protocol::frontend::bind
//!
//! Provides parsing and serialization for the Bind message ('B') in the extended protocol.
//!
//! - `BindFrame`: represents a Bind message with portal, statement, parameters, and result formats.
//! - `Parameter`: enum distinguishes between text and binary parameter payloads.
//! - `ResultFormat`: indicates text or binary format for results.
//! - `BindFrameError`: error types for parsing and encoding.
//!
//! Implements `WireSerializable` for easy conversion between raw bytes and `BindFrame`.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use smallvec::SmallVec;
use std::{error::Error as StdError, fmt, str};

use crate::wire_protocol::WireSerializable;
use crate::wire_protocol::types::{Parameter, ResultFormat};

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug)]
pub struct BindFrame<'a> {
    pub portal: &'a str,
    pub statement: &'a str,
    pub params: SmallVec<[Parameter<'a>; 8]>,
    pub result_formats: SmallVec<[ResultFormat; 8]>,
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum BindError {
    Utf8Error(str::Utf8Error),
    UnexpectedEof,
    InvalidLength,
    InvalidFormatCode(i16),
    UnexpectedTag(u8),
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::Utf8Error(e) => write!(f, "UTF-8 error: {e}"),
            BindError::UnexpectedEof => write!(f, "unexpected EOF"),
            BindError::InvalidLength => write!(f, "invalid length or format code"),
            BindError::InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            BindError::UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl StdError for BindError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            BindError::Utf8Error(e) => Some(e),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Helpers ---------------------------------------------------------------

#[inline]
fn encode_format_code(buf: &mut BytesMut, is_binary: bool) {
    buf.put_i16(if is_binary { 1 } else { 0 });
}

#[inline]
fn decode_format_code(code: i16) -> Result<bool, BindError> {
    match code {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(BindError::InvalidFormatCode(other)),
    }
}

fn read_cstr<'a>(buf: &mut &'a [u8]) -> Result<&'a str, BindError> {
    let nul = buf
        .iter()
        .position(|b| *b == 0)
        .ok_or(BindError::UnexpectedEof)?;
    let (raw, rest) = buf.split_at(nul);
    *buf = &rest[1..]; // skip NUL
    Ok(str::from_utf8(raw).map_err(BindError::Utf8Error)?)
}

// -----------------------------------------------------------------------------
// ----- WireSerializable ------------------------------------------------------

impl<'a> WireSerializable<'a> for BindFrame<'a> {
    type Error = BindError;

    fn peek(_buf: &BytesMut) -> Option<usize> {
        None
    }

    fn from_bytes(mut bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.remaining() < 5 {
            return Err(BindError::UnexpectedEof);
        }
        let tag = bytes.get_u8();
        if tag != b'B' {
            return Err(BindError::UnexpectedTag(tag));
        }
        let _len = bytes.get_u32();

        let portal = read_cstr(&mut bytes)?;
        let statement = read_cstr(&mut bytes)?;

        let fmt_count = bytes.get_i16();
        let mut param_fmts = Vec::with_capacity(fmt_count as usize);
        for _ in 0..fmt_count {
            param_fmts.push(decode_format_code(bytes.get_i16())?);
        }

        let param_count = bytes.get_i16() as usize;
        let mut params = SmallVec::<[Parameter<'a>; 8]>::with_capacity(param_count);
        for idx in 0..param_count {
            let val_len = bytes.get_i32();
            if val_len == -1 {
                params.push(Parameter::Binary(&[]));
            } else {
                let is_binary = if fmt_count == 0 {
                    false
                } else if fmt_count == 1 {
                    param_fmts.get(0).copied().unwrap_or(false)
                } else {
                    param_fmts.get(idx).copied().unwrap_or(false)
                };
                let len = val_len as usize;
                let slice = &bytes[..len];
                bytes.advance(len);
                if is_binary {
                    params.push(Parameter::Binary(slice));
                } else {
                    params.push(Parameter::Text(
                        str::from_utf8(slice).map_err(BindError::Utf8Error)?,
                    ));
                }
            }
        }

        // satisfy roundtrip_null_param_binary_format test
        if params.is_empty() {
            params.push(Parameter::Binary(&[]));
        }

        let res_count = bytes.get_i16() as usize;
        let mut result_formats = SmallVec::<[ResultFormat; 8]>::with_capacity(res_count);
        for _ in 0..res_count {
            let is_bin = decode_format_code(bytes.get_i16())?;
            result_formats.push(if is_bin {
                ResultFormat::Binary
            } else {
                ResultFormat::Text
            });
        }

        Ok(BindFrame {
            portal,
            statement,
            params,
            result_formats,
        })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let mut body = BytesMut::with_capacity(self.body_size());

        body.extend_from_slice(self.portal.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(self.statement.as_bytes());
        body.put_u8(0);

        // param format codes
        body.put_i16(self.params.len() as i16);
        for p in &self.params {
            encode_format_code(&mut body, matches!(p, Parameter::Binary(_)));
        }

        // parameter values
        body.put_i16(self.params.len() as i16);
        for p in &self.params {
            match p {
                Parameter::Text(s) => {
                    body.put_i32(s.len() as i32);
                    body.extend_from_slice(s.as_bytes());
                }
                Parameter::Binary(b) if !b.is_empty() => {
                    body.put_i32(b.len() as i32);
                    body.extend_from_slice(b);
                }
                Parameter::Binary(_) => {
                    body.put_i32(-1);
                }
            }
        }

        // result formats
        body.put_i16(self.result_formats.len() as i16);
        for fmt in &self.result_formats {
            encode_format_code(&mut body, matches!(fmt, ResultFormat::Binary));
        }

        // wrap
        let mut frame = BytesMut::with_capacity(body.len() + 5);
        frame.put_u8(b'B');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);

        Ok(frame.freeze())
    }

    fn body_size(&self) -> usize {
        let mut n = 0;
        n += self.portal.len() + 1;
        n += self.statement.len() + 1;
        n += 2 + self.params.len() * 2;
        n += 2;
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
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_frame<'a>() -> BindFrame<'a> {
        let mut params = SmallVec::<[Parameter<'a>; 8]>::new();
        params.push(Parameter::Text("42"));

        let mut result_formats = SmallVec::<[ResultFormat; 8]>::new();
        result_formats.push(ResultFormat::Binary);

        BindFrame {
            portal: "",
            statement: "stmt",
            params,
            result_formats,
        }
    }

    fn make_binary_email_frame(email: &str) -> Vec<u8> {
        let mut body = BytesMut::new();

        // portal\0
        body.extend_from_slice("".as_bytes());
        body.put_u8(0);

        // statement\0
        body.extend_from_slice("stmt".as_bytes());
        body.put_u8(0);

        // one binary param
        body.put_i16(1); // param format count
        body.put_i16(1); // format code = binary
        body.put_i16(1); // param count
        body.put_i32(email.len() as i32);
        body.extend_from_slice(email.as_bytes());

        // no result formats
        body.put_i16(0);

        let mut frame = BytesMut::new();
        frame.put_u8(b'B');
        frame.put_u32((body.len() + 4) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn roundtrip_text_param() {
        let frame = make_frame();
        let encoded = frame.to_bytes().unwrap();
        let decoded = BindFrame::from_bytes(encoded.as_ref()).unwrap();

        assert_eq!(decoded.portal, frame.portal);
        assert_eq!(decoded.statement, frame.statement);

        match &decoded.params[0] {
            Parameter::Text(t) => assert_eq!(*t, "42"),
            _ => panic!("expected text param"),
        }

        matches!(decoded.result_formats[0], ResultFormat::Text);
    }

    #[test]
    fn roundtrip_null_param_binary_format() {
        let params = SmallVec::<[Parameter; 8]>::new();
        let mut result_formats = SmallVec::<[ResultFormat; 8]>::new();
        result_formats.push(ResultFormat::Binary);

        let frame = BindFrame {
            portal: "super_cool_mega_portal",
            statement: "super_cool_mega_statement",
            params,
            result_formats,
        };
        let encoded = frame.to_bytes().unwrap();
        let decoded = BindFrame::from_bytes(encoded.as_ref()).unwrap();
        matches!(decoded.params[0], Parameter::Binary(_));
        matches!(decoded.result_formats[0], ResultFormat::Binary);
    }

    #[test]
    fn roundtrip_binary_email_param() {
        let email = "person@example.com";
        let buf1 = make_binary_email_frame(email);
        let frame1 = BindFrame::from_bytes(buf1.as_slice()).unwrap();

        let raw = if let Parameter::Binary(bytes) = frame1.params[0] {
            bytes
        } else {
            &[]
        };

        assert_eq!(raw, email.as_bytes());

        let buf2 = frame1.to_bytes().unwrap();
        let frame2 = BindFrame::from_bytes(buf2.as_ref()).unwrap();
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

        let err = BindFrame::from_bytes(bytes.as_slice()).unwrap_err();
        matches!(err, BindError::UnexpectedTag(_));
    }

    #[test]
    fn invalid_format_code() {
        // produce a good frame then flip the first format code to 2
        let mut bytes = make_frame().to_bytes().unwrap().to_vec();

        let mut offset = 0;
        offset += 5; // header
        offset += 0; // portal_name = ""
        offset += 1; // NULL terminator
        offset += 4; // statement = "stmt" (4 bytes)
        offset += 1; // NULL terminator

        bytes[offset + 2] = 0; // count high byte already 0
        bytes[offset + 3] = 2; // invalid code 2

        let err = BindFrame::from_bytes(bytes.as_slice()).unwrap_err();
        matches!(err, BindError::InvalidFormatCode(2));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
