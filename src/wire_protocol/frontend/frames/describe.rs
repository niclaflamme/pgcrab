//! Module: wire_protocol::frontend::describe
//!
//! Unified Describe message for Portal ('P') and Statement ('S')

use bytes::{BufMut, Bytes, BytesMut};
use std::{error::Error as StdError, fmt};

use crate::wire_protocol::WireSerializable;

// -----------------------------------------------------------------------------
// ----- ProtocolMessage -------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescribeFrame<'a> {
    pub kind: DescribeKind,
    pub name: &'a str,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DescribeKind {
    Portal,    // 'P'
    Statement, // 'S'
}

// -----------------------------------------------------------------------------
// ----- Error -----------------------------------------------------------------

#[derive(Debug)]
pub enum DescribeError {
    UnexpectedTag(u8),
    UnexpectedKind(u8),
    UnexpectedLength(u32),
    NoTerminator,
    InvalidUtf8(std::str::Utf8Error),
}

impl fmt::Display for DescribeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedTag(t) => write!(f, "unexpected tag: {:#X}", t),
            Self::UnexpectedKind(k) => write!(f, "unexpected kind: {:#X}", k),
            Self::UnexpectedLength(n) => write!(f, "unexpected length: {}", n),
            Self::NoTerminator => write!(f, "missing null terminator"),
            Self::InvalidUtf8(e) => write!(f, "invalid UTF-8: {}", e),
        }
    }
}

impl StdError for DescribeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match self {
            Self::InvalidUtf8(e) => Some(e),
            _ => None,
        }
    }
}

impl<'a> WireSerializable<'a> for DescribeFrame<'a> {
    type Error = DescribeError;

    fn peek(_buf: &BytesMut) -> Option<usize> {
        None
    }

    fn from_bytes(bytes: &'a [u8]) -> Result<Self, Self::Error> {
        if bytes.len() < 7 {
            return Err(DescribeError::UnexpectedLength(bytes.len() as u32));
        }
        if bytes[0] != b'D' {
            return Err(DescribeError::UnexpectedTag(bytes[0]));
        }

        let declared = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]);
        if declared as usize != bytes.len() - 1 {
            return Err(DescribeError::UnexpectedLength(declared));
        }

        let kind = match bytes[5] {
            b'P' => DescribeKind::Portal,
            b'S' => DescribeKind::Statement,
            k => return Err(DescribeError::UnexpectedKind(k)),
        };

        let rest = &bytes[6..];
        let nul = rest
            .iter()
            .position(|&b| b == 0)
            .ok_or(DescribeError::NoTerminator)?;
        if nul + 1 != rest.len() {
            return Err(DescribeError::NoTerminator);
        }

        let name = std::str::from_utf8(&rest[..nul]).map_err(DescribeError::InvalidUtf8)?;
        Ok(DescribeFrame { kind, name })
    }

    fn to_bytes(&self) -> Result<Bytes, Self::Error> {
        let n = self.name.as_bytes();
        let body_len = 1 + n.len() + 1;
        let total = 4 + body_len as u32;

        let mut buf = BytesMut::with_capacity(1 + total as usize);
        buf.put_u8(b'D');
        buf.put_u32(total);
        buf.put_u8(match self.kind {
            DescribeKind::Portal => b'P',
            DescribeKind::Statement => b'S',
        });
        buf.put_slice(n);
        buf.put_u8(0);
        Ok(buf.freeze())
    }

    fn body_size(&self) -> usize {
        1 + self.name.len() + 1
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make(kind: DescribeKind, name: &str) -> DescribeFrame {
        DescribeFrame { kind, name }
    }

    #[test]
    fn empty_portal() {
        let f = make(DescribeKind::Portal, "");
        let enc = f.to_bytes().unwrap();
        let dec = DescribeFrame::from_bytes(enc.as_ref()).unwrap();
        assert_eq!(f, dec);
    }

    #[test]
    fn named_statement() {
        let f = make(DescribeKind::Statement, "stmt1");
        let enc = f.to_bytes().unwrap();
        let dec = DescribeFrame::from_bytes(enc.as_ref()).unwrap();
        assert_eq!(f, dec);
    }

    #[test]
    fn bad_tag() {
        let mut buf = make(DescribeKind::Portal, "x").to_bytes().unwrap().to_vec();
        buf[0] = b'X';
        assert!(matches!(
            DescribeFrame::from_bytes(&buf),
            Err(DescribeError::UnexpectedTag(b'X'))
        ));
    }

    #[test]
    fn bad_kind() {
        let mut buf = make(DescribeKind::Portal, "x").to_bytes().unwrap().to_vec();
        buf[5] = b'Z';
        assert!(matches!(
            DescribeFrame::from_bytes(&buf),
            Err(DescribeError::UnexpectedKind(b'Z'))
        ));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
