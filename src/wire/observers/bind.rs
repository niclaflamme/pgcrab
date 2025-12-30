// src/wire/frontend/frames/bind_observer.rs

use memchr::memchr;
use std::{fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- BindFrameObserver -----------------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct BindFrameObserver<'a> {
    frame: &'a [u8],

    portal: &'a str,
    statement: &'a str,

    param_format_count: usize,
    param_format_codes_start: usize,

    param_count: usize,
    param_values_start: usize,

    result_format_count: usize,
    result_format_codes_start: usize,
}

// -----------------------------------------------------------------------------
// ----- BindFrameObserver: Sub Structs ----------------------------------------

#[derive(Clone, Copy, Debug)]
pub enum ParamView<'a> {
    Null,
    Text(&'a str),
    Binary(&'a [u8]),
}

// -----------------------------------------------------------------------------
// ----- BindFrameObserver: Static ---------------------------------------------

impl<'a> BindFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'B').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewBindObserverError> {
        let meta = match parse_tagged_frame(frame, b'B') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewBindObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewBindObserverError::UnexpectedLength);
            }
        };

        let total = meta.total_len;
        let mut pos = 5;

        // portal
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewBindObserverError::UnexpectedEof)?;
        let portal =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewBindObserverError::InvalidUtf8)?;
        pos += rel + 1;

        // statement
        let rel = memchr(0, &frame[pos..meta.total_len])
            .ok_or(NewBindObserverError::UnexpectedEof)?;
        let statement =
            str::from_utf8(&frame[pos..pos + rel]).map_err(NewBindObserverError::InvalidUtf8)?;
        pos += rel + 1;

        // param format count
        if pos + 2 > meta.total_len {
            return Err(NewBindObserverError::UnexpectedEof);
        }
        let param_format_count = be_u16(&frame[pos..]) as usize;
        pos += 2;

        // param format codes
        let need = pos + 2 * param_format_count;
        if need > total {
            return Err(NewBindObserverError::UnexpectedEof);
        }
        let param_format_codes_start = pos;
        for i in 0..param_format_count {
            let code = be_i16(&frame[param_format_codes_start + 2 * i..]);
            if code != 0 && code != 1 {
                return Err(NewBindObserverError::InvalidFormatCode(code));
            }
        }
        pos = need;

        // param count (reject negative Int16)
        if pos + 2 > total {
            return Err(NewBindObserverError::UnexpectedEof);
        }
        let signed_param_count = be_i16(&frame[pos..]);
        if signed_param_count < 0 {
            return Err(NewBindObserverError::InvalidCount(signed_param_count));
        }
        let param_count = signed_param_count as usize;
        pos += 2;

        // param values; also validate UTF-8 for text params
        let param_values_start = pos;

        // Enforce spec: if there are per-parameter format codes, the count must equal param_count.
        if param_format_count > 1 && param_format_count != param_count {
            return Err(NewBindObserverError::ParamFormatCountMismatch {
                count: param_format_count,
                expected: param_count,
            });
        }

        for idx in 0..param_count {
            if pos + 4 > total {
                return Err(NewBindObserverError::UnexpectedEof);
            }
            let n = be_i32(&frame[pos..]);
            pos += 4;

            if n == -1 {
                // NULL param: nothing to consume
                continue;
            } else if n < -1 {
                // this is your failing case
                return Err(NewBindObserverError::InvalidParamLength(n));
            }

            let n = n as usize;
            if pos + n > total {
                return Err(NewBindObserverError::UnexpectedEof);
            }

            let is_bin = match param_format_count {
                0 => false,
                1 => be_i16(&frame[param_format_codes_start..]) == 1,
                _ => {
                    // m > 1 must equal param_count (already enforced)
                    let off = param_format_codes_start + 2 * idx;
                    be_i16(&frame[off..]) == 1
                }
            };

            if !is_bin {
                let s = &frame[pos..pos + n];
                let _ = str::from_utf8(s).map_err(NewBindObserverError::InvalidUtf8)?;
            }

            pos += n;
        }

        // result formats
        if pos + 2 > total {
            return Err(NewBindObserverError::UnexpectedEof);
        }
        let result_format_count = be_u16(&frame[pos..]) as usize;
        pos += 2;

        let result_format_codes_start = pos;
        let need = pos + 2 * result_format_count;
        if need > total {
            return Err(NewBindObserverError::UnexpectedEof);
        }
        for i in 0..result_format_count {
            let code = be_i16(&frame[result_format_codes_start + 2 * i..]);
            if code != 0 && code != 1 {
                return Err(NewBindObserverError::InvalidFormatCode(code));
            }
        }
        pos = need;

        if pos != total {
            return Err(NewBindObserverError::UnexpectedLength);
        }

        Ok(Self {
            frame,
            portal,
            statement,
            param_format_count,
            param_format_codes_start,
            param_count,
            param_values_start,
            result_format_count,
            result_format_codes_start,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- BindFrameObserver: Public ---------------------------------------------

impl<'a> BindFrameObserver<'a> {
    #[inline]
    pub fn portal(&self) -> &'a str {
        self.portal
    }

    #[inline]
    pub fn statement(&self) -> &'a str {
        self.statement
    }

    #[inline]
    pub fn param_count(&self) -> usize {
        self.param_count
    }

    #[inline]
    pub fn result_format_count(&self) -> usize {
        self.result_format_count
    }

    #[inline]
    pub fn param_is_binary(&self, index: usize) -> bool {
        debug_assert!(index < self.param_count);
        match self.param_format_count {
            0 => false, // all text
            1 => be_i16(&self.frame[self.param_format_codes_start..]) == 1,
            m => {
                // m > 1 must equal param_count (enforced in new())
                debug_assert_eq!(m, self.param_count);
                let off = self.param_format_codes_start + 2 * index;
                be_i16(&self.frame[off..]) == 1
            }
        }
    }

    /// Result-column format helper (0 = text, 1 = binary; PG applies single code to all).
    #[inline]
    pub fn result_is_binary(&self, index: usize) -> bool {
        match self.result_format_count {
            0 => false, // default: all text
            1 => be_i16(&self.frame[self.result_format_codes_start..]) == 1,
            m => {
                // No tail-fill, no panics: out-of-range -> false.
                if index >= m {
                    return false;
                }
                let off = self.result_format_codes_start + 2 * index;
                be_i16(&self.frame[off..]) == 1
            }
        }
    }

    /// None = SQL NULL. Slice borrows from the frame.
    pub fn param_raw(&self, index: usize) -> Option<&'a [u8]> {
        debug_assert!(index < self.param_count);
        let mut pos = self.param_values_start;
        for i in 0..=index {
            let n = be_i32(&self.frame[pos..]);
            pos += 4;
            if i == index {
                if n < 0 {
                    return None;
                }
                let n = n as usize;
                return Some(&self.frame[pos..pos + n]);
            } else if n >= 0 {
                pos += n as usize;
            }
        }
        unreachable!("validated in new()");
    }

    /// Panics in debug if called for a binary param (we validated text UTF-8 in new()).
    pub fn param_text(&self, index: usize) -> Option<&'a str> {
        debug_assert!(!self.param_is_binary(index));
        match self.param_raw(index) {
            None => None,
            Some(bytes) => Some(unsafe { str::from_utf8_unchecked(bytes) }),
        }
    }

    pub fn param(&self, index: usize) -> ParamView<'a> {
        if self.param_is_binary(index) {
            match self.param_raw(index) {
                None => ParamView::Null,
                Some(b) => ParamView::Binary(b),
            }
        } else {
            match self.param_text(index) {
                None => ParamView::Null,
                Some(s) => ParamView::Text(s),
            }
        }
    }
}

// -----------------------------------------------------------------------------
// ----- Errors ----------------------------------------------------------------

#[derive(Debug)]
pub enum NewBindObserverError {
    InvalidCount(i16),
    InvalidFormatCode(i16),
    InvalidParamLength(i32),
    InvalidUtf8(str::Utf8Error),
    ParamFormatCountMismatch { count: usize, expected: usize },
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewBindObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewBindObserverError::*;
        match self {
            InvalidCount(c) => write!(f, "invalid count: {c}"),
            InvalidFormatCode(c) => write!(f, "invalid format code: {c}"),
            InvalidParamLength(l) => write!(f, "invalid param length: {l}"),
            InvalidUtf8(e) => write!(f, "utf8: {e}"),
            ParamFormatCountMismatch { count, expected } => write!(
                f,
                "parameter format count mismatch: expected {expected}, got {count}"
            ),
            UnexpectedEof => write!(f, "unexpected EOF"),
            UnexpectedLength => write!(f, "unexpected length"),
            UnexpectedTag(t) => write!(f, "unexpected tag: {t:#X}"),
        }
    }
}

impl std::error::Error for NewBindObserverError {}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

#[inline]
fn be_u16(x: &[u8]) -> u16 {
    u16::from_be_bytes([x[0], x[1]])
}

#[inline]
fn be_i16(x: &[u8]) -> i16 {
    i16::from_be_bytes([x[0], x[1]])
}

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

    #[derive(Debug, Clone, Copy)]
    enum ParamSpec<'a> {
        Text(&'a str),
        Binary(&'a [u8]),
    }

    fn build_frame<F: FnOnce(&mut BytesMut)>(f: F) -> Vec<u8> {
        let mut body = BytesMut::new();
        f(&mut body);

        let mut frame = BytesMut::new();
        frame.put_u8(b'B');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_minimal() {
        let frame = build_frame(|b| {
            b.put_u8(0); // portal
            b.put_u8(0); // statement
            b.put_u16(0); // fmt count
            b.put_u16(0); // param count
            b.put_u16(0); // result fmt count
        });

        let len = BindFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());

        let obs = BindFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.portal(), "");
        assert_eq!(obs.statement(), "");
        assert_eq!(obs.param_count(), 0);
        assert_eq!(obs.result_format_count(), 0);
    }

    #[test]
    fn text_param_and_formats() {
        let frame = build_frame(|b| {
            b.extend_from_slice(b"p");
            b.put_u8(0);
            b.extend_from_slice(b"s");
            b.put_u8(0);
            b.put_u16(1);
            b.put_i16(0); // one text code for all
            b.put_u16(2); // two params
            b.put_i32(2);
            b.extend_from_slice(b"42");
            b.put_i32(-1); // NULL
            b.put_u16(1);
            b.put_i16(1); // result fmt binary
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert_eq!(obs.portal(), "p");
        assert_eq!(obs.statement(), "s");
        assert_eq!(obs.param_count(), 2);
        assert!(!obs.param_is_binary(0));
        assert_eq!(obs.param_text(0), Some("42"));
        matches!(obs.param(1), ParamView::Null);
        assert!(obs.result_is_binary(0));
    }

    #[test]
    fn binary_param() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(1);
            b.put_i16(1); // binary for all
            b.put_u16(1);
            b.put_i32(3);
            b.extend_from_slice(&[1, 2, 3]);
            b.put_u16(0);
        });
        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();
        assert!(obs.param_is_binary(0));
        assert_eq!(obs.param_raw(0).unwrap(), &[1, 2, 3]);
        matches!(obs.param(0), ParamView::Binary(_));
    }

    #[test]
    fn invalid_format_code_rejected() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(1);
            b.put_i16(2); // invalid
            b.put_u16(0);
            b.put_u16(0);
        });
        let total = BindFrameObserver::peek(&frame).unwrap();
        let err = BindFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewBindObserverError::InvalidFormatCode(2));
    }

    #[test]
    fn invalid_utf8_text_param_rejected() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0); // all text
            b.put_u16(1);
            b.put_i32(2);
            b.extend_from_slice(&[0xFF, 0xFE]);
            b.put_u16(0);
        });
        let total = BindFrameObserver::peek(&frame).unwrap();
        let err = BindFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewBindObserverError::InvalidUtf8(_));
    }

    #[test]
    fn real_world_bind_with_uuid_email_pairs_text_params() {
        // INSERT INTO users (id, email)
        // VALUES ($1, $2), ($3, $4), ($5, $6), ($7, $8), ($9, $10);
        let expected_params = [
            "018c7b7a-1f4b-7c8d-a1b2-c3d4e5f67890",
            "person_001@example.com",
            "018c7b7a-1f4b-7c8d-a1b2-c3d4e5f67891",
            "person_002@example.com",
            "018c7b7a-1f4b-7c8d-a1b2-c3d4e5f67892",
            "person_003@example.com",
            "018c7b7a-1f4b-7c8d-a1b2-c3d4e5f67893",
            "person_004@example.com",
            "018c7b7a-1f4b-7c8d-a1b2-c3d4e5f67894",
            "person_005@example.com",
        ];

        let frame = build_frame(|b| {
            // portal="", statement=""
            b.put_u8(0);
            b.put_u8(0);

            // param formats: 0 => all text
            b.put_u16(0);

            // params
            b.put_u16(expected_params.len() as u16);
            for slice in expected_params.iter() {
                b.put_i32(slice.len() as i32);
                b.extend_from_slice(slice.as_bytes());
            }

            // result formats: none (defaults to text)
            b.put_u16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert_eq!(obs.portal(), "");
        assert_eq!(obs.statement(), "");
        assert_eq!(obs.param_count(), expected_params.len());
        assert_eq!(obs.result_format_count(), 0);

        for (i, expected) in expected_params.iter().enumerate() {
            assert!(!obs.param_is_binary(i));
            assert_eq!(obs.param_text(i), Some(*expected));

            match obs.param(i) {
                ParamView::Text(s) => assert_eq!(s, *expected),
                _ => panic!("param {i} should be text"),
            }
        }

        // sanity: with 0 result formats, everything is text
        assert!(!obs.result_is_binary(0));
    }

    fn build_bind_frame_with_params(
        portal: &str,
        statement: &str,
        params: &[ParamSpec<'_>],
    ) -> Vec<u8> {
        use bytes::BufMut;

        let mut body = BytesMut::new();

        // portal, statement
        body.extend_from_slice(portal.as_bytes());
        body.put_u8(0);
        body.extend_from_slice(statement.as_bytes());
        body.put_u8(0);

        // param format codes
        let all_text = params.iter().all(|p| matches!(p, ParamSpec::Text(_)));
        let all_bin = params.iter().all(|p| matches!(p, ParamSpec::Binary(_)));

        if all_text {
            body.put_u16(0); // all text
        } else if all_bin {
            body.put_u16(1); // one code for all
            body.put_i16(1);
        } else {
            // per-parameter codes (spec-correct; length == param_count)
            body.put_u16(params.len() as u16);
            for p in params {
                match p {
                    ParamSpec::Text(_) => body.put_i16(0),
                    ParamSpec::Binary(_) => body.put_i16(1),
                }
            }
        }

        // param values
        body.put_u16(params.len() as u16);
        for p in params {
            match p {
                ParamSpec::Text(s) => {
                    body.put_i32(s.len() as i32);
                    body.extend_from_slice(s.as_bytes());
                }
                ParamSpec::Binary(b) => {
                    body.put_i32(b.len() as i32);
                    body.extend_from_slice(b);
                }
            }
        }

        // result formats: none (defaults to text)
        body.put_u16(0);

        // wrap as a Bind frame
        let mut frame = BytesMut::new();
        frame.put_u8(b'B');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn bind_declarative_params_builder_mixed_formats() {
        let portal = "portal1";
        let statement = "stmt1";

        // parms, mixed text and binary formats
        let param_1 = ("text", "A");
        let param_2 = ("binary", [1u8, 2, 3]);
        let param_3 = ("text", "B");
        let param_4 = ("text", "C");

        let frame = build_bind_frame_with_params(
            portal,
            statement,
            &[
                ParamSpec::Text(param_1.1),
                ParamSpec::Binary(&param_2.1),
                ParamSpec::Text(param_3.1),
                ParamSpec::Text(param_4.1),
            ],
        );

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert_eq!(obs.portal(), portal);
        assert_eq!(obs.statement(), statement);
        assert_eq!(obs.param_count(), 4);

        assert!(!obs.param_is_binary(0));
        assert!(obs.param_is_binary(1));
        assert!(!obs.param_is_binary(2));
        assert!(!obs.param_is_binary(3));

        assert_eq!(obs.param_text(0), Some(param_1.1));
        assert_eq!(obs.param_raw(1).unwrap(), &param_2.1);
        assert_eq!(obs.param_text(2), Some(param_3.1));
        assert_eq!(obs.param_text(3), Some(param_4.1));
    }

    #[test]
    fn bind_with_nulls_and_empty_string_text_params() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);

            b.put_u16(0); // all text

            b.put_u16(4);
            b.put_i32(-1); // NULL
            b.put_i32(1);
            b.extend_from_slice(b"x");
            b.put_i32(-1); // NULL
            b.put_i32(0); // empty string

            b.put_u16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert_eq!(obs.param_count(), 4);
        assert_eq!(obs.param_text(0), None);
        assert_eq!(obs.param_text(1), Some("x"));
        assert_eq!(obs.param_text(2), None);
        assert_eq!(obs.param_text(3), Some(""));
    }

    #[test]
    fn bind_binary_param_with_zero_bytes_and_result_formats_mixed() {
        let bytes = [0u8, 1, 2, 0, 255];

        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);

            b.put_u16(1);
            b.put_i16(1); // all binary

            b.put_u16(1);
            b.put_i32(bytes.len() as i32);
            b.extend_from_slice(&bytes);

            // result formats: [binary, text]
            b.put_u16(2);
            b.put_i16(1);
            b.put_i16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert!(obs.param_is_binary(0));
        assert_eq!(obs.param_raw(0).unwrap(), &bytes);
        assert!(obs.result_is_binary(0));
        assert!(!obs.result_is_binary(1));
    }

    #[test]
    fn bind_non_ascii_portal_and_statement() {
        let portal = "ポータル";
        let statement = "stmt✓";

        let frame = build_frame(|b| {
            b.extend_from_slice(portal.as_bytes());
            b.put_u8(0);
            b.extend_from_slice(statement.as_bytes());
            b.put_u8(0);

            b.put_u16(0);
            b.put_u16(0);
            b.put_u16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.portal(), portal);
        assert_eq!(obs.statement(), statement);
    }

    #[test]
    fn bind_two_frames_back_to_back_in_a_stream() {
        let make = |s: &str| {
            build_frame(|b| {
                b.put_u8(0);
                b.extend_from_slice(b"s");
                b.put_u8(0);
                b.put_u16(0);
                b.put_u16(1);
                b.put_i32(s.len() as i32);
                b.extend_from_slice(s.as_bytes());
                b.put_u16(0);
            })
        };
        let f1 = make("alpha");
        let f2 = make("beta");
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);

        // frame 1
        let t1 = BindFrameObserver::peek(&stream).unwrap();
        let obs1 = BindFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.param_text(0), Some("alpha"));

        // frame 2
        let t2 = BindFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = BindFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.param_text(0), Some("beta"));
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        // build valid then truncate 1 byte -> peek None
        let frame_ok = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0);
            b.put_u16(0);
            b.put_u16(0);
        });

        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(BindFrameObserver::peek(&truncated).is_none());

        // append junk -> new() on full slice must error UnexpectedLength
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = BindFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewBindObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_bind() {
        let bogus = vec![b'X', 0, 0, 0, 4];
        assert!(BindFrameObserver::peek(&bogus).is_none());
        let err = BindFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewBindObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn large_param_set_stress() {
        let count = 64usize;
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0); // all text
            b.put_u16(count as u16);
            for i in 0..count {
                let s = format!("v{}", i);
                b.put_i32(s.len() as i32);
                b.extend_from_slice(s.as_bytes());
            }
            b.put_u16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_count(), count);
        assert_eq!(obs.param_text(0), Some("v0"));
    }

    #[test]
    fn zero_copy_param_slice_aliases_frame_memory() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0);
            b.put_u16(1);
            b.put_i32(3);
            b.extend_from_slice(b"hey");
            b.put_u16(0);
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = BindFrameObserver::new(frame_slice).unwrap();

        let p = obs.param_raw(0).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let ptr = p.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }

    #[test]
    fn reject_param_format_count_mismatch() {
        // 2 params but 3 per-param format codes => protocol violation
        let frame = build_frame(|b| {
            b.put_u8(0); // portal
            b.put_u8(0); // statement
            b.put_u16(3); // param format codes count = 3
            b.put_i16(0);
            b.put_i16(1);
            b.put_i16(0);
            b.put_u16(2); // param_count = 2
            // two params (text then binary)
            b.put_i32(1);
            b.extend_from_slice(b"a");
            b.put_i32(2);
            b.extend_from_slice(&[1, 2]);
            b.put_u16(0); // result fmts
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let err = BindFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewBindObserverError::ParamFormatCountMismatch { .. });
    }

    #[test]
    fn result_formats_no_tail_fill() {
        // m=2 result formats; ask index 0,1 valid; index 2 should default false (and assert in debug)
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0); // params text
            b.put_u16(0); // zero params
            b.put_u16(2); // result format count = 2
            b.put_i16(1); // binary
            b.put_i16(0); // text
        });

        let total = BindFrameObserver::peek(&frame).unwrap();
        let obs = BindFrameObserver::new(&frame[..total]).unwrap();

        assert!(obs.result_is_binary(0));
        assert!(!obs.result_is_binary(1));
        assert!(!obs.result_is_binary(2)); // out of range -> false (debug assert guards)
    }

    #[test]
    fn invalid_negative_param_count() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0);
            b.put_i16(-1); // instead of u16
            b.put_u16(0);
        });
        let err = BindFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewBindObserverError::InvalidCount(-1));
    }

    #[test]
    fn invalid_param_length_negative_not_minus_one() {
        let frame = build_frame(|b| {
            b.put_u8(0);
            b.put_u8(0);
            b.put_u16(0);
            b.put_u16(1);
            b.put_i32(-2); // invalid
            b.put_u16(0);
        });
        let err = BindFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewBindObserverError::InvalidParamLength(-2));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
