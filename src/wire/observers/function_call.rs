use std::{fmt, str};

use crate::wire::utils::{parse_tagged_frame, peek_tagged_frame, TaggedFrameError};

// -----------------------------------------------------------------------------
// ----- FunctionCallFrameObserver ---------------------------------------------

#[derive(Clone, Copy, Debug)]
pub struct FunctionCallFrameObserver<'a> {
    frame: &'a [u8],
    oid: i32,
    param_format_count: usize,
    param_format_codes_start: usize,
    param_count: usize,
    param_values_start: usize,
    result_format_code: i16,
}

// -----------------------------------------------------------------------------
// ----- FunctionCallFrameObserver: Sub Structs --------------------------------

#[derive(Clone, Copy, Debug)]
pub enum ParamView<'a> {
    Null,
    Text(&'a str),
    Binary(&'a [u8]),
}

// -----------------------------------------------------------------------------
// ----- FunctionCallFrameObserver: Static -------------------------------------

impl<'a> FunctionCallFrameObserver<'a> {
    /// Cheap, peeks at the header-only. Returns total frame length if fully present.
    #[inline]
    pub fn peek(buf: &[u8]) -> Option<usize> {
        peek_tagged_frame(buf, b'F').map(|meta| meta.total_len)
    }

    /// Validate and build zero-copy observer over a complete frame slice.
    pub fn new(frame: &'a [u8]) -> Result<Self, NewFunctionCallObserverError> {
        let meta = match parse_tagged_frame(frame, b'F') {
            Ok(meta) => meta,
            Err(TaggedFrameError::UnexpectedTag(tag)) => {
                return Err(NewFunctionCallObserverError::UnexpectedTag(tag));
            }
            Err(TaggedFrameError::UnexpectedLength | TaggedFrameError::InvalidLength(_)) => {
                return Err(NewFunctionCallObserverError::UnexpectedLength);
            }
        };

        let total = meta.total_len;
        let mut pos = 5;

        // oid
        if pos + 4 > total {
            return Err(NewFunctionCallObserverError::UnexpectedEof);
        }
        let oid = be_i32(&frame[pos..]);
        pos += 4;

        // param format count
        if pos + 2 > total {
            return Err(NewFunctionCallObserverError::UnexpectedEof);
        }
        let param_format_count = be_u16(&frame[pos..]) as usize;
        pos += 2;

        // param format codes
        let need = pos + 2 * param_format_count;
        if need > total {
            return Err(NewFunctionCallObserverError::UnexpectedEof);
        }
        let param_format_codes_start = pos;
        for i in 0..param_format_count {
            let code = be_i16(&frame[param_format_codes_start + 2 * i..]);
            if code != 0 && code != 1 {
                return Err(NewFunctionCallObserverError::InvalidFormatCode(code));
            }
        }
        pos = need;

        // param count
        if pos + 2 > total {
            return Err(NewFunctionCallObserverError::UnexpectedEof);
        }
        let signed_param_count = be_i16(&frame[pos..]);
        if signed_param_count < 0 {
            return Err(NewFunctionCallObserverError::InvalidCount(
                signed_param_count,
            ));
        }
        let param_count = signed_param_count as usize;
        pos += 2;

        // param values; also validate UTF-8 for text params
        let param_values_start = pos;
        // Enforce spec: if there are per-parameter format codes, the count must equal param_count.
        // If param_format_count == 1, it applies to all; if >1, must match.
        if param_format_count > 1 && param_format_count != param_count {
            return Err(NewFunctionCallObserverError::ParamFormatCountMismatch {
                count: param_format_count,
                expected: param_count,
            });
        }

        for idx in 0..param_count {
            if pos + 4 > total {
                return Err(NewFunctionCallObserverError::UnexpectedEof);
            }
            let n = be_i32(&frame[pos..]);
            pos += 4;
            if n == -1 {
                // NULL param: nothing to consume
                continue;
            } else if n < -1 {
                return Err(NewFunctionCallObserverError::InvalidParamLength(n));
            }
            let n = n as usize;
            if pos + n > total {
                return Err(NewFunctionCallObserverError::UnexpectedEof);
            }
            let is_bin = match param_format_count {
                0 => false,
                1 => be_i16(&frame[param_format_codes_start..]) == 1,
                _ => {
                    // >1 must equal param_count (already enforced)
                    let off = param_format_codes_start + 2 * idx;
                    be_i16(&frame[off..]) == 1
                }
            };
            if !is_bin {
                let s = &frame[pos..pos + n];
                let _ = str::from_utf8(s).map_err(NewFunctionCallObserverError::InvalidUtf8)?;
            }
            pos += n;
        }

        // result format code
        if pos + 2 > total {
            return Err(NewFunctionCallObserverError::UnexpectedEof);
        }
        let result_format_code = be_i16(&frame[pos..]);
        if result_format_code != 0 && result_format_code != 1 {
            return Err(NewFunctionCallObserverError::InvalidFormatCode(
                result_format_code,
            ));
        }
        pos += 2;

        if pos != total {
            return Err(NewFunctionCallObserverError::UnexpectedLength);
        }

        Ok(Self {
            frame,
            oid,
            param_format_count,
            param_format_codes_start,
            param_count,
            param_values_start,
            result_format_code,
        })
    }
}

// -----------------------------------------------------------------------------
// ----- FunctionCallFrameObserver: Public -------------------------------------
impl<'a> FunctionCallFrameObserver<'a> {
    #[inline]
    pub fn oid(&self) -> i32 {
        self.oid
    }

    #[inline]
    pub fn param_count(&self) -> usize {
        self.param_count
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

    /// Result format helper (0 = text, 1 = binary).
    #[inline]
    pub fn result_is_binary(&self) -> bool {
        self.result_format_code == 1
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
pub enum NewFunctionCallObserverError {
    InvalidCount(i16),
    InvalidFormatCode(i16),
    InvalidParamLength(i32),
    InvalidUtf8(str::Utf8Error),
    ParamFormatCountMismatch { count: usize, expected: usize },
    UnexpectedEof,
    UnexpectedLength,
    UnexpectedTag(u8),
}

impl fmt::Display for NewFunctionCallObserverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use NewFunctionCallObserverError::*;
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

impl std::error::Error for NewFunctionCallObserverError {}

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

    fn build_frame(oid: i32, params: &[ParamSpec<'_>], result_binary: bool) -> Vec<u8> {
        let mut body = BytesMut::new();
        body.put_i32(oid);
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
        // result format
        body.put_i16(if result_binary { 1 } else { 0 });
        // wrap as a FunctionCall frame
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        frame.to_vec()
    }

    #[test]
    fn peek_then_new_minimal() {
        let frame = build_frame(123, &[], false);
        let len = FunctionCallFrameObserver::peek(&frame).unwrap();
        assert_eq!(len, frame.len());
        let obs = FunctionCallFrameObserver::new(&frame[..len]).unwrap();
        assert_eq!(obs.oid(), 123);
        assert_eq!(obs.param_count(), 0);
        assert!(!obs.result_is_binary());
    }

    #[test]
    fn text_param_and_formats() {
        let frame = build_frame(456, &[ParamSpec::Text("42"), ParamSpec::Text("")], true);
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let obs = FunctionCallFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.oid(), 456);
        assert_eq!(obs.param_count(), 2);
        assert!(!obs.param_is_binary(0));
        assert_eq!(obs.param_text(0), Some("42"));
        assert_eq!(obs.param_text(1), Some(""));
        assert!(obs.result_is_binary());
    }

    #[test]
    fn binary_param() {
        let frame = build_frame(789, &[ParamSpec::Binary(&[1, 2, 3])], false);
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let obs = FunctionCallFrameObserver::new(&frame[..total]).unwrap();
        assert!(obs.param_is_binary(0));
        assert_eq!(obs.param_raw(0).unwrap(), &[1, 2, 3]);
        matches!(obs.param(0), ParamView::Binary(_));
    }

    #[test]
    fn invalid_format_code_rejected() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(1);
        body.put_i16(2); // invalid
        body.put_u16(0);
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let err = FunctionCallFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewFunctionCallObserverError::InvalidFormatCode(2));
    }

    #[test]
    fn invalid_utf8_text_param_rejected() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(0); // all text
        body.put_u16(1);
        body.put_i32(2);
        body.extend_from_slice(&[0xFF, 0xFE]);
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let err = FunctionCallFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewFunctionCallObserverError::InvalidUtf8(_));
    }

    #[test]
    fn mixed_params() {
        let params = [
            ParamSpec::Text("text1"),
            ParamSpec::Binary(&[4, 5]),
            ParamSpec::Text("text2"),
        ];
        let frame = build_frame(101, &params, true);
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let obs = FunctionCallFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_count(), 3);
        assert!(!obs.param_is_binary(0));
        assert!(obs.param_is_binary(1));
        assert!(!obs.param_is_binary(2));
        assert_eq!(obs.param_text(0), Some("text1"));
        assert_eq!(obs.param_raw(1).unwrap(), &[4, 5]);
        assert_eq!(obs.param_text(2), Some("text2"));
        assert!(obs.result_is_binary());
    }

    #[test]
    fn null_and_empty_params() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(0); // all text
        body.put_u16(3);
        body.put_i32(-1); // NULL
        body.put_i32(0); // empty
        body.put_i32(-1); // NULL
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let obs = FunctionCallFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_count(), 3);
        assert_eq!(obs.param_text(0), None);
        assert_eq!(obs.param_text(1), Some(""));
        assert_eq!(obs.param_text(2), None);
    }

    #[test]
    fn invalid_param_length_negative_not_minus_one() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(0);
        body.put_u16(1);
        body.put_i32(-2); // invalid
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let err = FunctionCallFrameObserver::new(&frame).unwrap_err();
        matches!(err, NewFunctionCallObserverError::InvalidParamLength(-2));
    }

    #[test]
    fn param_format_count_mismatch() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(3); // 3 formats
        body.put_i16(0);
        body.put_i16(1);
        body.put_i16(0);
        body.put_u16(2); // but 2 params
        body.put_i32(1);
        body.put_u8(b'a');
        body.put_i32(1);
        body.put_u8(b'b');
        body.put_i16(0);
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let err = FunctionCallFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(
            err,
            NewFunctionCallObserverError::ParamFormatCountMismatch { .. }
        );
    }

    #[test]
    fn invalid_result_format_code() {
        let mut body = BytesMut::new();
        body.put_i32(123);
        body.put_u16(0);
        body.put_u16(0);
        body.put_i16(2); // invalid
        let mut frame = BytesMut::new();
        frame.put_u8(b'F');
        frame.put_u32((4 + body.len()) as u32);
        frame.extend_from_slice(&body);
        let frame = frame.to_vec();
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let err = FunctionCallFrameObserver::new(&frame[..total]).unwrap_err();
        matches!(err, NewFunctionCallObserverError::InvalidFormatCode(2));
    }

    #[test]
    fn peek_rejects_incomplete_frame_and_new_rejects_length_mismatch() {
        let frame_ok = build_frame(123, &[], false);
        let mut truncated = frame_ok.clone();
        truncated.pop();
        assert!(FunctionCallFrameObserver::peek(&truncated).is_none());
        let mut with_junk = frame_ok.clone();
        with_junk.push(0);
        let err = FunctionCallFrameObserver::new(&with_junk).unwrap_err();
        matches!(err, NewFunctionCallObserverError::UnexpectedLength);
    }

    #[test]
    fn new_rejects_wrong_tag_and_peek_ignores_non_function_call() {
        let bogus = vec![b'X', 0, 0, 0, 4];
        assert!(FunctionCallFrameObserver::peek(&bogus).is_none());
        let err = FunctionCallFrameObserver::new(&bogus).unwrap_err();
        matches!(err, NewFunctionCallObserverError::UnexpectedTag(b'X'));
    }

    #[test]
    fn two_frames_back_to_back_in_a_stream() {
        let f1 = build_frame(123, &[ParamSpec::Text("a")], false);
        let f2 = build_frame(456, &[ParamSpec::Binary(&[1])], true);
        let mut stream = Vec::with_capacity(f1.len() + f2.len());
        stream.extend_from_slice(&f1);
        stream.extend_from_slice(&f2);
        // frame 1
        let t1 = FunctionCallFrameObserver::peek(&stream).unwrap();
        let obs1 = FunctionCallFrameObserver::new(&stream[..t1]).unwrap();
        assert_eq!(obs1.param_text(0), Some("a"));
        assert!(!obs1.result_is_binary());
        // frame 2
        let t2 = FunctionCallFrameObserver::peek(&stream[t1..]).unwrap();
        let obs2 = FunctionCallFrameObserver::new(&stream[t1..t1 + t2]).unwrap();
        assert_eq!(obs2.param_raw(0).unwrap(), &[1]);
        assert!(obs2.result_is_binary());
    }

    #[test]
    fn zero_copy_param_slice_aliases_frame_memory() {
        let frame = build_frame(123, &[ParamSpec::Text("hey")], false);
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let frame_slice = &frame[..total];
        let obs = FunctionCallFrameObserver::new(frame_slice).unwrap();
        let p = obs.param_raw(0).unwrap();
        let base = frame_slice.as_ptr() as usize;
        let ptr = p.as_ptr() as usize;
        assert!(ptr >= base && ptr < base + frame_slice.len());
    }

    #[test]
    fn large_param_set_stress() {
        let count = 64usize;

        let param_storage: Vec<String> = (0..count).map(|i| format!("v{}", i)).collect();

        let params: Vec<ParamSpec> = param_storage
            .iter()
            .map(|s| ParamSpec::Text(s.as_str()))
            .collect();

        let frame = build_frame(123, &params, false);
        let total = FunctionCallFrameObserver::peek(&frame).unwrap();
        let obs = FunctionCallFrameObserver::new(&frame[..total]).unwrap();
        assert_eq!(obs.param_count(), count);
        assert_eq!(obs.param_text(0), Some("v0"));
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
