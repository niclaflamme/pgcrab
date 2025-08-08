use bytes::{Buf, BytesMut};

use crate::wire_protocol::WireSerializable;
use crate::wire_protocol::frontend::FrontendProtocolMessage;
use crate::wire_protocol::frontend::error::ParseError;
use crate::wire_protocol::frontend::frames;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SSL_REQUEST_CODE: u32 = 0x04D2_162F; // 80877103
const GSSENC_REQUEST_CODE: u32 = 0x04D2_1630; // 80877104
const CANCEL_REQUEST_CODE: u32 = 0x04D2_162E; // 80877102
const MIN_STARTUP_VERSION: u32 = 0x0003_0000; // Minimum protocol version for StartupFrame (3.0)
const MAX_STARTUP_SIZE: usize = 10240; // 10KB, reasonable upper bound for Startup messages

const COMPACT_BUFFER_THRESHOLD: usize = 8 * 1024;

// -----------------------------------------------------------------------------
// ----- FrontendDecoder -------------------------------------------------------

pub struct FrontendDecoder {
    buffer: BytesMut,
    cursor: usize,
    state: ClientState,
}

impl FrontendDecoder {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(cap),
            cursor: 0,
            state: ClientState::Initial,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendDecoder: Sub Structs ------------------------------------------

#[derive(Copy, Clone, PartialEq)]
pub enum ClientState {
    Initial,
    Authenticating(AuthExpectation),
    Active,
}

#[derive(Copy, Clone, PartialEq)]
pub enum AuthExpectation {
    None,
    PasswordLike,
    SASLInitial,
    SASLResponse,
    GSSResponse,
    SSPIResponse,
}

// -----------------------------------------------------------------------------
// ----- FrontendDecoder: Public Methods ---------------------------------------

impl FrontendDecoder {
    /// Return the next complete message if one is buffered.
    pub fn next(&mut self) -> Option<FrontendProtocolMessage> {
        self.maybe_compact();

        // 1. All complete messages have a minimum of 5-byte length.
        if self.remaining() < 5 {
            return None;
        }

        // 2. Parse the untagged frame if it exists.
        if self.has_untagged_frame() {
            let frame = self.parse_untagged().expect("Decoder in invalid state");
            return Some(frame);
        }

        // 3. Parse the tagged frame if it exists.
        if self.has_tagged_frame() {
            let frame = self.parse_tagged().expect("Decoder in invalid state");
            return Some(frame);
        }

        None
    }

    /// Set the authentication expectation
    pub fn set_state(&mut self, state: ClientState) {
        self.state = state;
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendDecoder: Utils ------------------------------------------------

impl FrontendDecoder {
    fn maybe_compact(&mut self) {
        if self.cursor > COMPACT_BUFFER_THRESHOLD {
            self.compact();
        }
    }

    fn compact(&mut self) {
        if self.cursor > 0 {
            self.buffer.advance(self.cursor);
            self.cursor = 0;
        }
    }

    fn remaining(&self) -> usize {
        self.buffer.len() - self.cursor
    }

    fn has_untagged_frame(&self) -> bool {
        // bytes already in the buffer after the current cursor
        if self.remaining() < 8 {
            return false;
        }

        let header_slice = &self.buffer[self.cursor..];

        // first four bytes are the length (includes itself)
        let frame_len = u32::from_be_bytes(header_slice[..4].try_into().unwrap()) as usize;

        // malformed length or not enough bytes yet
        if frame_len < 8 || self.remaining() < frame_len {
            return false;
        }

        // next four bytes are the request / version code
        let request_code = u32::from_be_bytes(header_slice[4..8].try_into().unwrap());

        let is_ssl_request = request_code == SSL_REQUEST_CODE && frame_len == 8;
        let is_gssenc_request = request_code == GSSENC_REQUEST_CODE && frame_len == 8;
        let is_cancel_request = request_code == CANCEL_REQUEST_CODE && frame_len == 16;

        // Startup message is a VERY special case
        let is_startup_message = request_code >= MIN_STARTUP_VERSION
            && frame_len <= MAX_STARTUP_SIZE
            && !is_ssl_request
            && !is_gssenc_request
            && !is_cancel_request;

        is_ssl_request || is_gssenc_request || is_cancel_request || is_startup_message
    }

    fn has_tagged_frame(&self) -> bool {
        if self.remaining() < 5 {
            return false;
        }

        let start = self.cursor;

        let length_bytes = self.buffer[start + 1..start + 5].try_into().unwrap();
        let length_value = u32::from_be_bytes(length_bytes) as usize;

        // Incomplete frame, wait for more data
        if self.remaining() < 1 + length_value {
            return false;
        }

        true
    }

    fn parse_untagged(&mut self) -> Result<FrontendProtocolMessage, ParseError> {
        use FrontendProtocolMessage::*;

        // should never happen because we check during has_untagged_frame()
        if self.remaining() < 8 {
            return Err(ParseError::Unparsable);
        }

        let buf = &self.buffer[self.cursor..];
        let frame_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;

        // Wait until the full frame is buffered.
        if self.remaining() < frame_len {
            return Err(ParseError::Unparsable);
        }

        let code = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let frame = &buf[..frame_len];
        self.cursor += frame_len;

        // SSLRequest (8 bytes, fixed code)
        if code == SSL_REQUEST_CODE && frame_len == 8 {
            use frames::ssl_request::SslRequestFrame;

            let message = SslRequest(SslRequestFrame::from_bytes(frame)?);
            return Ok(message);
        }

        // CancelRequest (16 bytes, fixed code)
        if code == CANCEL_REQUEST_CODE && frame_len == 16 {
            use frames::cancel_request::CancelRequestFrame;

            let message = CancelRequest(CancelRequestFrame::from_bytes(frame)?);
            return Ok(message);
        }

        // GSSENCRequest (8 bytes, fixed code)
        if code == GSSENC_REQUEST_CODE && frame_len == 8 {
            use frames::gssenc_request::GssencRequestFrame;

            let message = GssEncRequest(GssencRequestFrame::from_bytes(frame)?);
            return Ok(message);
        }

        // StartupMessage (length-prefixed, version ≥ 3.0)
        if code >= MIN_STARTUP_VERSION {
            use frames::startup::StartupFrame;

            let message = Startup(StartupFrame::from_bytes(frame)?);
            return Ok(message);
        }

        Err(ParseError::Unparsable)
    }

    fn parse_tagged(&mut self) -> Result<FrontendProtocolMessage, ParseError> {
        use FrontendProtocolMessage::*;

        // should never happen because we check during has_tagged_frame()
        if self.remaining() < 5 {
            return Err(ParseError::Unparsable); // should never happen
        }

        let start = self.cursor;
        let buf = &self.buffer[start..];
        let tag = buf[0];
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        let total = 1 + len;

        // advance cursor now that slice is borrowed mutably
        self.cursor = start + total;

        let frame = &buf[..total];

        let message = match tag {
            // ---------- Query ------------------------------------------------
            b'Q' => Query(frames::QueryFrame::from_bytes(frame)?),

            // ---------- Extended-query protocol ------------------------------
            b'B' => Bind(frames::BindFrame::from_bytes(frame)?),
            b'C' => Close(frames::CloseFrame::from_bytes(frame)?),
            b'D' => Describe(frames::DescribeFrame::from_bytes(frame)?),
            b'E' => Execute(frames::ExecuteFrame::from_bytes(frame)?),
            b'F' => FunctionCall(frames::FunctionCallFrame::from_bytes(frame)?),
            b'H' => Flush(frames::FlushFrame::from_bytes(frame)?),
            b'P' => Parse(frames::ParseFrame::from_bytes(frame)?),
            b'S' => Sync(frames::SyncFrame::from_bytes(frame)?),
            b'X' => Terminate(frames::TerminateFrame::from_bytes(frame)?),

            // ---------- COPY sub-protocol ------------------------------------
            b'd' => CopyData(frames::CopyDataFrame::from_bytes(frame)?),
            b'c' => CopyDone(frames::CopyDoneFrame::from_bytes(frame)?),
            b'f' => CopyFail(frames::CopyFailFrame::from_bytes(frame)?),

            // ---------- Authentication (all share tag 'p') -------------------
            b'p' => {
                return self.parse_p_tagged(frame);
            }

            // ---------- Unknown tag ------------------------------------------
            other => return Err(ParseError::UnknownFrameTag(other)),
        };

        Ok(message)
    }

    fn parse_p_tagged<'a>(
        &'a self,
        frame: &'a [u8],
    ) -> Result<FrontendProtocolMessage<'a>, ParseError> {
        use FrontendProtocolMessage::*;
        use frames::{
            gss_response::GssResponseFrame, password_message::PasswordMessageFrame,
            sasl_initial_response::SaslInitialResponseFrame, sasl_response::SaslResponseFrame,
            sspi_response::SspiResponseFrame,
        };

        let auth_expectation = if let ClientState::Authenticating(ref auth_expectation) = self.state
        {
            auth_expectation
        } else {
            return Err(ParseError::UnexpectedAuthOutsideAuthState);
        };

        match auth_expectation {
            AuthExpectation::PasswordLike => {
                let pwd = PasswordMessageFrame::from_bytes(frame)?;
                Ok(PasswordMessage(pwd))
            }
            AuthExpectation::SASLInitial => {
                let sasl_init = SaslInitialResponseFrame::from_bytes(frame)?;
                Ok(SaslInitialResponse(sasl_init))
            }
            AuthExpectation::SASLResponse => {
                let sasl_resp = SaslResponseFrame::from_bytes(frame)?;
                Ok(SaslResponse(sasl_resp))
            }
            AuthExpectation::GSSResponse => {
                let gss = GssResponseFrame::from_bytes(frame)?;
                Ok(GssResponse(gss))
            }
            AuthExpectation::SSPIResponse => {
                let sspi = SspiResponseFrame::from_bytes(frame)?;
                Ok(SspiResponse(sspi))
            }
            AuthExpectation::None => Err(ParseError::NoAuthExpectation),
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    fn make_ssl_request_frame() -> BytesMut {
        let mut frame_buffer = BytesMut::with_capacity(8);
        frame_buffer.put_u32(8);
        frame_buffer.put_u32(SSL_REQUEST_CODE);
        frame_buffer
    }

    fn make_gssenc_request_frame() -> BytesMut {
        let mut frame_buffer = BytesMut::with_capacity(8);
        frame_buffer.put_u32(8);
        frame_buffer.put_u32(GSSENC_REQUEST_CODE);
        frame_buffer
    }

    fn make_cancel_request_frame(pid: u32, secret: u32) -> BytesMut {
        let mut frame_buffer = BytesMut::with_capacity(16);
        frame_buffer.put_u32(16);
        frame_buffer.put_u32(CANCEL_REQUEST_CODE);
        frame_buffer.put_u32(pid);
        frame_buffer.put_u32(secret);
        frame_buffer
    }

    fn make_startup_frame(params: &[(&str, &str)]) -> BytesMut {
        let mut frame_buffer = BytesMut::new();
        frame_buffer.put_u32(0); // placeholder for length
        frame_buffer.put_u32(MIN_STARTUP_VERSION);

        for (key, value) in params {
            frame_buffer.extend_from_slice(key.as_bytes());
            frame_buffer.put_u8(0);
            frame_buffer.extend_from_slice(value.as_bytes());
            frame_buffer.put_u8(0);
        }
        frame_buffer.put_u8(0); // parameter list terminator

        let frame_len = frame_buffer.len() as u32;
        frame_buffer[..4].copy_from_slice(&frame_len.to_be_bytes());
        frame_buffer
    }

    /// Generate a buffer containing `n` back-to-back Flush frames (`H` + length=4)
    fn make_many_flushes_buffer(n: usize) -> BytesMut {
        let mut buf = BytesMut::with_capacity(n * 5);
        for _ in 0..n {
            buf.put_u8(b'H');
            buf.put_u32(4);
        }
        buf
    }

    #[test]
    fn decode_one_million_flush_frames() {
        let mut decoder = FrontendDecoder::with_capacity(1024);

        decoder.buffer = make_many_flushes_buffer(1_000_000);

        let mut count = 0;
        while let Some(msg) = decoder.next() {
            matches!(msg, FrontendProtocolMessage::Flush(_))
                .then(|| count += 1)
                .unwrap();
        }

        assert_eq!(count, 1_000_000);
    }

    #[test]
    fn rolling_add_and_drain_batches() {
        const INIT_CAP: usize = 8 * 1024;
        const BATCH_SIZE: usize = 100;
        const BATCHES: usize = 1_000;

        let mut decoder = FrontendDecoder::with_capacity(INIT_CAP);
        assert_eq!(decoder.buffer.capacity(), INIT_CAP);

        let mut total_seen = 0;

        for _ in 0..BATCHES {
            // 1) append a batch
            let chunk = make_many_flushes_buffer(BATCH_SIZE);
            decoder.buffer.extend_from_slice(&chunk);

            // 2) drain and count
            let mut seen = 0;
            while let Some(msg) = decoder.next() {
                matches!(msg, FrontendProtocolMessage::Flush(_))
                    .then(|| {
                        seen += 1;
                        total_seen += 1;
                    })
                    .unwrap();
            }
            assert_eq!(seen, BATCH_SIZE);

            // 3) ensure we’re back to “empty” state
            assert_eq!(decoder.cursor, decoder.buffer.len());
        }

        // final sanity
        assert_eq!(decoder.cursor, decoder.buffer.len());
        assert_eq!(total_seen, BATCHES * BATCH_SIZE);
    }

    #[test]
    fn ssl_request_single() {
        let mut decoder = FrontendDecoder::with_capacity(16);
        decoder.buffer = make_ssl_request_frame();

        match decoder.next() {
            Some(FrontendProtocolMessage::SslRequest(_)) => {}
            other => panic!("expected SslRequest, got {:?}", other),
        }
    }

    #[test]
    fn gssenc_request_single() {
        let mut decoder = FrontendDecoder::with_capacity(16);
        decoder.buffer = make_gssenc_request_frame();

        match decoder.next() {
            Some(FrontendProtocolMessage::GssEncRequest(_)) => {}
            other => panic!("expected GssEncRequest, got {:?}", other),
        }
    }

    #[test]
    fn cancel_request_single() {
        let mut decoder = FrontendDecoder::with_capacity(32);
        decoder.buffer = make_cancel_request_frame(1234, 5678);

        match decoder.next() {
            Some(FrontendProtocolMessage::CancelRequest(_)) => {}
            other => panic!("expected CancelRequest, got {:?}", other),
        }
    }

    #[test]
    fn startup_frame_single() {
        let params = [("user", "pgdog"), ("database", "pgdog")];
        let mut decoder = FrontendDecoder::with_capacity(64);
        decoder.buffer = make_startup_frame(&params);

        match decoder.next() {
            Some(FrontendProtocolMessage::Startup(_)) => {}
            other => panic!("expected Startup, got {:?}", other),
        }
    }

    #[test]
    fn mixed_frames_back_to_back() {
        let mut decoder = FrontendDecoder::with_capacity(128);

        let mut buffer = BytesMut::new();
        buffer.extend_from_slice(&make_ssl_request_frame());
        buffer.put_u8(b'H'); // tagged Flush
        buffer.put_u32(4);
        buffer.extend_from_slice(&make_cancel_request_frame(1, 1));
        decoder.buffer = buffer;

        let mut seen_ssl = false;
        let mut seen_flush = false;
        let mut seen_cancel = false;

        while let Some(msg) = decoder.next() {
            match msg {
                FrontendProtocolMessage::SslRequest(_) => seen_ssl = true,
                FrontendProtocolMessage::Flush(_) => seen_flush = true,
                FrontendProtocolMessage::CancelRequest(_) => seen_cancel = true,
                _ => panic!("unexpected message {:?}", msg),
            }
        }

        assert!(seen_ssl && seen_flush && seen_cancel);
    }

    #[test]
    fn incomplete_then_complete_ssl_request() {
        let mut decoder = FrontendDecoder::with_capacity(16);
        let mut partial = make_ssl_request_frame();
        let tail = partial.split_off(4); // only first 4 bytes (length)

        decoder.buffer.extend_from_slice(&partial);
        assert!(decoder.next().is_none()); // incomplete

        decoder.buffer.extend_from_slice(&tail);
        match decoder.next() {
            Some(FrontendProtocolMessage::SslRequest(_)) => {}
            other => panic!("expected SslRequest after completion, got {:?}", other),
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
