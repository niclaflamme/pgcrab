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

const COMPACT_BUFFER_THRESHOLD: usize = 8 * 1024;

// -----------------------------------------------------------------------------
// ----- FrontendDecoder -------------------------------------------------------

pub struct FrontendDecoder {
    buffer: BytesMut,
    cursor: usize,
}

impl FrontendDecoder {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(cap),
            cursor: 0,
        }
    }
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
        let remaining_bytes = self.remaining();
        if remaining_bytes < 8 {
            return false;
        }

        let header_slice = &self.buffer[self.cursor..];

        // first four bytes are the length (includes itself)
        let frame_len = u32::from_be_bytes(header_slice[..4].try_into().unwrap()) as usize;

        // malformed length or not enough bytes yet
        if frame_len < 8 || remaining_bytes < frame_len {
            return false;
        }

        // next four bytes are the request / version code
        let request_code = u32::from_be_bytes(header_slice[4..8].try_into().unwrap());

        let is_ssl_request = request_code == SSL_REQUEST_CODE;
        let is_gssenc_request = request_code == GSSENC_REQUEST_CODE;
        let is_cancel_request = request_code == CANCEL_REQUEST_CODE;
        let is_startup_message = request_code >= MIN_STARTUP_VERSION;

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

        // Need the 8-byte header to read length + code.
        if self.remaining() < 8 {
            return Err(ParseError::UnparsableSpecialFrame);
        }

        let buf = &self.buffer[self.cursor..];
        let frame_len = u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize;

        // Wait until the full frame is buffered.
        if self.remaining() < frame_len {
            return Err(ParseError::UnparsableSpecialFrame);
        }

        let code = u32::from_be_bytes(buf[4..8].try_into().unwrap());
        let frame = &buf[..frame_len];
        self.cursor += frame_len; // advance once; every branch keeps it

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

        Err(ParseError::UnparsableSpecialFrame)
    }

    fn parse_tagged(&mut self) -> Result<FrontendProtocolMessage, ParseError> {
        let start = self.cursor;
        let buf = &self.buffer[start..];
        let tag = buf[0];
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        let total = 1 + len;

        // advance cursor now that slice is borrowed
        self.cursor = start + total;

        let frame = &buf[..total];

        use FrontendProtocolMessage::*;
        let message = match tag {
            // ---------- Extended-query protocol ----------
            b'B' => Bind(frames::BindFrame::from_bytes(frame)?),
            b'C' => Close(frames::CloseFrame::from_bytes(frame)?),
            b'D' => Describe(frames::DescribeFrame::from_bytes(frame)?),
            b'E' => Execute(frames::ExecuteFrame::from_bytes(frame)?),
            b'F' => FunctionCall(frames::FunctionCallFrame::from_bytes(frame)?),
            b'H' => Flush(frames::FlushFrame::from_bytes(frame)?),
            b'P' => Parse(frames::ParseFrame::from_bytes(frame)?),
            b'Q' => Query(frames::QueryFrame::from_bytes(frame)?),
            b'S' => Sync(frames::SyncFrame::from_bytes(frame)?),
            b'X' => Terminate(frames::TerminateFrame::from_bytes(frame)?),

            // ---------- COPY sub-protocol ----------
            b'd' => CopyData(frames::CopyDataFrame::from_bytes(frame)?),
            b'c' => CopyDone(frames::CopyDoneFrame::from_bytes(frame)?),
            b'f' => CopyFail(frames::CopyFailFrame::from_bytes(frame)?),

            // ---------- Authentication responses (all share tag 'p') ----------
            b'p' => {
                return self.parse_p_tagged(frame);
            }

            // ---------- Unknown tag ----------
            other => return Err(ParseError::UnknownFrameTag(other)),
        };

        Ok(message)
    }

    fn parse_p_tagged<'a>(
        &'a self,
        frame: &'a [u8],
    ) -> Result<FrontendProtocolMessage<'a>, ParseError> {
        use frames::{
            gss_response::GssResponseFrame, password_message::PasswordMessageFrame,
            sasl_initial_response::SaslInitialResponseFrame, sasl_response::SaslResponseFrame,
            sspi_response::SspiResponseFrame,
        };

        if let Ok(sspi) = SspiResponseFrame::from_bytes(frame) {
            return Ok(FrontendProtocolMessage::SspiResponse(sspi));
        }

        if let Ok(sasl_init) = SaslInitialResponseFrame::from_bytes(frame) {
            return Ok(FrontendProtocolMessage::SaslInitialResponse(sasl_init));
        }

        if let Ok(sasl_resp) = SaslResponseFrame::from_bytes(frame) {
            return Ok(FrontendProtocolMessage::SaslResponse(sasl_resp));
        }

        if let Ok(gss) = GssResponseFrame::from_bytes(frame) {
            return Ok(FrontendProtocolMessage::GssResponse(gss));
        }

        let pwd = PasswordMessageFrame::from_bytes(frame)?;
        Ok(FrontendProtocolMessage::PasswordMessage(pwd))
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    /// Generate a buffer containing `n` back-to-back Flush frames (`H` + length=4)
    fn make_flush_buffer(n: usize) -> BytesMut {
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

        decoder.buffer = make_flush_buffer(1_000_000);

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
            let chunk = make_flush_buffer(BATCH_SIZE);
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
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
