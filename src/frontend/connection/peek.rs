use crate::shared_types::AuthStage;
use crate::wire_protocol::frontend::message_type::MessageType;

// -----------------------------------------------------------------------------
// ----- Peek ------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FoundMessage {
    pub message_type: MessageType,
    pub len: usize,
}

// -----------------------------------------------------------------------------
// ----- Exported: peek() ------------------------------------------------------

/// Peeks at the next message in the buffer, considering the current authentication stage.
///
/// Returns a `Peek` struct containing the message type and length if a valid message is detected,
/// or `None` if the buffer is incomplete or the message is invalid for the current stage.
/// The stage determines which message types are expected.
pub fn peek(stage: AuthStage, bytes: &[u8]) -> Option<FoundMessage> {
    match stage {
        AuthStage::Startup => peek_startup(bytes),
        AuthStage::Authenticating { .. } => peek_auth_in_progress(bytes),
        AuthStage::Ready => peek_ready(bytes),
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: peek() by AuthStage -----------------------------------------

fn peek_startup(bytes: &[u8]) -> Option<FoundMessage> {
    if bytes.len() < 8 {
        return None;
    }

    let len = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    if bytes.len() < len {
        return None;
    }

    let code = u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);

    if len == 8 && code == 80877103 {
        return Some(FoundMessage {
            message_type: MessageType::SslRequest,
            len,
        });
    }

    if len == 8 && code == 80877104 {
        return Some(FoundMessage {
            message_type: MessageType::GssEncRequest,
            len,
        });
    }

    if len == 16 && code == 80877102 {
        return Some(FoundMessage {
            message_type: MessageType::CancelRequest,
            len,
        });
    }

    if code >> 16 == 3 {
        return Some(FoundMessage {
            message_type: MessageType::Startup,
            len,
        });
    }

    None
}

fn peek_auth_in_progress(bytes: &[u8]) -> Option<FoundMessage> {
    // Minimum size: tag (1 byte) + length (4 bytes)
    if bytes.len() < 5 {
        return None;
    }

    // Only 'p' tagged messages are allowed in AuthInProgress
    if bytes[0] != b'p' {
        return None;
    }

    // Extract payload length (excludes tag, includes all content after length field)
    let payload_len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;

    // Total message length: tag (1) + length field (4) + payload (payload_len - 4)
    // Note: payload_len includes the length field itself in PostgreSQL wire protocol
    let total_len = payload_len + 1;

    // Ensure the full message is available in the buffer
    if bytes.len() < total_len {
        return None;
    }

    // Content starts after header (tag + length)
    let header_len = 5;
    let content_start = header_len;
    let content_end = total_len;
    let content = &bytes[content_start..content_end];

    // Find the position of the first null byte in content (if any)
    let nul_pos_in_content_opt = content.iter().position(|&b| b == 0);

    // If a null is found, attempt to match structured messages
    if let Some(nul_pos_in_content) = nul_pos_in_content_opt {
        // Absolute position of null from start of bytes
        let nul_pos_from_start = content_start + nul_pos_in_content;

        // First, check for SaslInitialResponse: null-terminated mechanism name, followed by i32 data length, then data
        let data_len_pos = nul_pos_from_start + 1;
        if data_len_pos + 4 <= total_len {
            let data_len_bytes: [u8; 4] = [
                bytes[data_len_pos],
                bytes[data_len_pos + 1],
                bytes[data_len_pos + 2],
                bytes[data_len_pos + 3],
            ];

            let data_len_i32 = i32::from_be_bytes(data_len_bytes);

            // Valid data length: -1 indicates empty data, >=0 indicates data follows
            if data_len_i32 >= -1 {
                let data_len_usize = if data_len_i32 == -1 {
                    0
                } else {
                    data_len_i32 as usize
                };
                let expected_end = data_len_pos + 4 + data_len_usize;

                // If structure matches exactly to end of message, it's SaslInitialResponse
                if expected_end == total_len {
                    return Some(FoundMessage {
                        message_type: MessageType::SaslInitialResponse,
                        len: total_len,
                    });
                }
            }
        }

        // Next, check for PasswordMessage: null terminator exactly at the end
        if nul_pos_from_start + 1 == total_len {
            return Some(FoundMessage {
                message_type: MessageType::PasswordMessage,
                len: total_len,
            });
        }
    }

    // Fallback: raw bytes without matching structure above (treat as SaslResponse by default)
    Some(FoundMessage {
        message_type: MessageType::SaslResponse,
        len: total_len,
    })
}

fn peek_ready(bytes: &[u8]) -> Option<FoundMessage> {
    if bytes.len() < 5 {
        return None;
    }

    let tag = bytes[0];

    let payload_len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    let total_len = payload_len + 1;

    // Incoming message
    if bytes.len() < total_len {
        return None;
    }

    match tag {
        b'B' => Some(FoundMessage {
            message_type: MessageType::Bind,
            len: total_len,
        }),
        b'C' => Some(FoundMessage {
            message_type: MessageType::Close,
            len: total_len,
        }),
        b'd' => Some(FoundMessage {
            message_type: MessageType::CopyData,
            len: total_len,
        }),
        b'c' => Some(FoundMessage {
            message_type: MessageType::CopyDone,
            len: total_len,
        }),
        b'f' => Some(FoundMessage {
            message_type: MessageType::CopyFail,
            len: total_len,
        }),
        b'D' => Some(FoundMessage {
            message_type: MessageType::Describe,
            len: total_len,
        }),
        b'E' => Some(FoundMessage {
            message_type: MessageType::Execute,
            len: total_len,
        }),
        b'H' => Some(FoundMessage {
            message_type: MessageType::Flush,
            len: total_len,
        }),
        b'F' => Some(FoundMessage {
            message_type: MessageType::FunctionCall,
            len: total_len,
        }),
        b'P' => Some(FoundMessage {
            message_type: MessageType::Parse,
            len: total_len,
        }),
        b'Q' => Some(FoundMessage {
            message_type: MessageType::Query,
            len: total_len,
        }),
        b'S' => Some(FoundMessage {
            message_type: MessageType::Sync,
            len: total_len,
        }),
        b'X' => Some(FoundMessage {
            message_type: MessageType::Terminate,
            len: total_len,
        }),
        _ => None,
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{BufMut, BytesMut};

    #[test]
    fn peek_startup_ssl_request() {
        let mut bytes = BytesMut::new();
        bytes.put_u32(8);
        bytes.put_u32(80877103);

        let peek = peek(AuthStage::Startup, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::SslRequest);
        assert_eq!(peek.len, 8);
    }

    #[test]
    fn peek_startup_gss_enc_request() {
        let mut bytes = BytesMut::new();

        bytes.put_u32(8);
        bytes.put_u32(80877104);

        let peek = peek(AuthStage::Startup, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::GssEncRequest);
        assert_eq!(peek.len, 8);
    }

    #[test]
    fn peek_startup_cancel_request() {
        let mut bytes = BytesMut::new();
        bytes.put_u32(16);
        bytes.put_u32(80877102);
        bytes.put_u32(1234); // dummy PID
        bytes.put_u32(5678); // dummy key

        let peek = peek(AuthStage::Startup, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::CancelRequest);
        assert_eq!(peek.len, 16);
    }

    #[test]
    fn peek_startup_startup() {
        let mut bytes = BytesMut::new();
        bytes.put_u32(23);
        bytes.put_u32(196608);
        bytes.extend_from_slice(b"user\0postgres\0\0");

        let peek = peek(AuthStage::Startup, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::Startup);
        assert_eq!(peek.len, 23);
    }

    #[test]
    fn peek_auth_in_progress_password() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'p');
        bytes.put_u32(12);
        bytes.extend_from_slice(b"hunter2\0");

        let peek = peek(AuthStage::Authenticating, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::PasswordMessage);
        assert_eq!(peek.len, 13);
    }

    #[test]
    fn peek_auth_in_progress_sasl_initial_response() {
        let mut bytes = BytesMut::new();

        bytes.put_u8(b'p');
        bytes.put_u32(34);
        bytes.extend_from_slice(b"SCRAM-SHA-256\0");
        bytes.put_i32(12);
        bytes.extend_from_slice(b"n,,n=user,r=");

        let peek = peek(AuthStage::Authenticating, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::SaslInitialResponse);
        assert_eq!(peek.len, 35);
    }

    #[test]
    fn peek_auth_in_progress_sasl_response() {
        let mut bytes = BytesMut::new();

        bytes.put_u8(b'p');
        bytes.put_u32(15);
        bytes.extend_from_slice(b"bi=rO0ABXNyA==");

        let peek = peek(AuthStage::Authenticating, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::SaslResponse);
        assert_eq!(peek.len, 16);
    }

    #[test]
    fn peek_ready_query() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'Q');
        bytes.put_u32(13);
        bytes.extend_from_slice(b"SELECT 1\0");

        let peek = peek(AuthStage::Ready, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::Query);
        assert_eq!(peek.len, 14);
    }

    #[test]
    fn peek_ready_terminate() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'X');
        bytes.put_u32(4);

        let peek = peek(AuthStage::Ready, &bytes).unwrap();

        assert_eq!(peek.message_type, MessageType::Terminate);
        assert_eq!(peek.len, 5);
    }

    #[test]
    fn peek_invalid_stage() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(b'Q');
        bytes.put_u32(13);
        bytes.extend_from_slice(b"SELECT 1\0");

        // Query not allowed in Startup
        assert_eq!(peek(AuthStage::Startup, &bytes), None);
    }

    #[test]
    fn peek_insufficient_data() {
        let bytes = BytesMut::from(&b"p\x00\x00"[..]);
        assert_eq!(peek(AuthStage::Authenticating, &bytes), None);
    }

    #[test]
    fn peek_incomplete_message() {
        let mut bytes = BytesMut::new();

        bytes.put_u8(b'p');
        bytes.put_u32(99);
        bytes.extend_from_slice(b"openses..."); // Insufficient data (missing null and more)

        assert_eq!(peek(AuthStage::Authenticating, &bytes), None);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
