use crate::{shared_types::AuthStage, wire_protocol::types::MessageType};

use super::super::observers::{
    bind::BindFrameObserver, cancel_request::CancelRequestFrameObserver, close::CloseFrameObserver,
    copy_data::CopyDataFrameObserver, copy_done::CopyDoneFrameObserver,
    copy_fail::CopyFailFrameObserver, describe::DescribeFrameObserver,
    execute::ExecuteFrameObserver, flush::FlushFrameObserver,
    function_call::FunctionCallFrameObserver, gss_response::GSSResponseFrameObserver,
    gssenc_request::GSSENCRequestFrameObserver, parse::ParseFrameObserver,
    password_message::PasswordMessageFrameObserver, query::QueryFrameObserver,
    sasl_initial_response::SASLInitialResponseFrameObserver,
    sasl_response::SASLResponseFrameObserver, ssl_request::SSLRequestFrameObserver,
    sspi_response::SSPIResponseFrameObserver, startup::StartupFrameObserver,
    sync::SyncFrameObserver, terminate::TerminateFrameObserver,
};

// -----------------------------------------------------------------------------
// ----- Structs ---------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PeekResult {
    pub message_type: MessageType,
    pub len: usize,
}

// -----------------------------------------------------------------------------
// ----- peek_frontend ---------------------------------------------------------

pub fn peek_frontend(stage: AuthStage, bytes: &[u8]) -> Option<PeekResult> {
    match stage {
        AuthStage::Startup => peek_frontend_startup(bytes),
        AuthStage::Authenticating => peek_frontend_authenticating(bytes),
        AuthStage::Ready => peek_frontend_ready(bytes),
    }
}

// -----------------------------------------------------------------------------
// ----- peek_frontend: Startup ------------------------------------------------

#[inline]
fn peek_frontend_startup(bytes: &[u8]) -> Option<PeekResult> {
    let bytes = bytes.as_ref();

    if let Some(len) = SSLRequestFrameObserver::peek(bytes) {
        return Some(PeekResult {
            message_type: MessageType::SSLRequest,
            len,
        });
    }

    if let Some(len) = GSSENCRequestFrameObserver::peek(bytes) {
        return Some(PeekResult {
            message_type: MessageType::GSSENCRequest,
            len,
        });
    }

    if let Some(len) = CancelRequestFrameObserver::peek(bytes) {
        return Some(PeekResult {
            message_type: MessageType::CancelRequest,
            len,
        });
    }

    if let Some(len) = StartupFrameObserver::peek(bytes) {
        return Some(PeekResult {
            message_type: MessageType::Startup,
            len,
        });
    }

    None
}

// -----------------------------------------------------------------------------
// ----- peek_frontend: Authenticating -----------------------------------------

#[inline]
fn peek_frontend_authenticating(bytes: &[u8]) -> Option<PeekResult> {
    let buf = bytes.as_ref();

    let len = get_len(buf)?;
    let frame_slice = &buf[..len];

    if SASLInitialResponseFrameObserver::new(frame_slice).is_ok() {
        return Some(PeekResult {
            message_type: MessageType::SASLInitialResponse,
            len,
        });
    }

    if PasswordMessageFrameObserver::new(frame_slice).is_ok() {
        return Some(PeekResult {
            message_type: MessageType::PasswordMessage,
            len,
        });
    }

    // Add GSS/SSPI before generic SASLResponse to avoid misclassification.
    if GSSResponseFrameObserver::new(frame_slice).is_ok() {
        return Some(PeekResult {
            message_type: MessageType::GSSResponse,
            len,
        });
    }

    if SSPIResponseFrameObserver::new(frame_slice).is_ok() {
        return Some(PeekResult {
            message_type: MessageType::SSPIResponse,
            len,
        });
    }

    if SASLResponseFrameObserver::new(frame_slice).is_ok() {
        return Some(PeekResult {
            message_type: MessageType::SASLResponse,
            len,
        });
    }

    None
}

#[inline]
fn get_len(buf: &[u8]) -> Option<usize> {
    if buf.len() < 5 || buf[0] != b'p' {
        return None;
    }

    // length field itself
    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if len < 4 {
        return None;
    }

    // tag + length-based body
    let total = 1 + len;
    if total > buf.len() {
        return None;
    }

    Some(total)
}

// -----------------------------------------------------------------------------
// ----- peek_frontend: Ready --------------------------------------------------

fn peek_frontend_ready(bytes: &[u8]) -> Option<PeekResult> {
    if bytes.len() < 5 {
        return None;
    }

    let tag = bytes[0];

    match tag {
        b'B' => BindFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Bind,
            len,
        }),

        b'C' => CloseFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Close,
            len,
        }),

        b'd' => CopyDataFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::CopyData,
            len,
        }),

        b'c' => CopyDoneFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::CopyDone,
            len,
        }),

        b'f' => CopyFailFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::CopyFail,
            len,
        }),

        b'D' => DescribeFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Describe,
            len,
        }),

        b'E' => ExecuteFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Execute,
            len,
        }),

        b'H' => FlushFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Flush,
            len,
        }),

        b'F' => FunctionCallFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::FunctionCall,
            len,
        }),

        b'P' => ParseFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Parse,
            len,
        }),

        b'Q' => QueryFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Query,
            len,
        }),

        b'S' => SyncFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Sync,
            len,
        }),

        b'X' => TerminateFrameObserver::peek(&bytes).map(|len| PeekResult {
            message_type: MessageType::Terminate,
            len,
        }),

        _ => None,
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
