//! wire_protocol::frontend::error.rs
//!
//! Common parse errors for all frontend frames.

use crate::wire_protocol::frontend::frames::{
    bind, cancel_request, close, copy_data, copy_done, copy_fail, describe, execute, flush,
    function_call, gss_response, gssenc_request, parse as parse_mod, password_message, query,
    sasl_initial_response, sasl_response, ssl_request, sspi_response, startup, sync, terminate,
};

/// A unified error type when parsing any frontend protocol message.
#[derive(Debug)]
pub enum ParseError {
    // Message parsing errors
    Bind(bind::BindError),
    CancelRequest(cancel_request::CancelRequestError),
    Close(close::CloseError),
    CopyData(copy_data::CopyDataError),
    CopyDone(copy_done::CopyDoneError),
    CopyFail(copy_fail::CopyFailError),
    Describe(describe::DescribeError),
    Execute(execute::ExecuteError),
    Flush(flush::FlushError),
    FunctionCall(function_call::FunctionCallError),
    GssEncRequest(gssenc_request::GssencRequestError),
    GssResponse(gss_response::GssResponseError),
    Parse(parse_mod::ParseError),
    PasswordMessage(password_message::PasswordMessageError),
    Query(query::QueryError),
    SaslInitialResponse(sasl_initial_response::SaslInitialResponseError),
    SaslResponse(sasl_response::SaslResponseError),
    SslRequest(ssl_request::SslRequestError),
    SspiResponse(sspi_response::SspiResponseError),
    Startup(startup::StartupError),
    Sync(sync::SyncError),
    Terminate(terminate::TerminateError),

    // Unknown frame tag
    UnknownFrameTag(u8),
    UnparsableSpecialFrame,
}

impl From<bind::BindError> for ParseError {
    fn from(e: bind::BindError) -> Self {
        ParseError::Bind(e)
    }
}

impl From<cancel_request::CancelRequestError> for ParseError {
    fn from(e: cancel_request::CancelRequestError) -> Self {
        ParseError::CancelRequest(e)
    }
}

impl From<close::CloseError> for ParseError {
    fn from(e: close::CloseError) -> Self {
        ParseError::Close(e)
    }
}

impl From<copy_data::CopyDataError> for ParseError {
    fn from(e: copy_data::CopyDataError) -> Self {
        ParseError::CopyData(e)
    }
}

impl From<copy_done::CopyDoneError> for ParseError {
    fn from(e: copy_done::CopyDoneError) -> Self {
        ParseError::CopyDone(e)
    }
}

impl From<copy_fail::CopyFailError> for ParseError {
    fn from(e: copy_fail::CopyFailError) -> Self {
        ParseError::CopyFail(e)
    }
}

impl From<describe::DescribeError> for ParseError {
    fn from(e: describe::DescribeError) -> Self {
        ParseError::Describe(e)
    }
}

impl From<execute::ExecuteError> for ParseError {
    fn from(e: execute::ExecuteError) -> Self {
        ParseError::Execute(e)
    }
}

impl From<flush::FlushError> for ParseError {
    fn from(e: flush::FlushError) -> Self {
        ParseError::Flush(e)
    }
}

impl From<function_call::FunctionCallError> for ParseError {
    fn from(e: function_call::FunctionCallError) -> Self {
        ParseError::FunctionCall(e)
    }
}

impl From<gss_response::GssResponseError> for ParseError {
    fn from(e: gss_response::GssResponseError) -> Self {
        ParseError::GssResponse(e)
    }
}

impl From<gssenc_request::GssencRequestError> for ParseError {
    fn from(e: gssenc_request::GssencRequestError) -> Self {
        ParseError::GssEncRequest(e)
    }
}

impl From<parse_mod::ParseError> for ParseError {
    fn from(e: parse_mod::ParseError) -> Self {
        ParseError::Parse(e)
    }
}

impl From<password_message::PasswordMessageError> for ParseError {
    fn from(e: password_message::PasswordMessageError) -> Self {
        ParseError::PasswordMessage(e)
    }
}

impl From<query::QueryError> for ParseError {
    fn from(e: query::QueryError) -> Self {
        ParseError::Query(e)
    }
}

impl From<sasl_initial_response::SaslInitialResponseError> for ParseError {
    fn from(e: sasl_initial_response::SaslInitialResponseError) -> Self {
        ParseError::SaslInitialResponse(e)
    }
}

impl From<sasl_response::SaslResponseError> for ParseError {
    fn from(e: sasl_response::SaslResponseError) -> Self {
        ParseError::SaslResponse(e)
    }
}

impl From<ssl_request::SslRequestError> for ParseError {
    fn from(e: ssl_request::SslRequestError) -> Self {
        ParseError::SslRequest(e)
    }
}

impl From<sspi_response::SspiResponseError> for ParseError {
    fn from(e: sspi_response::SspiResponseError) -> Self {
        ParseError::SspiResponse(e)
    }
}

impl From<startup::StartupError> for ParseError {
    fn from(e: startup::StartupError) -> Self {
        ParseError::Startup(e)
    }
}

impl From<sync::SyncError> for ParseError {
    fn from(e: sync::SyncError) -> Self {
        ParseError::Sync(e)
    }
}

impl From<terminate::TerminateError> for ParseError {
    fn from(e: terminate::TerminateError) -> Self {
        ParseError::Terminate(e)
    }
}
