use crate::wire_protocol::frontend::frames;

/// Represents any frontend-initiated protocol message.
/// Bidirectional protocol messages are also included.
#[derive(Debug)]
pub enum FrontendProtocolMessage<'a> {
    /// Extended-protocol Bind message
    Bind(frames::BindFrame<'a>),

    /// CancelRequest message for canceling queries
    CancelRequest(frames::CancelRequestFrame),

    /// Close message
    Close(frames::CloseFrame<'a>),

    /// CopyData message for COPY operations
    CopyData(frames::CopyDataFrame<'a>),

    /// CopyDone message for COPY operations
    CopyDone(frames::CopyDoneFrame),

    /// CopyFail message for COPY operations
    CopyFail(frames::CopyFailFrame<'a>),

    /// Describe message for describing prepared statements and portals
    Describe(frames::DescribeFrame<'a>),

    /// Extended-protocol Execute message for executing prepared statements
    Execute(frames::ExecuteFrame<'a>),

    /// Flush message for flushing data
    Flush(frames::FlushFrame),

    /// FunctionCall message for calling functions
    FunctionCall(frames::FunctionCallFrame<'a>),

    /// GssResponse message for GSSAPI authentication
    GssResponse(frames::GssResponseFrame<'a>),

    /// GssEncRequest message for GSSAPI encryption
    GssEncRequest(frames::GssencRequestFrame),

    /// Parse message for parsing SQL statements
    Parse(frames::ParseFrame<'a>),

    /// Password message for password authentication
    PasswordMessage(frames::PasswordMessageFrame<'a>),

    /// Query message for executing SQL queries
    Query(frames::QueryFrame<'a>),

    /// SaslInitialResponse message for SASL authentication
    SaslInitialResponse(frames::SaslInitialResponseFrame<'a>),

    /// SaslResponse message for SASL authentication
    SaslResponse(frames::SaslResponseFrame<'a>),

    /// SslRequest message for SSL negotiation
    SslRequest(frames::SslRequestFrame),

    /// SspiResponse message for SSPI authentication
    SspiResponse(frames::SspiResponseFrame<'a>),

    /// Startup message for initializing the connection
    Startup(frames::StartupFrame<'a>),

    /// Sync message for synchronizing the connection
    Sync(frames::SyncFrame),

    /// Terminate message signaling session end
    Terminate(frames::TerminateFrame),

    /// Should never happen, store tag as u8
    Unknown(u8),
}
