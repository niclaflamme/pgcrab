// -----------------------------------------------------------------------------
// ----- FrontendMessageType ---------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Bind,
    CancelRequest,
    Close,
    CopyData,
    CopyDone,
    CopyFail,
    Describe,
    Execute,
    Flush,
    FunctionCall,
    GssResponse,
    GssEncRequest,
    Parse,
    PasswordMessage,
    Query,
    SaslInitialResponse,
    SaslResponse,
    SslRequest,
    SspiResponse,
    Startup,
    Sync,
    Terminate,
    Unknown,
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
