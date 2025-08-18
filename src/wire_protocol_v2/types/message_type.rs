// -----------------------------------------------------------------------------
// ----- MessageKind -----------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // Frontend
    Startup,
    SSLRequest,
    GSSENCRequest,
    CancelRequest,
    Bind,
    Close,
    CopyFail,
    Describe,
    Execute,
    Flush,
    FunctionCall,
    GSSResponse,
    Parse,
    PasswordMessage,
    Query,
    SASLInitialResponse,
    SASLResponse,
    SSPIResponse,
    Sync,
    Terminate,
    // -- Backend

    // -- Bidirectional
    CopyData,
    CopyDone,
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
