//! v3 protocol Byte1 tags, plus special pre-startup responses.
//! Direction disambiguates duplicates (e.g., 'H' is Flush from FE, CopyOut from BE).

// -----------------------------------------------------------------------------
// ----- MessageType -----------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    // -- Pre-startup single-byte responses
    SSLAllowed,       // 'S' to SSLRequest
    SSLNotAllowed,    // 'N' to SSLRequest
    GSSENCAllowed,    // 'G' to GSSENCRequest
    GSSENCNotAllowed, // 'N' to GSSENCRequest

    // -- Frontend (startup)
    Startup,       // no tag (length + protocol + params)
    SSLRequest,    // no tag (special startup packet)
    GSSENCRequest, // no tag (special startup packet)
    CancelRequest, // no tag (special startup packet)

    // -- Frontend (ready)
    Bind,                // 'B'
    Close,               // 'C'
    CopyFail,            // 'f'
    Describe,            // 'D'
    Execute,             // 'E'
    Flush,               // 'H'
    FunctionCall,        // 'F'
    GSSResponse,         // 'p'
    Parse,               // 'P'
    PasswordMessage,     // 'p'
    Query,               // 'Q'
    SASLInitialResponse, // 'p'
    SASLResponse,        // 'p'
    SSPIResponse,        // 'p'
    Sync,                // 'S'
    Terminate,           // 'X'

    // -- Backend
    Authentication,           // 'R' (see AuthenticationType)
    BackendKeyData,           // 'K'
    BindComplete,             // '2'
    CloseComplete,            // '3'
    CommandComplete,          // 'C'
    CopyInResponse,           // 'G'
    CopyOutResponse,          // 'H'
    CopyBothResponse,         // 'W'
    DataRow,                  // 'D'
    EmptyQueryResponse,       // 'I'
    ErrorResponse,            // 'E'
    FunctionCallResponse,     // 'V'
    NegotiateProtocolVersion, // 'v'
    NoData,                   // 'n'
    NoticeResponse,           // 'N'
    NotificationResponse,     // 'A'
    ParameterDescription,     // 't'
    ParameterStatus,          // 'S'
    ParseComplete,            // '1'
    PortalSuspended,          // 's'
    ReadyForQuery,            // 'Z'
    RowDescription,           // 'T'

    // -- Bidirectional
    CopyData, // 'd'
    CopyDone, // 'c'
}

// -----------------------------------------------------------------------------
// ----- AuthenticationType ----------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationType {
    Ok,                // 0
    KerberosV5,        // 2 (legacy)
    CleartextPassword, // 3
    MD5Password,       // 5
    SCMCredential,     // 6
    GSS,               // 7
    GSSContinue,       // 8
    SSPI,              // 9
    SASL,              // 10
    SASLContinue,      // 11
    SASLFinal,         // 12
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
