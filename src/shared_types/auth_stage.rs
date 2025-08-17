/// Represents the stages of a frontend connection's authentication process in
/// the PostgreSQL wire protocol.
///
/// This enum tracks the state of a client connection as it progresses through
/// the initial handshake and authentication phases. It is used to manage the
/// expected message types and protocol behavior in each stage, as handled in
/// the connection logic
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthStage {
    /// The initial stage where the client sends a `Startup` or `SslRequest` message.
    Startup,

    /// The stage where authentication is in progress, expecting messages like `PasswordMessage`
    /// or `SaslInitialResponse`.
    Authenticating,

    /// The stage where authentication is complete, and the connection is ready for queries.
    Ready,
}
