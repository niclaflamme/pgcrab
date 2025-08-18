/// Identifies which side of the PostgreSQL wire protocol produced a message.
///
/// This enum is used anywhere the proxy needs to tag, route, or log frames by
/// origin (client vs server). It keeps parsers, state machines, and logs
/// unambiguous.
///
/// - `Frontend`: messages coming from the client.
/// - `Backend`: messages coming from the PostgreSQL server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolMessageOrigin {
    /// Message sent by the PostgreSQL server (the "backend" in protocol docs).
    Backend,

    /// Message sent by the client (the "frontend" in protocol docs).
    Frontend,
}
