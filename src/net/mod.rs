// net/mod.rs
//! Networking layer: thin wrappers around sockets and reusable buffers.
//! Everything in here is transport-agnostic.

pub mod connection_buffer;

pub use connection_buffer::ConnectionBuffer;
