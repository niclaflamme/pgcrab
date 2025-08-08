// net/mod.rs
//! Networking layer: thin wrappers around sockets and reusable buffers.
//! Everything in here is transport-agnostic.

pub mod conn_buff;

pub use conn_buff::ConnBuff; // expose the struct at `crate::net::DualBuf`
