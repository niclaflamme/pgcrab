// Proxy responses are backend protocol messages emitted by the proxy itself
// (auth/parameter/ready), rather than forwarded from an upstream backend.

use bytes::{BufMut, Bytes, BytesMut};

use crate::shared_types::{BackendIdentity, ReadyStatus};

// -----------------------------------------------------------------------------
// ----- Local Backend Responses -----------------------------------------------

pub(crate) fn ssl_no() -> Bytes {
    Bytes::from_static(b"N")
}

pub(crate) fn auth_cleartext() -> Bytes {
    let mut b = BytesMut::with_capacity(1 + 4 + 4);
    b.put_u8(b'R');
    b.put_u32(8);
    b.put_i32(3);
    b.freeze()
}

pub(crate) fn auth_ok() -> Bytes {
    let mut b = BytesMut::with_capacity(1 + 4 + 4);
    b.put_u8(b'R');
    b.put_u32(8);
    b.put_i32(0);
    b.freeze()
}

pub(crate) fn param_status(name: &str, value: &str) -> Bytes {
    let n = name.as_bytes();
    let v = value.as_bytes();
    let payload_len = 4 + n.len() + 1 + v.len() + 1;
    let mut b = BytesMut::with_capacity(1 + payload_len);
    b.put_u8(b'S');
    b.put_u32(payload_len as u32);
    b.extend_from_slice(n);
    b.put_u8(0);
    b.extend_from_slice(v);
    b.put_u8(0);
    b.freeze()
}

pub(crate) fn ready_with_status(status: ReadyStatus) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + 4 + 1);
    b.put_u8(b'Z');
    b.put_u32(5);
    b.put_u8(status.as_byte());
    b.freeze()
}

pub(crate) fn backend_key_data(identity: BackendIdentity) -> Bytes {
    let mut b = BytesMut::with_capacity(1 + 4 + 8);
    b.put_u8(b'K');
    b.put_u32(12);
    b.put_i32(identity.process_id);
    b.put_i32(identity.secret_key);
    b.freeze()
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
