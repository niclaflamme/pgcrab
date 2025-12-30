use bytes::{Buf, BufMut, BytesMut};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use crate::wire_protocol::utils::peek_backend;

#[derive(Debug)]
pub struct BackendConnection {
    stream: TcpStream,
    buffer: BytesMut,
}

impl BackendConnection {
    pub async fn connect(host: &str, port: u16) -> std::io::Result<Self> {
        let addr = format!("{}:{}", host, port);
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;

        Ok(Self {
            stream,
            buffer: BytesMut::with_capacity(8192),
        })
    }

    pub async fn send(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.stream.write_all(data).await
    }

    pub async fn read(&mut self) -> std::io::Result<usize> {
        self.stream.read_buf(&mut self.buffer).await
    }

    pub fn buffer(&self) -> &[u8] {
        self.buffer.as_ref()
    }

    pub fn consume(&mut self, n: usize) {
        self.buffer.advance(n);
    }

    pub fn peer_addr(&self) -> std::io::Result<SocketAddr> {
        self.stream.peer_addr()
    }

    pub async fn startup(
        &mut self,
        user: &str,
        database: &str,
        password: &str,
    ) -> Result<(), String> {
        let startup = build_startup_message(user, database);
        self.send(&startup)
            .await
            .map_err(|e| format!("backend startup send failed: {e}"))?;

        let mut requested_password = false;
        loop {
            let n = self
                .read()
                .await
                .map_err(|e| format!("backend startup read failed: {e}"))?;
            if n == 0 {
                return Err("backend closed during startup".to_string());
            }

            loop {
                let (tag, len) = match peek_backend(self.buffer()) {
                    Some(frame) => frame,
                    None => break,
                };

                let total_len = 1 + len;
                let frame = &self.buffer()[..total_len];

                match tag {
                    b'R' => {
                        if frame.len() < 9 {
                            return Err("backend auth response too short".to_string());
                        }
                        let code =
                            i32::from_be_bytes([frame[5], frame[6], frame[7], frame[8]]);
                        match code {
                            0 => {}
                            3 => {
                                if requested_password {
                                    return Err("backend requested password twice".to_string());
                                }
                                if password.is_empty() {
                                    return Err(
                                        "backend requested password but none configured"
                                            .to_string(),
                                    );
                                }
                                let password_message = build_password_message(password);
                                self.send(&password_message)
                                    .await
                                    .map_err(|e| format!("backend password send failed: {e}"))?;
                                requested_password = true;
                            }
                            _ => {
                                return Err(format!(
                                    "unsupported backend auth method: {code}"
                                ));
                            }
                        }
                    }
                    b'E' => {
                        return Err("backend startup error response".to_string());
                    }
                    b'Z' => {
                        self.consume(total_len);
                        return Ok(());
                    }
                    _ => {}
                }

                self.consume(total_len);
            }
        }
    }
}

fn build_startup_message(user: &str, database: &str) -> BytesMut {
    let mut buf = BytesMut::with_capacity(128);
    buf.put_u32(0);
    buf.put_u32(196608);
    buf.extend_from_slice(b"user");
    buf.put_u8(0);
    buf.extend_from_slice(user.as_bytes());
    buf.put_u8(0);
    buf.extend_from_slice(b"database");
    buf.put_u8(0);
    buf.extend_from_slice(database.as_bytes());
    buf.put_u8(0);
    buf.put_u8(0);
    let len = buf.len() as u32;
    buf[0..4].copy_from_slice(&len.to_be_bytes());
    buf
}

fn build_password_message(password: &str) -> BytesMut {
    let payload_len = 4 + password.len() + 1;
    let mut buf = BytesMut::with_capacity(1 + payload_len);
    buf.put_u8(b'p');
    buf.put_u32(payload_len as u32);
    buf.extend_from_slice(password.as_bytes());
    buf.put_u8(0);
    buf
}
