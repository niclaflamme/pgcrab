use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

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
}
