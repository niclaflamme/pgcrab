use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;

// -----------------------------------------------------------------------------
// ----- FrontendTransport -----------------------------------------------------

#[derive(Debug)]
pub(crate) enum FrontendTransport {
    Plain(Option<TcpStream>),
    Tls(TlsStream<TcpStream>),
}

impl FrontendTransport {
    pub(crate) fn new(stream: TcpStream) -> Self {
        FrontendTransport::Plain(Some(stream))
    }

    pub(crate) async fn read_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<usize> {
        match self {
            FrontendTransport::Plain(Some(stream)) => stream.read_buf(buf).await,
            FrontendTransport::Plain(None) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "missing plaintext stream",
            )),
            FrontendTransport::Tls(stream) => stream.read_buf(buf).await,
        }
    }

    pub(crate) async fn write_all_buf(&mut self, buf: &mut BytesMut) -> std::io::Result<()> {
        match self {
            FrontendTransport::Plain(Some(stream)) => stream.write_all_buf(buf).await,
            FrontendTransport::Plain(None) => Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "missing plaintext stream",
            )),
            FrontendTransport::Tls(stream) => stream.write_all_buf(buf).await,
        }
    }

    pub(crate) async fn upgrade_to_tls(
        &mut self,
        acceptor: &TlsAcceptor,
    ) -> std::io::Result<()> {
        let FrontendTransport::Plain(stream) = self else {
            return Ok(());
        };

        let stream = stream.take().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::Other, "missing plaintext stream")
        })?;

        let tls_stream = acceptor.accept(stream).await?;
        *self = FrontendTransport::Tls(tls_stream);

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
