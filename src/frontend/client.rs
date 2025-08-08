use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    sync::mpsc,
};

use crate::net::ConnBuff;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const READ_CAPACITY: usize = 4 * 1024;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

pub struct FrontendConnection {
    state: ConnState,

    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
    inbound: BytesMut,
    outbound: ConnBuff,

    #[allow(dead_code)]
    rx: mpsc::UnboundedReceiver<Frame>,
    #[allow(dead_code)]
    tx: mpsc::UnboundedSender<Frame>,
}

impl FrontendConnection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();
        let (tx, rx) = mpsc::unbounded_channel();

        Self {
            state: ConnState::Startup,
            reader,
            writer,
            inbound: BytesMut::with_capacity(READ_CAPACITY),
            outbound: ConnBuff::new(),
            rx,
            tx,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: SubStructs ----------------------------------------

type Frame = Vec<u8>;

type Response = Vec<u8>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnState {
    Startup,        // not yet started handshake (unauthenticated)
    AuthInProgress, // in the auth conversation
    Ready,          // authenticated and ready for SQL
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Public Methods ------------------------------------

impl FrontendConnection {
    pub async fn run(mut self) -> std::io::Result<()> {
        let mut scratch = [0u8; READ_CAPACITY];

        loop {
            select! {

                // --- Read socket ---
                n = self.reader.read(&mut scratch), if !self.outbound.needs_flush() => {
                    let n = n?;
                    if n == 0 { break; } // client closed

                    self.inbound.extend_from_slice(&scratch[..n]);
                    // Drain all parseable messages from buffer (pipelining support)
                    while let Some(frame) = match self.state {
                        ConnState::Startup        => Self::try_parse_startup(&mut self.inbound),
                        ConnState::AuthInProgress => Self::try_parse_auth(&mut self.inbound),
                        ConnState::Ready          => Self::try_parse_sql(&mut self.inbound),
                    } {
                        self.process_frame(frame);
                    }
                }

                // --- Write socket ---
                _ = self.writer.writable(), if self.outbound.needs_flush() => {
                    if let Some(mut slab) = self.outbound.take_full() {
                        self.writer.write_all_buf(&mut slab).await?;
                        self.outbound.rebalance();
                    }
                }

            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Private Methods -----------------------------------

impl FrontendConnection {
    fn process_frame(&mut self, frame: Frame) {
        match self.state {
            ConnState::Startup => {
                // TODO: real startup protocol parsing here
                // Advance to AuthInProgress if handshake OK
                self.outbound.push(b"Authentication request".as_ref());
                self.state = ConnState::AuthInProgress;
            }
            ConnState::AuthInProgress => {
                // TODO: real auth protocol here
                // Advance to Ready on success
                self.outbound.push(b"AuthenticationOk".as_ref());
                self.state = ConnState::Ready;
            }
            ConnState::Ready => {
                // TODO: parse and handle SQL protocol
                let resp = Self::handle_sql(frame);
                self.outbound.push(&resp);
            }
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Dummy Methods -------------------------------------

impl FrontendConnection {
    fn handle_sql(_frame: Frame) -> Response {
        b"Z\0\0\0\x05I".to_vec() // ReadyForQuery
    }

    fn try_parse_startup(buf: &mut BytesMut) -> Option<Frame> {
        if buf.len() >= 8 {
            return Some(buf.split_to(8).to_vec());
        }

        None
    }

    fn try_parse_auth(buf: &mut BytesMut) -> Option<Frame> {
        if buf.len() >= 6 {
            return Some(buf.split_to(6).to_vec());
        }

        None
    }

    fn try_parse_sql(buf: &mut BytesMut) -> Option<Frame> {
        if buf.len() >= 12 {
            Some(buf.split_to(12).to_vec())
        } else {
            None
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
