use bytes::{BufMut, Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    sync::mpsc,
};

use crate::wire_protocol::frontend::frames::StartupFrame;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SCRATCH_CAPACITY_HINT: usize = 4096;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

pub struct FrontendConnection {
    stage: Stage,

    inbox_scratch: BytesMut,  // socket -> parser
    outbox_scratch: BytesMut, // encoder -> channel

    reader: tokio::net::tcp::OwnedReadHalf,
    async_writer: mpsc::UnboundedSender<Bytes>,
}

// -----------------------------------------------------------------------------
// ----- Internal: Stage -------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stage {
    Startup,        // not yet started handshake (unauthenticated)
    AuthInProgress, // in the auth conversation
    Ready,          // authenticated and ready for SQL
}

// -----------------------------------------------------------------------------
// ----- Internal: Remove me ---------------------------------------------------

enum OutMsg {
    Static(&'static [u8]),
    Owned(Bytes),
}

impl OutMsg {
    #[inline]
    fn into_bytes(self) -> Bytes {
        match self {
            OutMsg::Static(s) => Bytes::from_static(s), // zero-copy ref to static
            OutMsg::Owned(b) => b,                      // move, no clone
        }
    }
}

const SSL_NO: &[u8] = b"N";
const AUTH_CLEAR: &[u8] = b"R\0\0\0\x08\0\0\0\x03"; // cleartext request

// const READY_I: &[u8] = b"Z\0\0\0\x05I";

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Static --------------------------------------------

impl FrontendConnection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();

        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Bytes>();
        spawn_writer_task(writer, writer_rx);

        Self {
            stage: Stage::Startup,
            inbox_scratch: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            outbox_scratch: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            reader,
            async_writer: writer_tx,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Public --------------------------------------------

impl FrontendConnection {
    pub async fn serve(mut self) -> std::io::Result<()> {
        loop {
            select! {
                read_res = async {
                    self.inbox_scratch.reserve(SCRATCH_CAPACITY_HINT);
                    self.reader.read_buf(&mut self.inbox_scratch).await
                } => {
                    let n = read_res?;
                    if n == 0 { break; }

                    loop {
                        let maybe = match self.stage {
                            Stage::Startup        => Self::try_parse_startup(&mut self.inbox_scratch),
                            Stage::AuthInProgress => Self::try_parse_auth(&mut self.inbox_scratch),
                            Stage::Ready          => Self::try_parse_sql(&mut self.inbox_scratch),
                        };
                        let Some(frame) = maybe else { break };
                        self.process_frame(frame);
                    }
                }
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Private -------------------------------------------

impl FrontendConnection {
    // static frame
    fn send_static(&self, s: &'static [u8]) {
        let _ = self.async_writer.send(Bytes::from_static(s));
    }

    // dynamic frame
    fn finish_and_send(&mut self, start: usize) {
        let end = self.outbox_scratch.len();
        let frame_len = end - start;
        let len_field = (frame_len as u32) - 1;
        self.outbox_scratch[start + 1..start + 5].copy_from_slice(&len_field.to_be_bytes());
        let bytes = self.outbox_scratch.split_to(frame_len).freeze(); // -> Bytes
        let _ = self.async_writer.send(bytes);
    }

    fn process_frame(&mut self, frame: BytesMut) {
        match self.stage {
            Stage::Startup => {
                if frame.len() == 8 {
                    let code = u32::from_be_bytes([frame[4], frame[5], frame[6], frame[7]]);
                    if code == 80877103 || code == 80877104 {
                        let _ = self.async_writer.send(OutMsg::Static(SSL_NO).into_bytes());
                        return;
                    }
                }

                let _ = self
                    .async_writer
                    .send(OutMsg::Static(AUTH_CLEAR).into_bytes());

                self.stage = Stage::AuthInProgress;
            }

            Stage::AuthInProgress => {
                self.send_auth_ok(); // dynamic example
                self.send_ready_for_query(); // dynamic example
                self.stage = Stage::Ready;
            }

            Stage::Ready => {
                self.send_ready_for_query();
            }
        }
    }

    fn try_parse_startup(buf: &mut BytesMut) -> Option<BytesMut> {
        let Some(len) = StartupFrame::peek(buf) else {
            return None;
        };

        Some(buf.split_to(len))
    }

    fn try_parse_auth(buf: &mut BytesMut) -> Option<BytesMut> {
        if buf.len() < 5 {
            println!("buffy: {:?}", &buf);
            println!("too short");
            return None;
        }

        if buf[0] != b'p' {
            println!("not p");
            return None;
        }

        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        let total = 1 + len; // len excludes the type byte

        if buf.len() < total {
            println!("not long enough");
            return None;
        }

        println!("WOOHOOO");
        println!("WOOHOOO");
        println!("WOOHOOO");
        Some(buf.split_to(total))
    }

    fn try_parse_sql(_buf: &mut BytesMut) -> Option<BytesMut> {
        println!("parsing sql...");
        None
    }

    fn send_auth_ok(&mut self) {
        let start = self.begin_typed_frame(b'R');
        self.outbox_scratch.put_u32(0); // AuthenticationOk
        self.finish_and_send(start);
    }

    fn send_ready_for_query(&mut self) {
        let start = self.begin_typed_frame(b'Z');
        self.outbox_scratch.put_u8(b'I'); // idle
        self.finish_and_send(start);
    }

    #[inline]
    fn begin_typed_frame(&mut self, tag: u8) -> usize {
        let start = self.outbox_scratch.len();
        self.outbox_scratch.put_u8(tag);
        self.outbox_scratch.put_u32(0); // placeholder
        start
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: Async Writer ------------------------------------------------

fn spawn_writer_task(
    mut writer: tokio::net::tcp::OwnedWriteHalf,
    mut rx: mpsc::UnboundedReceiver<Bytes>,
) {
    tokio::spawn(async move {
        while let Some(mut buf) = rx.recv().await {
            if writer.write_all_buf(&mut buf).await.is_err() {
                break;
            }

            // coalesce bursts to cut syscalls
            while let Ok(mut more) = rx.try_recv() {
                if writer.write_all_buf(&mut more).await.is_err() {
                    break;
                }
            }
        }
    });
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
