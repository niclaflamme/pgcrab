use bytes::{Bytes, BytesMut};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
    sync::mpsc,
};

use crate::shared_types::AuthStage;
use crate::wire_protocol::frontend::MessageType;

use super::{peek::peek, sequence_tracker::SequenceTracker};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SCRATCH_CAPACITY_HINT: usize = 4096;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

#[derive(Debug)]
pub struct FrontendConnection {
    stage: AuthStage,

    inbox: BytesMut,
    inbox_tracker: SequenceTracker,

    #[allow(dead_code)]
    outbox: BytesMut,

    reader: tokio::net::tcp::OwnedReadHalf,
    async_writer: mpsc::UnboundedSender<Bytes>,
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Static --------------------------------------------

impl FrontendConnection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();

        let (writer_tx, writer_rx) = mpsc::unbounded_channel::<Bytes>();
        spawn_writer_task(writer, writer_rx);

        Self {
            stage: AuthStage::Startup,
            inbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            outbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            inbox_tracker: SequenceTracker::new(),
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

                // -- Client messages --
                read_res = async {
                    self.inbox.reserve(SCRATCH_CAPACITY_HINT);
                    self.reader.read_buf(&mut self.inbox).await
                } => {
                    let n = read_res?;
                    if n == 0 { break; }

                    // 1) check all untracked frames into inbox_tracker
                    self.track_new_inbox_frames();

                    // 2) process all complete sequences in the inbox
                    while let Some(sequence) = self.pull_next_sequence() {
                        self.process_sequence(sequence);
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
    fn track_new_inbox_frames(&mut self) {
        loop {
            let cursor = self.inbox_tracker.len();

            let frame_slice = &self.inbox[cursor..];
            if frame_slice.is_empty() {
                break;
            }

            let Some(message) = peek(self.stage, frame_slice) else {
                break;
            };

            self.inbox_tracker.push(message.message_type, message.len);
        }
    }

    fn pull_next_sequence(&mut self) -> Option<BytesMut> {
        let (messages, bytes_taken) = self.inbox_tracker.take_until_flush(self.stage)?;
        if messages.is_empty() {
            return None;
        }

        let sequence = self.inbox.split_to(bytes_taken);
        Some(sequence)
    }

    fn process_sequence(&mut self, sequence: BytesMut) {
        match self.stage {
            AuthStage::Startup => self.process_sequence_startup(sequence),
            AuthStage::Authenticating => self.process_sequence_authenticating(sequence),
            AuthStage::Ready => self.process_sequence_ready(sequence),
        }
    }

    fn process_sequence_startup(&mut self, sequence: BytesMut) {
        let found = peek(AuthStage::Startup, &sequence[..]).unwrap();

        match found.message_type {
            MessageType::SslRequest => {
                let response = Bytes::from_static(b"TODO");
                self.async_writer.send(response).unwrap();
            }

            MessageType::GssEncRequest => {
                let response = Bytes::from_static(b"TODO");
                self.async_writer.send(response).unwrap();
            }

            MessageType::CancelRequest => {
                let response = Bytes::from_static(b"TODO");
                self.async_writer.send(response).unwrap();
            }

            MessageType::Startup => {
                let response = Bytes::from_static(b"TODO");
                self.async_writer.send(response).unwrap();
            }

            _ => {
                println!("Unexpected message in startup: {:?}", found.message_type);
            }
        }
    }

    fn process_sequence_authenticating(&mut self, sequence: BytesMut) {
        let found = peek(AuthStage::Authenticating, &sequence[..]).unwrap();

        match found.message_type {
            MessageType::PasswordMessage => {
                let response = Bytes::from_static(b"TODO");
                self.async_writer.send(response).unwrap();
                self.stage = AuthStage::Ready;
            }

            _ => {
                println!(
                    "Unexpected message in authenticating: {:?}",
                    found.message_type
                );
            }
        }
    }

    fn process_sequence_ready(&mut self, sequence: BytesMut) {
        println!("R.SEQ: {:?}", sequence);
    }
}

// -----------------------------------------------------------------------------
// ----- Internal: Helpers -----------------------------------------------------

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
