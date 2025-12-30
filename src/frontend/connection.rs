use bytes::BytesMut;
use tokio::net::TcpStream;
use tokio::select;

use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::handlers;
use crate::shared_types::AuthStage;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

/// Drives the client connection through the Startup -> Authenticating -> Ready
/// stages, delegating protocol handling to stage-specific handlers.
#[derive(Debug)]
pub struct FrontendConnection {
    context: FrontendContext,
    buffers: FrontendBuffers,
    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Static --------------------------------------------

impl FrontendConnection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();

        Self {
            context: FrontendContext::new(),
            buffers: FrontendBuffers::new(),
            reader,
            writer,
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
                    self.buffers.read_from(&mut self.reader).await
                } => {
                    let n = read_res?;
                    if n == 0 { break; }

                    // read -> track -> process -> flush
                    self.buffers.track_new_inbox_frames(self.context.stage);

                    while let Some(sequence) = self.buffers.pull_next_sequence(self.context.stage) {
                        self.process_sequence(sequence).await;
                        if self.context.should_close() {
                            break;
                        }
                    }

                    self.buffers.flush_to(&mut self.writer).await?;
                    if self.context.should_close() {
                        break;
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
    async fn process_sequence(&mut self, seq_or_msg: BytesMut) {
        match self.context.stage {
            AuthStage::Startup => {
                handlers::startup::handle_startup(&mut self.context, &mut self.buffers, seq_or_msg)
            }
            AuthStage::Authenticating => {
                handlers::authenticating::handle_authenticating(
                    &mut self.context,
                    &mut self.buffers,
                    seq_or_msg,
                )
                .await
            }
            AuthStage::Ready => {
                handlers::ready::handle_ready(&mut self.context, &mut self.buffers, seq_or_msg)
            }
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
