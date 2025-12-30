use bytes::BytesMut;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::select;

use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::handlers;
use crate::frontend::transport::FrontendTransport;
use crate::gateway::GatewayPools;
use crate::shared_types::AuthStage;
use crate::tls;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

/// Drives the client connection through the Startup -> Authenticating -> Ready
/// stages, delegating protocol handling to stage-specific handlers.
pub struct FrontendConnection {
    context: FrontendContext,
    buffers: FrontendBuffers,
    transport: FrontendTransport,
    tls_acceptor: Option<tokio_rustls::TlsAcceptor>,
    _pools: Arc<GatewayPools>,
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Static --------------------------------------------

impl FrontendConnection {
    pub fn new(stream: TcpStream, pools: Arc<GatewayPools>) -> Self {
        Self {
            context: FrontendContext::new(),
            buffers: FrontendBuffers::new(),
            transport: FrontendTransport::new(stream),
            tls_acceptor: tls::acceptor(),
            _pools: pools,
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
                    self.buffers.read_from(&mut self.transport).await
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

                        if self.context.wants_tls_upgrade() {
                            break;
                        }
                    }

                    self.buffers.flush_to(&mut self.transport).await?;

                    if self.context.should_close() {
                        break;
                    }

                    if self.context.take_tls_upgrade() {
                        if let Some(acceptor) = self.tls_acceptor.as_ref() {
                            self.transport.upgrade_to_tls(acceptor).await?;
                        }
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
            AuthStage::Startup => handlers::startup::handle_startup(
                &mut self.context,
                &mut self.buffers,
                seq_or_msg,
                self.tls_acceptor.is_some(),
            ),
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
