use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::select;

use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::handlers;
use crate::frontend::transport::FrontendTransport;
use crate::gateway::GatewayPools;
use crate::frontend::proxy_responses as responses;
use crate::shared_types::ReadyStatus;
use crate::shared_types::AuthStage;
use crate::wire::utils::peek_backend;
use crate::ErrorResponse;
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
    pools: Arc<GatewayPools>,
    backend_tracker: BackendFrameTracker,
}

#[derive(Debug, Default)]
struct BackendFrameTracker {
    pending: Option<(u8, usize)>,
}

impl BackendFrameTracker {
    fn next_frame(&mut self, buf: &[u8]) -> Option<(u8, usize)> {
        if let Some(pending) = self.pending {
            let total = 1 + pending.1;
            if buf.len() >= total {
                self.pending = None;
                return Some(pending);
            }
            return None;
        }

        if let Some(frame) = peek_backend(buf) {
            return Some(frame);
        }

        if buf.len() < 5 {
            return None;
        }

        let tag = buf[0];
        let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
        if len < 4 {
            return None;
        }

        let total = 1 + len;
        if buf.len() < total {
            self.pending = Some((tag, len));
        }

        None
    }

    fn reset(&mut self) {
        self.pending = None;
    }
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
            pools,
            backend_tracker: BackendFrameTracker::default(),
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Public --------------------------------------------

impl FrontendConnection {
    pub async fn serve(mut self) -> std::io::Result<()> {
        loop {
            if self.context.gateway_session.is_some() {
                select! {
                    read_res = async {
                        self.buffers.read_from(&mut self.transport).await
                    } => {
                        if !self.handle_frontend_read(read_res).await? {
                            break;
                        }
                    }
                    backend_res = Self::read_backend(&mut self.context) => {
                        if !self.handle_backend_read(backend_res).await? {
                            break;
                        }
                    }
                }
            } else {
                let read_res = self.buffers.read_from(&mut self.transport).await;
                if !self.handle_frontend_read(read_res).await? {
                    break;
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
                handlers::ready::handle_ready(
                    &mut self.context,
                    &mut self.buffers,
                    seq_or_msg,
                    self.pools.as_ref(),
                )
                .await
            }
        }
    }

    async fn read_backend(context: &mut FrontendContext) -> std::io::Result<usize> {
        let session = context
            .gateway_session
            .as_mut()
            .expect("backend read without gateway session");
        session.backend().read().await
    }

    async fn handle_frontend_read(
        &mut self,
        read_res: std::io::Result<usize>,
    ) -> std::io::Result<bool> {
        let n = read_res?;
        if n == 0 {
            return Ok(false);
        }

        // read -> track -> process -> flush
        self.buffers.track_new_inbox_frames(self.context.stage);

        while let Some(sequence) = self.buffers.pull_next_sequence(self.context.stage) {
            let had_session = self.context.gateway_session.is_some();
            self.process_sequence(sequence).await;

            if self.context.should_close() {
                break;
            }

            if self.context.wants_tls_upgrade() {
                break;
            }

            if had_session && self.context.gateway_session.is_none() {
                self.backend_tracker.reset();
            }
        }

        self.buffers.flush_to(&mut self.transport).await?;

        if self.context.should_close() {
            return Ok(false);
        }

        if self.context.take_tls_upgrade() {
            if let Some(acceptor) = self.tls_acceptor.as_ref() {
                self.transport.upgrade_to_tls(acceptor).await?;
            }
        }

        Ok(true)
    }

    async fn handle_backend_read(
        &mut self,
        read_res: std::io::Result<usize>,
    ) -> std::io::Result<bool> {
        let n = match read_res {
            Ok(n) => n,
            Err(err) => {
                self.backend_error(format!("backend read failed: {err}"));
                self.buffers.flush_to(&mut self.transport).await?;
                return Ok(true);
            }
        };

        if n == 0 {
            self.backend_error("backend closed connection".to_string());
            self.buffers.flush_to(&mut self.transport).await?;
            return Ok(true);
        }

        let Some(session) = self.context.gateway_session.as_mut() else {
            return Ok(true);
        };

        let backend = session.backend();
        let mut saw_ready = false;
        loop {
            let (tag, total_len, frame) = {
                let buffer = backend.buffer();
                let Some((tag, len)) = self.backend_tracker.next_frame(buffer) else {
                    break;
                };
                let total_len = 1 + len;
                let frame = Bytes::copy_from_slice(&buffer[..total_len]);
                (tag, total_len, frame)
            };

            backend.consume(total_len);
            self.buffers.queue_response(&frame);

            if tag == b'Z' {
                saw_ready = true;
            }
        }

        if saw_ready {
            self.context.gateway_session = None;
            self.backend_tracker.reset();
        }

        self.buffers.flush_to(&mut self.transport).await?;

        Ok(true)
    }

    fn backend_error(&mut self, message: String) {
        let error = ErrorResponse::internal_error(message);
        self.buffers.queue_response(&error.to_bytes());
        self.buffers
            .queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        self.context.gateway_session = None;
        self.backend_tracker.reset();
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
