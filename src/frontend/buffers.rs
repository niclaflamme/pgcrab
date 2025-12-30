use bytes::{Bytes, BytesMut};
use crate::frontend::transport::FrontendTransport;
use crate::frontend::sequence_tracker::SequenceTracker;
use crate::shared_types::AuthStage;
use crate::wire_protocol::utils::peek_frontend;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SCRATCH_CAPACITY_HINT: usize = 4096;

// -----------------------------------------------------------------------------
// ----- FrontendBuffers -------------------------------------------------------

#[derive(Debug)]
pub(crate) struct FrontendBuffers {
    inbox: BytesMut,
    inbox_tracker: SequenceTracker,
    outbox: BytesMut,
}

impl FrontendBuffers {
    pub(crate) fn new() -> Self {
        Self {
            inbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            inbox_tracker: SequenceTracker::new(),
            outbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
        }
    }

    pub(crate) async fn read_from(
        &mut self,
        transport: &mut FrontendTransport,
    ) -> std::io::Result<usize> {
        self.inbox.reserve(SCRATCH_CAPACITY_HINT);
        transport.read_buf(&mut self.inbox).await
    }

    pub(crate) fn track_new_inbox_frames(&mut self, stage: AuthStage) {
        loop {
            let cursor = self.inbox_tracker.len();

            let frame_slice = &self.inbox[cursor..];
            if frame_slice.is_empty() {
                break;
            }

            let Some(result) = peek_frontend(stage, frame_slice) else {
                break;
            };

            self.inbox_tracker.push(result.message_type, result.len);
        }
    }

    pub(crate) fn pull_next_sequence(&mut self, stage: AuthStage) -> Option<BytesMut> {
        let Some(bytes_to_take) = self.inbox_tracker.take_until_flush(stage) else {
            return None;
        };

        let sequence = self.inbox.split_to(bytes_to_take);

        Some(sequence)
    }

    pub(crate) fn queue_response(&mut self, response: &Bytes) {
        self.outbox.extend_from_slice(response);
    }

    pub(crate) async fn flush_to(
        &mut self,
        transport: &mut FrontendTransport,
    ) -> std::io::Result<()> {
        if !self.outbox.is_empty() {
            transport.write_all_buf(&mut self.outbox).await?;
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
