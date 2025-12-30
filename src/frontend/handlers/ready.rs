use bytes::BytesMut;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::shared_types::ReadyStatus;

// -----------------------------------------------------------------------------
// ----- Ready Handler ---------------------------------------------------------

pub(crate) fn handle_ready(
    _context: &mut FrontendContext,
    buffers: &mut FrontendBuffers,
    _sequence: BytesMut,
) {
    // Dummy failure so psql doesn't hang. Then return to idle.
    let err = ErrorResponse::internal_error("statement execution not implemented");
    buffers.queue_response(&err.to_bytes());
    buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
