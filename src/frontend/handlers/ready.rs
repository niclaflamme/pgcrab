use bytes::BytesMut;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::gateway::GatewayPools;
use crate::gateway::GatewaySession;
use crate::shared_types::ReadyStatus;

// -----------------------------------------------------------------------------
// ----- Ready Handler ---------------------------------------------------------

pub(crate) async fn handle_ready(
    context: &mut FrontendContext,
    buffers: &mut FrontendBuffers,
    sequence: BytesMut,
    pools: &GatewayPools,
) {
    if context.gateway_session.is_none() {
        let Some(pool) = pools.random_pool() else {
            let err = ErrorResponse::internal_error("no backend shards available");
            buffers.queue_response(&err.to_bytes());
            buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
            return;
        };

        match GatewaySession::from_pool(&pool).await {
            Ok(session) => {
                context.gateway_session = Some(session);
            }
            Err(err) => {
                let error = ErrorResponse::internal_error(err);
                buffers.queue_response(&error.to_bytes());
                buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
                return;
            }
        }
    }

    let Some(session) = context.gateway_session.as_mut() else {
        return;
    };

    if let Err(err) = session.backend().send(&sequence).await {
        let error = ErrorResponse::internal_error(format!("backend write failed: {err}"));
        buffers.queue_response(&error.to_bytes());
        buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        context.gateway_session = None;
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
