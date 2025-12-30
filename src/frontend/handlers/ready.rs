use bytes::BytesMut;
use tracing::debug;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::gateway::GatewayPools;
use crate::gateway::GatewaySession;
use crate::parser;
use crate::shared_types::AuthStage;
use crate::shared_types::ReadyStatus;
use crate::wire::observers::parse::ParseFrameObserver;
use crate::wire::observers::query::QueryFrameObserver;
use crate::wire::types::MessageType;
use crate::wire::utils::peek_frontend;

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

    inspect_for_parsing(&sequence);

    if let Err(err) = session.backend().send(&sequence).await {
        let error = ErrorResponse::internal_error(format!("backend write failed: {err}"));
        buffers.queue_response(&error.to_bytes());
        buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        context.gateway_session = None;
    }
}

fn inspect_for_parsing(sequence: &[u8]) {
    let mut cursor = 0;
    while cursor < sequence.len() {
        let Some(peek) = peek_frontend(AuthStage::Ready, &sequence[cursor..]) else {
            break;
        };

        if peek.len == 0 {
            break;
        }

        let end = cursor.saturating_add(peek.len);
        if end > sequence.len() {
            break;
        }

        let frame = &sequence[cursor..end];
        match peek.message_type {
            MessageType::Query => inspect_query_frame(frame),
            MessageType::Parse => inspect_parse_frame(frame),
            _ => {}
        }

        cursor = end;
    }
}

fn inspect_query_frame(frame: &[u8]) {
    match QueryFrameObserver::new(frame) {
        Ok(observer) => parse_and_log(observer.query(), "Query"),
        Err(err) => debug!(error = %err, "failed to decode Query frame"),
    }
}

fn inspect_parse_frame(frame: &[u8]) {
    match ParseFrameObserver::new(frame) {
        Ok(observer) => parse_and_log(observer.query(), "Parse"),
        Err(err) => debug!(error = %err, "failed to decode Parse frame"),
    }
}

fn parse_and_log(query: &str, message_type: &'static str) {
    match parser::parse(query) {
        Ok(parsed) => debug!(message_type, ?parsed.ast, "parsed SQL"),
        Err(err) => debug!(message_type, error = %err, "failed to parse SQL"),
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
