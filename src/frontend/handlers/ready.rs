use bytes::{BufMut, BytesMut};
use std::collections::HashSet;
use tracing::debug;

use crate::ErrorResponse;
use crate::admin;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::{FrontendContext, PendingParse, PreparedStatement};
use crate::frontend::proxy_responses as responses;
use crate::gateway::GatewayPools;
use crate::gateway::GatewaySession;
use crate::parser;
use crate::shared_types::AuthStage;
use crate::shared_types::ReadyStatus;
use crate::wire::observers::bind::BindFrameObserver;
use crate::wire::observers::close::{CloseFrameObserver, CloseTarget};
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
    if context.is_admin && try_handle_admin_sequence(buffers, &sequence) {
        return;
    }

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

    let Some(mut session) = context.gateway_session.take() else {
        return;
    };

    let sequence = prepare_sequence(context, &mut session, sequence);

    if let Err(err) = session.backend().send(&sequence).await {
        let error = ErrorResponse::internal_error(format!("backend write failed: {err}"));
        buffers.queue_response(&error.to_bytes());
        buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        context.gateway_session = None;
        context.pending_parses.clear();
        return;
    }

    context.gateway_session = Some(session);
}

fn try_handle_admin_sequence(buffers: &mut FrontendBuffers, sequence: &[u8]) -> bool {
    let Some(peek) = peek_frontend(AuthStage::Ready, sequence) else {
        return false;
    };

    if peek.len != sequence.len() || peek.message_type != MessageType::Query {
        return false;
    }

    let observer = match QueryFrameObserver::new(sequence) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Query frame");
            return false;
        }
    };

    let Some(command) = admin::parse_admin_command(observer.query()) else {
        return false;
    };

    for response in admin::command_responses(command) {
        buffers.queue_response(&response);
    }
    buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
    true
}

fn prepare_sequence(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    sequence: BytesMut,
) -> BytesMut {
    let mut output = BytesMut::with_capacity(sequence.len());
    let mut parsed_in_sequence: HashSet<String> = HashSet::new();

    let mut cursor = 0;
    while cursor < sequence.len() {
        let Some(peek) = peek_frontend(AuthStage::Ready, &sequence[cursor..]) else {
            output.extend_from_slice(&sequence[cursor..]);
            return output;
        };

        if peek.len == 0 {
            output.extend_from_slice(&sequence[cursor..]);
            return output;
        }

        let end = cursor.saturating_add(peek.len);
        if end > sequence.len() {
            output.extend_from_slice(&sequence[cursor..]);
            return output;
        }

        let frame = &sequence[cursor..end];
        match peek.message_type {
            MessageType::Query => inspect_query_frame(frame),
            MessageType::Parse => {
                if let Some(statement) = handle_parse_frame(context, frame) {
                    parsed_in_sequence.insert(statement);
                }
            }
            MessageType::Bind => {
                handle_bind_frame(context, session, frame, &mut output, &mut parsed_in_sequence);
            }
            MessageType::Close => {
                handle_close_frame(context, session, frame);
            }
            _ => {}
        }

        output.extend_from_slice(frame);
        cursor = end;
    }

    output
}

fn inspect_query_frame(frame: &[u8]) {
    match QueryFrameObserver::new(frame) {
        Ok(observer) => parse_and_log(observer.query(), "Query"),
        Err(err) => debug!(error = %err, "failed to decode Query frame"),
    }
}

fn handle_parse_frame(context: &mut FrontendContext, frame: &[u8]) -> Option<String> {
    match ParseFrameObserver::new(frame) {
        Ok(observer) => {
            parse_and_log(observer.query(), "Parse");
            let statement = observer.statement();
            let pending_name = if statement.is_empty() {
                None
            } else {
                Some(statement.to_string())
            };
            context.pending_parses.push_back(PendingParse {
                name: pending_name.clone(),
                suppress_response: false,
            });

            if let Some(name) = pending_name {
                let mut param_type_oids = Vec::with_capacity(observer.param_type_count());
                for idx in 0..observer.param_type_count() {
                    param_type_oids.push(observer.param_type_oid(idx));
                }
                context.prepared_statements.insert(
                    name.clone(),
                    PreparedStatement {
                        query: observer.query().to_string(),
                        param_type_oids,
                    },
                );
                Some(name)
            } else {
                None
            }
        }
        Err(err) => {
            debug!(error = %err, "failed to decode Parse frame");
            context.pending_parses.push_back(PendingParse {
                name: None,
                suppress_response: false,
            });
            None
        }
    }
}

fn handle_bind_frame(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    frame: &[u8],
    output: &mut BytesMut,
    parsed_in_sequence: &mut HashSet<String>,
) {
    let observer = match BindFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Bind frame");
            return;
        }
    };

    let statement = observer.statement();
    if statement.is_empty() {
        return;
    }

    if parsed_in_sequence.contains(statement) {
        return;
    }

    if session.backend().prepared_contains(statement) {
        return;
    }

    let Some(cached) = context.prepared_statements.get(statement) else {
        debug!(statement, "Bind references unknown prepared statement");
        return;
    };

    let injected = build_parse_frame(statement, &cached.query, &cached.param_type_oids);
    output.extend_from_slice(&injected);
    context.pending_parses.push_back(PendingParse {
        name: Some(statement.to_string()),
        suppress_response: true,
    });
    parsed_in_sequence.insert(statement.to_string());
}

fn handle_close_frame(context: &mut FrontendContext, session: &mut GatewaySession, frame: &[u8]) {
    let observer = match CloseFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Close frame");
            return;
        }
    };

    if observer.target() != CloseTarget::Statement {
        return;
    }

    let name = observer.name();
    if name.is_empty() {
        return;
    }

    context.prepared_statements.remove(name);
    session.backend().prepared_remove(name);
}

fn build_parse_frame(statement: &str, query: &str, param_type_oids: &[i32]) -> BytesMut {
    let body_len = statement.len() + 1 + query.len() + 1 + 2 + 4 * param_type_oids.len();
    let mut body = BytesMut::with_capacity(body_len);
    body.extend_from_slice(statement.as_bytes());
    body.put_u8(0);
    body.extend_from_slice(query.as_bytes());
    body.put_u8(0);
    body.put_i16(param_type_oids.len() as i16);
    for &oid in param_type_oids {
        body.put_i32(oid);
    }

    let mut frame = BytesMut::with_capacity(1 + 4 + body.len());
    frame.put_u8(b'P');
    frame.put_u32((4 + body.len()) as u32);
    frame.extend_from_slice(&body);
    frame
}

fn parse_and_log(query: &str, message_type: &'static str) {
    match parser::parse(query) {
        Ok(parsed) => debug!(message_type, ?parsed.ast, "parsed SQL"),
        Err(err) => debug!(message_type, error = %err, "failed to parse SQL"),
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
