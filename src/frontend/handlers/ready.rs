use bytes::{BufMut, BytesMut};
use memchr::memchr;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::debug;

use crate::ErrorResponse;
use crate::admin;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::{FrontendContext, PendingParse, PortalBinding, VirtualStatement};
use crate::frontend::proxy_responses as responses;
use crate::gateway::GatewayPools;
use crate::gateway::GatewaySession;
use crate::parser;
use crate::shared_types::StatementSignature;
use crate::shared_types::AuthStage;
use crate::shared_types::ReadyStatus;
use crate::wire::observers::bind::BindFrameObserver;
use crate::wire::observers::close::{CloseFrameObserver, CloseTarget};
use crate::wire::observers::describe::{DescribeFrameObserver, DescribeTarget};
use crate::wire::observers::execute::ExecuteFrameObserver;
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

    let sequence = prepare_sequence(context, &mut session, buffers, sequence);

    if let Err(err) = session.backend().send(&sequence).await {
        let error = ErrorResponse::internal_error(format!("backend write failed: {err}"));
        buffers.queue_response(&error.to_bytes());
        buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        context.gateway_session = None;
        context.pending_parses.clear();
        context.pending_syncs = 0;
        context.virtual_portals.clear();
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
    buffers: &mut FrontendBuffers,
    sequence: BytesMut,
) -> BytesMut {
    let mut output = BytesMut::with_capacity(sequence.len());
    let mut in_flight_prepares = std::mem::take(&mut context.in_flight_prepares);
    in_flight_prepares.clear();

    let mut cursor = 0;
    while cursor < sequence.len() {
        let Some(peek) = peek_frontend(AuthStage::Ready, &sequence[cursor..]) else {
            output.extend_from_slice(&sequence[cursor..]);
            break;
        };

        if peek.len == 0 {
            output.extend_from_slice(&sequence[cursor..]);
            break;
        }

        let end = cursor.saturating_add(peek.len);
        if end > sequence.len() {
            output.extend_from_slice(&sequence[cursor..]);
            break;
        }

        let frame = &sequence[cursor..end];
        match peek.message_type {
            MessageType::Query => {
                handle_query_frame(context, session, frame);
                context.pending_syncs = context.pending_syncs.saturating_add(1);
                output.extend_from_slice(frame);
            }
            MessageType::Parse => {
                handle_parse_frame(
                    context,
                    session,
                    buffers,
                    frame,
                    &mut output,
                    &mut in_flight_prepares,
                );
            }
            MessageType::Bind => {
                handle_bind_frame(
                    context,
                    session,
                    frame,
                    &mut output,
                    &mut in_flight_prepares,
                );
            }
            MessageType::Describe => {
                handle_describe_frame(
                    context,
                    session,
                    frame,
                    &mut output,
                    &mut in_flight_prepares,
                );
            }
            MessageType::Execute => {
                handle_execute_frame(context, frame, &mut output);
            }
            MessageType::Close => {
                handle_close_frame(context, session, frame, &mut output);
            }
            MessageType::Sync => {
                context.pending_syncs = context.pending_syncs.saturating_add(1);
                context.virtual_portals.clear();
                output.extend_from_slice(frame);
            }
            _ => {
                output.extend_from_slice(frame);
            }
        }
        cursor = end;
    }

    context.in_flight_prepares = in_flight_prepares;
    output
}

fn handle_query_frame(context: &mut FrontendContext, session: &mut GatewaySession, frame: &[u8]) {
    match QueryFrameObserver::new(frame) {
        Ok(observer) => {
            parse_and_log(observer.query(), "Query");
            if is_reset_query(observer.query()) {
                session.backend().prepared_reset();
                context.virtual_statements.clear();
                context.virtual_portals.clear();
            }
        }
        Err(err) => debug!(error = %err, "failed to decode Query frame"),
    }
}

fn handle_parse_frame(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    buffers: &mut FrontendBuffers,
    frame: &[u8],
    output: &mut BytesMut,
    in_flight_prepares: &mut HashMap<StatementSignature, String>,
) {
    let observer = match ParseFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Parse frame");
            context.pending_parses.push_back(PendingParse {
                signature: None,
                backend_statement_name: None,
                suppress_response: false,
            });
            output.extend_from_slice(frame);
            return;
        }
    };

    parse_and_log(observer.query(), "Parse");

    let statement = observer.statement();
    let mut param_type_oids = Vec::with_capacity(observer.param_type_count());
    for idx in 0..observer.param_type_count() {
        param_type_oids.push(observer.param_type_oid(idx));
    }
    let signature = StatementSignature::new(observer.query(), &param_type_oids);

    if let Some(existing) = context.virtual_statements.get(statement) {
        if existing.signature == signature && !existing.closed {
            buffers.queue_response(&responses::parse_complete());
            return;
        }
    }

    let generation = match context.virtual_statements.get(statement) {
        Some(existing) if existing.signature == signature && !existing.closed => existing.generation,
        Some(existing) => existing.generation.saturating_add(1),
        None => 1,
    };

    if let Some(existing) = context.virtual_statements.get(statement) {
        if existing.signature != signature || existing.closed {
            debug!(
                statement = statement,
                "statement name reused with different signature"
            );
        }
    }

    let query = Arc::<str>::from(observer.query());
    let param_type_oids = Arc::<[i32]>::from(param_type_oids);

    context.virtual_statements.insert(
        statement.to_string(),
        VirtualStatement {
            generation,
            query: query.clone(),
            param_type_oids: param_type_oids.clone(),
            signature,
            closed: false,
        },
    );

    let backend_statement_name = session.backend().allocate_statement_name();
    build_parse_frame_into(
        output,
        &backend_statement_name,
        query.as_ref(),
        param_type_oids.as_ref(),
    );
    context.pending_parses.push_back(PendingParse {
        signature: Some(signature),
        backend_statement_name: Some(backend_statement_name.clone()),
        suppress_response: false,
    });
    in_flight_prepares.insert(signature, backend_statement_name);
}

struct PrepareOutcome {
    backend_statement_name: String,
}

fn ensure_prepared(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    signature: StatementSignature,
    query: &Arc<str>,
    param_type_oids: &Arc<[i32]>,
    output: &mut BytesMut,
    in_flight_prepares: &mut HashMap<StatementSignature, String>,
    suppress_response: bool,
) -> PrepareOutcome {
    if let Some(name) = in_flight_prepares.get(&signature) {
        return PrepareOutcome {
            backend_statement_name: name.clone(),
        };
    }

    if let Some(name) = session.backend().prepared_lookup(&signature) {
        return PrepareOutcome {
            backend_statement_name: name.to_string(),
        };
    }

    let backend_statement_name = session.backend().allocate_statement_name();
    build_parse_frame_into(
        output,
        &backend_statement_name,
        query.as_ref(),
        param_type_oids.as_ref(),
    );
    context.pending_parses.push_back(PendingParse {
        signature: Some(signature),
        backend_statement_name: Some(backend_statement_name.clone()),
        suppress_response,
    });
    in_flight_prepares.insert(signature, backend_statement_name.clone());
    PrepareOutcome {
        backend_statement_name,
    }
}

fn handle_bind_frame(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    frame: &[u8],
    output: &mut BytesMut,
    in_flight_prepares: &mut HashMap<StatementSignature, String>,
) {
    let observer = match BindFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Bind frame");
            output.extend_from_slice(frame);
            return;
        }
    };

    let statement = observer.statement();
    let portal = observer.portal();

    let Some(virtual_statement) = context.virtual_statements.get(statement) else {
        debug!(statement, "Bind references unknown prepared statement");
        output.extend_from_slice(frame);
        return;
    };

    if virtual_statement.closed {
        debug!(statement, "Bind references closed prepared statement");
        output.extend_from_slice(frame);
        return;
    }

    let signature = virtual_statement.signature;
    let prepared = ensure_prepared(
        context,
        session,
        signature,
        &virtual_statement.query,
        &virtual_statement.param_type_oids,
        output,
        in_flight_prepares,
        true,
    );

    let backend_portal_name = session.backend().allocate_portal_name();
    if !rewrite_bind_frame_into(
        output,
        frame,
        &backend_portal_name,
        &prepared.backend_statement_name,
    ) {
        output.extend_from_slice(frame);
        return;
    }

    context.virtual_portals.insert(
        portal.to_string(),
        PortalBinding {
            backend_portal_name: backend_portal_name.clone(),
        },
    );
}

fn handle_describe_frame(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    frame: &[u8],
    output: &mut BytesMut,
    in_flight_prepares: &mut HashMap<StatementSignature, String>,
) {
    let observer = match DescribeFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Describe frame");
            output.extend_from_slice(frame);
            return;
        }
    };

    match observer.target() {
        DescribeTarget::Statement => {
            let name = observer.name();
            let Some(virtual_statement) = context.virtual_statements.get(name) else {
                debug!(statement = name, "Describe references unknown prepared statement");
                output.extend_from_slice(frame);
                return;
            };

            if virtual_statement.closed {
                debug!(statement = name, "Describe references closed prepared statement");
                output.extend_from_slice(frame);
                return;
            }

            let signature = virtual_statement.signature;
            let prepared = ensure_prepared(
                context,
                session,
                signature,
                &virtual_statement.query,
                &virtual_statement.param_type_oids,
                output,
                in_flight_prepares,
                true,
            );

            build_describe_frame_into(
                output,
                DescribeTarget::Statement,
                &prepared.backend_statement_name,
            );
        }
        DescribeTarget::Portal => {
            let name = observer.name();
            let Some(binding) = context.virtual_portals.get(name) else {
                debug!(portal = name, "Describe references unknown portal");
                output.extend_from_slice(frame);
                return;
            };

            build_describe_frame_into(output, DescribeTarget::Portal, &binding.backend_portal_name);
        }
    }
}

fn handle_execute_frame(context: &mut FrontendContext, frame: &[u8], output: &mut BytesMut) {
    let observer = match ExecuteFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Execute frame");
            output.extend_from_slice(frame);
            return;
        }
    };

    let portal = observer.portal();
    let Some(binding) = context.virtual_portals.get(portal) else {
        debug!(portal, "Execute references unknown portal");
        output.extend_from_slice(frame);
        return;
    };

    build_execute_frame_into(output, &binding.backend_portal_name, observer.max_rows());
}

fn handle_close_frame(
    context: &mut FrontendContext,
    session: &mut GatewaySession,
    frame: &[u8],
    output: &mut BytesMut,
) {
    let observer = match CloseFrameObserver::new(frame) {
        Ok(observer) => observer,
        Err(err) => {
            debug!(error = %err, "failed to decode Close frame");
            output.extend_from_slice(frame);
            return;
        }
    };

    match observer.target() {
        CloseTarget::Statement => {
            let name = observer.name();
            let signature = context
                .virtual_statements
                .get_mut(name)
                .map(|statement| {
                    statement.closed = true;
                    statement.signature
                });
            if let Some(signature) = signature {
                let backend_name = session
                    .backend()
                    .prepared_lookup(&signature)
                    .map(str::to_string);
                if let Some(backend_name) = backend_name {
                    session.backend().prepared_remove_name(&backend_name);
                    build_close_frame_into(output, CloseTarget::Statement, &backend_name);
                    return;
                }
            }
            output.extend_from_slice(frame);
        }
        CloseTarget::Portal => {
            let name = observer.name();
            let removed = context.virtual_portals.remove(name);
            if let Some(binding) = removed {
                build_close_frame_into(output, CloseTarget::Portal, &binding.backend_portal_name);
                return;
            }
            output.extend_from_slice(frame);
        }
    }
}

fn build_parse_frame_into(
    output: &mut BytesMut,
    statement: &str,
    query: &str,
    param_type_oids: &[i32],
) {
    let body_len = statement.len() + 1 + query.len() + 1 + 2 + 4 * param_type_oids.len();
    output.reserve(1 + 4 + body_len);
    output.put_u8(b'P');
    output.put_u32((4 + body_len) as u32);
    output.extend_from_slice(statement.as_bytes());
    output.put_u8(0);
    output.extend_from_slice(query.as_bytes());
    output.put_u8(0);
    output.put_i16(param_type_oids.len() as i16);
    for &oid in param_type_oids {
        output.put_i32(oid);
    }
}

fn rewrite_bind_frame_into(
    output: &mut BytesMut,
    frame: &[u8],
    portal: &str,
    statement: &str,
) -> bool {
    if frame.len() < 5 || frame[0] != b'B' {
        return false;
    }

    let mut pos = 5;
    let Some(portal_rel) = memchr(0, &frame[pos..]) else {
        return false;
    };
    pos += portal_rel + 1;
    let Some(statement_rel) = memchr(0, &frame[pos..]) else {
        return false;
    };
    let tail_start = pos + statement_rel + 1;
    if tail_start > frame.len() {
        return false;
    }

    let tail = &frame[tail_start..];
    let body_len = portal.len() + 1 + statement.len() + 1 + tail.len();
    output.reserve(1 + 4 + body_len);
    output.put_u8(b'B');
    output.put_u32((4 + body_len) as u32);
    output.extend_from_slice(portal.as_bytes());
    output.put_u8(0);
    output.extend_from_slice(statement.as_bytes());
    output.put_u8(0);
    output.extend_from_slice(tail);
    true
}

fn build_describe_frame_into(output: &mut BytesMut, target: DescribeTarget, name: &str) {
    let body_len = 1 + name.len() + 1;
    output.reserve(1 + 4 + body_len);
    output.put_u8(b'D');
    output.put_u32((4 + body_len) as u32);
    let target_byte = match target {
        DescribeTarget::Portal => b'P',
        DescribeTarget::Statement => b'S',
    };
    output.put_u8(target_byte);
    output.extend_from_slice(name.as_bytes());
    output.put_u8(0);
}

fn build_execute_frame_into(output: &mut BytesMut, portal: &str, max_rows: i32) {
    let body_len = portal.len() + 1 + 4;
    output.reserve(1 + 4 + body_len);
    output.put_u8(b'E');
    output.put_u32((4 + body_len) as u32);
    output.extend_from_slice(portal.as_bytes());
    output.put_u8(0);
    output.put_i32(max_rows);
}

fn build_close_frame_into(output: &mut BytesMut, target: CloseTarget, name: &str) {
    let body_len = 1 + name.len() + 1;
    output.reserve(1 + 4 + body_len);
    output.put_u8(b'C');
    output.put_u32((4 + body_len) as u32);
    let target_byte = match target {
        CloseTarget::Portal => b'P',
        CloseTarget::Statement => b'S',
    };
    output.put_u8(target_byte);
    output.extend_from_slice(name.as_bytes());
    output.put_u8(0);
}

fn parse_and_log(query: &str, message_type: &'static str) {
    match parser::parse(query) {
        Ok(parsed) => debug!(message_type, ?parsed.ast, "parsed SQL"),
        Err(err) => debug!(message_type, error = %err, "failed to parse SQL"),
    }
}

fn is_reset_query(query: &str) -> bool {
    let trimmed = query.trim().trim_end_matches(';').trim();
    if trimmed.is_empty() {
        return false;
    }

    trimmed.eq_ignore_ascii_case("DISCARD ALL")
        || trimmed.eq_ignore_ascii_case("DEALLOCATE ALL")
        || trimmed.eq_ignore_ascii_case("RESET ALL")
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
