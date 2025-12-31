use bytes::{BufMut, BytesMut};
use memchr::memchr;
use std::collections::HashMap;
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

    let sequence = prepare_sequence(context, &mut session, sequence);

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
    sequence: BytesMut,
) -> BytesMut {
    let mut output = BytesMut::with_capacity(sequence.len());
    let mut in_flight_prepares: HashMap<StatementSignature, String> = HashMap::new();

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
            MessageType::Query => {
                handle_query_frame(context, session, frame);
                context.pending_syncs = context.pending_syncs.saturating_add(1);
                output.extend_from_slice(frame);
            }
            MessageType::Parse => {
                handle_parse_frame(
                    context,
                    session,
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

    context.virtual_statements.insert(
        statement.to_string(),
        VirtualStatement {
            generation,
            query: observer.query().to_string(),
            param_type_oids: param_type_oids.clone(),
            signature,
            closed: false,
        },
    );

    let backend_statement_name = session.backend().allocate_statement_name();
    let injected = build_parse_frame(&backend_statement_name, observer.query(), &param_type_oids);
    output.extend_from_slice(&injected);
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
    query: &str,
    param_type_oids: &[i32],
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
    let injected = build_parse_frame(&backend_statement_name, query, param_type_oids);
    output.extend_from_slice(&injected);
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
    let query = virtual_statement.query.clone();
    let param_type_oids = virtual_statement.param_type_oids.clone();

    let prepared = ensure_prepared(
        context,
        session,
        signature,
        &query,
        &param_type_oids,
        output,
        in_flight_prepares,
        true,
    );

    let backend_portal_name = session.backend().allocate_portal_name();
    let Some(rewritten) =
        rewrite_bind_frame(frame, &backend_portal_name, &prepared.backend_statement_name)
    else {
        output.extend_from_slice(frame);
        return;
    };

    context.virtual_portals.insert(
        portal.to_string(),
        PortalBinding {
            backend_portal_name: backend_portal_name.clone(),
        },
    );

    output.extend_from_slice(&rewritten);
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
            let query = virtual_statement.query.clone();
            let param_type_oids = virtual_statement.param_type_oids.clone();

            let prepared = ensure_prepared(
                context,
                session,
                signature,
                &query,
                &param_type_oids,
                output,
                in_flight_prepares,
                true,
            );

            let rewritten =
                build_describe_frame(DescribeTarget::Statement, &prepared.backend_statement_name);
            output.extend_from_slice(&rewritten);
        }
        DescribeTarget::Portal => {
            let name = observer.name();
            let Some(binding) = context.virtual_portals.get(name) else {
                debug!(portal = name, "Describe references unknown portal");
                output.extend_from_slice(frame);
                return;
            };

            let rewritten =
                build_describe_frame(DescribeTarget::Portal, &binding.backend_portal_name);
            output.extend_from_slice(&rewritten);
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

    let rewritten = build_execute_frame(&binding.backend_portal_name, observer.max_rows());
    output.extend_from_slice(&rewritten);
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
                    let rewritten = build_close_frame(CloseTarget::Statement, &backend_name);
                    output.extend_from_slice(&rewritten);
                    return;
                }
            }
            output.extend_from_slice(frame);
        }
        CloseTarget::Portal => {
            let name = observer.name();
            let removed = context.virtual_portals.remove(name);
            if let Some(binding) = removed {
                let rewritten = build_close_frame(CloseTarget::Portal, &binding.backend_portal_name);
                output.extend_from_slice(&rewritten);
                return;
            }
            output.extend_from_slice(frame);
        }
    }
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

fn rewrite_bind_frame(frame: &[u8], portal: &str, statement: &str) -> Option<BytesMut> {
    if frame.len() < 5 || frame[0] != b'B' {
        return None;
    }

    let mut pos = 5;
    let portal_rel = memchr(0, &frame[pos..])?;
    pos += portal_rel + 1;
    let statement_rel = memchr(0, &frame[pos..])?;
    let tail_start = pos + statement_rel + 1;
    if tail_start > frame.len() {
        return None;
    }

    let tail = &frame[tail_start..];
    let body_len = portal.len() + 1 + statement.len() + 1 + tail.len();
    let mut out = BytesMut::with_capacity(1 + 4 + body_len);
    out.put_u8(b'B');
    out.put_u32((4 + body_len) as u32);
    out.extend_from_slice(portal.as_bytes());
    out.put_u8(0);
    out.extend_from_slice(statement.as_bytes());
    out.put_u8(0);
    out.extend_from_slice(tail);
    Some(out)
}

fn build_describe_frame(target: DescribeTarget, name: &str) -> BytesMut {
    let body_len = 1 + name.len() + 1;
    let mut frame = BytesMut::with_capacity(1 + 4 + body_len);
    frame.put_u8(b'D');
    frame.put_u32((4 + body_len) as u32);
    let target_byte = match target {
        DescribeTarget::Portal => b'P',
        DescribeTarget::Statement => b'S',
    };
    frame.put_u8(target_byte);
    frame.extend_from_slice(name.as_bytes());
    frame.put_u8(0);
    frame
}

fn build_execute_frame(portal: &str, max_rows: i32) -> BytesMut {
    let body_len = portal.len() + 1 + 4;
    let mut frame = BytesMut::with_capacity(1 + 4 + body_len);
    frame.put_u8(b'E');
    frame.put_u32((4 + body_len) as u32);
    frame.extend_from_slice(portal.as_bytes());
    frame.put_u8(0);
    frame.put_i32(max_rows);
    frame
}

fn build_close_frame(target: CloseTarget, name: &str) -> BytesMut {
    let body_len = 1 + name.len() + 1;
    let mut frame = BytesMut::with_capacity(1 + 4 + body_len);
    frame.put_u8(b'C');
    frame.put_u32((4 + body_len) as u32);
    let target_byte = match target {
        CloseTarget::Portal => b'P',
        CloseTarget::Statement => b'S',
    };
    frame.put_u8(target_byte);
    frame.extend_from_slice(name.as_bytes());
    frame.put_u8(0);
    frame
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

    match trimmed.to_ascii_uppercase().as_str() {
        "DISCARD ALL" | "DEALLOCATE ALL" | "RESET ALL" => true,
        _ => false,
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
