use bytes::BytesMut;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::shared_types::AuthStage;
use crate::wire_protocol::observers::cancel_request::CancelRequestFrameObserver;
use crate::wire_protocol::observers::startup::StartupFrameObserver;
use crate::wire_protocol::types::MessageType;
use crate::wire_protocol::utils::peek_frontend;

// -----------------------------------------------------------------------------
// ----- Startup Handler -------------------------------------------------------

pub(crate) fn handle_startup(
    context: &mut FrontendContext,
    buffers: &mut FrontendBuffers,
    message: BytesMut,
) {
    let Some(found) = peek_frontend(AuthStage::Startup, &message[..]) else {
        let err = ErrorResponse::protocol_violation("bad startup message");
        buffers.queue_response(&err.to_bytes());
        context.request_close();
        return;
    };

    match found.message_type {
        MessageType::SSLRequest => {
            // Not supporting TLS yet -> reply 'N' and stay in Startup.
            // Client will send real Startup next.
            buffers.queue_response(&responses::ssl_no());
        }

        MessageType::GSSENCRequest => {
            // Not supporting GSS yet -> reply 'N' and stay in Startup.
            // Client will send real Startup next.
            buffers.queue_response(&responses::ssl_no());
        }

        MessageType::CancelRequest => {
            let _ = CancelRequestFrameObserver::new(&message);
            // TODO: Route cancel request using pid/secret.
            // NOTE: No response is expected by the client.
            context.request_close();
        }

        MessageType::Startup => {
            let Ok(startup_frame) = StartupFrameObserver::new(&message) else {
                let err = ErrorResponse::protocol_violation("bad startup message");
                buffers.queue_response(&err.to_bytes());
                context.request_close();
                return;
            };

            let Some(username) = startup_frame.param("user").filter(|v| !v.is_empty()) else {
                let err = ErrorResponse::protocol_violation("startup missing user");
                buffers.queue_response(&err.to_bytes());
                context.request_close();
                return;
            };

            let database = startup_frame
                .param("database")
                .filter(|v| !v.is_empty())
                .unwrap_or(username);

            context.username = Some(username.to_string());
            context.database = Some(database.to_string());
            context.stage = AuthStage::Authenticating;

            buffers.queue_response(&responses::auth_cleartext());
        }

        _ => {
            // Protocol violation during startup.
            let err = ErrorResponse::protocol_violation("unexpected message in startup");
            buffers.queue_response(&err.to_bytes());
            context.request_close();
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
