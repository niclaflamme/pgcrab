use bytes::BytesMut;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::shared_types::AuthStage;
use crate::wire::observers::cancel_request::CancelRequestFrameObserver;
use crate::wire::observers::startup::{NewStartupObserverError, StartupFrameObserver};
use crate::wire::types::MessageType;
use crate::wire::utils::peek_frontend;

// -----------------------------------------------------------------------------
// ----- Startup Handler -------------------------------------------------------

pub(crate) fn handle_startup(
    context: &mut FrontendContext,
    buffers: &mut FrontendBuffers,
    message: BytesMut,
    tls_available: bool,
) {
    let Some(found) = peek_frontend(AuthStage::Startup, &message[..]) else {
        let err = ErrorResponse::protocol_violation("bad startup message");
        buffers.queue_response(&err.to_bytes());
        context.request_close();
        return;
    };

    match found.message_type {
        MessageType::SSLRequest => {
            if tls_available {
                buffers.queue_response(&responses::ssl_yes());
                context.request_tls_upgrade();
            } else {
                buffers.queue_response(&responses::ssl_no());
            }
        }

        MessageType::GSSENCRequest => {
            // Not supporting GSS encryption -> reply 'N' and stay in Startup.
            // Client will send real Startup next.
            buffers.queue_response(&responses::gssenc_no());
        }

        MessageType::CancelRequest => {
            if let Ok(frame) = CancelRequestFrameObserver::new(&message) {
                let _pid = frame.pid();
                let _secret = frame.secret();
            }
            // CancelRequest expects no response; close after reading the frame.
            context.request_close();
        }

        MessageType::Startup => {
            let startup_frame = match StartupFrameObserver::new(&message) {
                Ok(frame) => frame,
                Err(NewStartupObserverError::UnexpectedVersion(version)) => {
                    let err = ErrorResponse::protocol_violation(
                        "unsupported startup protocol version",
                    )
                    .with_detail(format!("version: {}", version));
                    buffers.queue_response(&err.to_bytes());
                    context.request_close();
                    return;
                }
                Err(_) => {
                    let err = ErrorResponse::protocol_violation("bad startup message");
                    buffers.queue_response(&err.to_bytes());
                    context.request_close();
                    return;
                }
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
