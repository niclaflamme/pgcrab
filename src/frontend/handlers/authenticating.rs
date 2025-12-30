use bytes::BytesMut;

use crate::ErrorResponse;
use crate::frontend::buffers::FrontendBuffers;
use crate::frontend::context::FrontendContext;
use crate::frontend::proxy_responses as responses;
use crate::shared_types::AuthStage;
use crate::shared_types::ReadyStatus;
use crate::wire_protocol::observers::password_message::PasswordMessageFrameObserver;

// -----------------------------------------------------------------------------
// ----- Authenticating Handler -----------------------------------------------

pub(crate) async fn handle_authenticating(
    context: &mut FrontendContext,
    buffers: &mut FrontendBuffers,
    message: BytesMut,
) {
    let Ok(frame) = PasswordMessageFrameObserver::new(&message) else {
        let error = ErrorResponse::protocol_violation("cannot parse password");
        buffers.queue_response(&error.to_bytes());
        return;
    };

    match context.authenticate(frame.password()).await {
        Ok(_) => {
            context.stage = AuthStage::Ready;

            // AuthenticationOk
            buffers.queue_response(&responses::auth_ok());

            // ParameterStatus (keep it minimal but sane)
            buffers.queue_response(&responses::param_status("server_encoding", "UTF8"));
            buffers.queue_response(&responses::param_status("client_encoding", "UTF8"));

            // BackendKeyData
            buffers.queue_response(&responses::backend_key_data(context.backend_identity));

            // ReadyForQuery (idle)
            buffers.queue_response(&responses::ready_with_status(ReadyStatus::Idle));
        }
        Err(e) => {
            let error = ErrorResponse::internal_error(&e);
            buffers.queue_response(&error.to_bytes());
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
