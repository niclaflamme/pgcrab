use bytes::{BufMut, Bytes, BytesMut};
use secrecy::ExposeSecret;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
};

use crate::config::users::UsersConfig;
use crate::shared_types::AuthStage;
use crate::wire_protocol::WireSerializable;
use crate::wire_protocol::backend::BackendKeyDataFrame;
use crate::wire_protocol::frontend::{MessageType, frames as fe_frames};
use crate::wire_protocol_v2::utils::peek_frontend;
use crate::{ErrorResponse, shared_types::BackendIdentity};

use super::{peek::peek, sequence_tracker::SequenceTracker};

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SCRATCH_CAPACITY_HINT: usize = 4096;

// -----------------------------------------------------------------------------
// ----- FrontendConnection ----------------------------------------------------

#[derive(Debug)]
pub struct FrontendConnection {
    database: Option<String>,
    username: Option<String>,

    #[allow(dead_code)]
    backend_identity: BackendIdentity,

    stage: AuthStage,

    inbox: BytesMut,
    inbox_tracker: SequenceTracker,

    outbox: BytesMut,

    reader: tokio::net::tcp::OwnedReadHalf,
    writer: tokio::net::tcp::OwnedWriteHalf,
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Static --------------------------------------------

impl FrontendConnection {
    pub fn new(stream: TcpStream) -> Self {
        let (reader, writer) = stream.into_split();

        Self {
            database: None,
            username: None,
            backend_identity: BackendIdentity::random(),
            stage: AuthStage::Startup,
            inbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            inbox_tracker: SequenceTracker::new(),
            outbox: BytesMut::with_capacity(SCRATCH_CAPACITY_HINT),
            reader,
            writer,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Public --------------------------------------------

impl FrontendConnection {
    pub async fn serve(mut self) -> std::io::Result<()> {
        loop {
            select! {

                // -- Client Requests --
                read_res = async {
                    self.inbox.reserve(SCRATCH_CAPACITY_HINT);
                    self.reader.read_buf(&mut self.inbox).await
                } => {
                    let n = read_res?;
                    if n == 0 { break; }

                    // 1) check all untracked frames into inbox_tracker
                    self.track_new_inbox_frames();

                    // 2) process all complete sequences in the inbox
                    while let Some(sequence) = self.pull_next_sequence() {
                        self.process_sequence(sequence);
                    }

                    // 3) flush outbox to writer
                    self.flush_outbox().await?;
                }

                // -- Async responses --
            }
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Private -------------------------------------------

impl FrontendConnection {
    fn track_new_inbox_frames(&mut self) {
        loop {
            let cursor = self.inbox_tracker.len();

            let frame_slice = &self.inbox[cursor..];
            if frame_slice.is_empty() {
                break;
            }

            let Some(result) = peek_frontend(self.stage, frame_slice) else {
                break;
            };

            self.inbox_tracker.push(result.message_type, result.len);
        }
    }

    fn pull_next_sequence(&mut self) -> Option<BytesMut> {
        let Some(bytes_to_take) = self.inbox_tracker.take_until_flush(self.stage) else {
            return None;
        };

        let sequence = self.inbox.split_to(bytes_to_take);

        Some(sequence)
    }

    fn process_sequence(&mut self, seq_or_msg: BytesMut) {
        match self.stage {
            AuthStage::Startup => self.process_startup_message(seq_or_msg),
            AuthStage::Authenticating => self.process_authenticating_message(seq_or_msg),
            AuthStage::Ready => self.process_ready_sequence(seq_or_msg),
        }
    }

    fn batch_response(&mut self, response: &Bytes) {
        self.outbox.extend_from_slice(&response);
    }

    async fn flush_outbox(&mut self) -> std::io::Result<()> {
        if !self.outbox.is_empty() {
            self.writer.write_all_buf(&mut self.outbox).await?;
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Process Startup Sequence --------------------------

impl FrontendConnection {
    fn process_startup_message(&mut self, message: BytesMut) {
        let found = peek(AuthStage::Startup, &message[..]).unwrap();

        match found.message_type {
            MessageType::SslRequest => {
                // Not supporting TLSyet -> reply 'N' and stay in Startup
                // Client will send real Startup next
                self.batch_response(&Self::be_ssl_no());
            }

            MessageType::GssEncRequest => {
                // Not supporting GSS yet -> reply 'N' and stay in Startup.
                // Client will send real Startup next
                self.batch_response(&Self::be_ssl_no());
            }

            MessageType::CancelRequest => {
                // TODO: Implement cancel request handling
                // NOTE: No response is expected by the client
            }

            MessageType::Startup => {
                let Ok(startup_frame) = fe_frames::StartupFrame::from_bytes(&message) else {
                    let err = ErrorResponse::internal_error("bad startup message");
                    self.batch_response(&err.to_bytes());
                    return;
                };

                self.stage = AuthStage::Authenticating;

                self.username = Some(startup_frame.user.to_string());
                self.database = Some(startup_frame.database.to_string());

                self.batch_response(&Self::be_auth_cleartext());
            }

            _ => {
                // protocol violation during startup
                let err = ErrorResponse::internal_error("unexpected message in startup");
                self.batch_response(&err.to_bytes());
            }
        }
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Process Authenticating Sequence -------------------

impl FrontendConnection {
    fn process_authenticating_message(&mut self, message: BytesMut) {
        let Ok(frame) = fe_frames::PasswordMessageFrame::from_bytes(&message) else {
            let error = ErrorResponse::internal_error("cannot parse password");
            self.batch_response(&error.to_bytes());
            return;
        };

        let ok = self.authenticate(frame.password);
        if !ok {
            let error = ErrorResponse::internal_error("password authentication failed");
            self.batch_response(&error.to_bytes());
            return;
        }

        self.stage = AuthStage::Ready;

        // AuthenticationOk
        self.batch_response(&Self::be_auth_ok());

        // ParameterStatus (keep it minimal but sane)
        self.batch_response(&Self::be_param_status("server_encoding", "UTF8"));
        self.batch_response(&Self::be_param_status("client_encoding", "UTF8"));

        // BackendKeyData
        let backend_key_data_frame = BackendKeyDataFrame::from(&self.backend_identity);
        self.batch_response(&backend_key_data_frame.to_bytes_safe());

        // ReadyForQuery (idle)
        self.batch_response(&Self::be_ready(b'I'));
    }

    fn authenticate(&mut self, supplied_password: &str) -> bool {
        // Never happens
        let Some(username) = self.username.as_ref() else {
            return false;
        };

        // Never happens
        let Some(database) = self.database.as_ref() else {
            return false;
        };

        let users = UsersConfig::snapshot();

        let maybe_user = users.iter().find(|u| {
            let matches_user = u.client_username == username.to_string();
            let matches_database = u.database_name == database.to_string();

            matches_user && matches_database
        });

        let Some(user) = maybe_user else {
            return false;
        };

        let config_password = user.client_password.expose_secret();

        config_password == supplied_password
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Process Ready Sequence ----------------------------

impl FrontendConnection {
    fn process_ready_sequence(&mut self, sequence: BytesMut) {
        println!("i am here: process_ready_sequence (len={})", sequence.len());
        println!("R.SEQ: {:?}", sequence);

        // Dummy failure so psql doesn't hang. Then return to idle.
        let err = ErrorResponse::internal_error("statement execution not implemented");
        self.batch_response(&err.to_bytes());
        self.batch_response(&Self::be_ready(b'I'));
    }
}

// -----------------------------------------------------------------------------
// ----- FrontendConnection: Helpers, Backend Messages -------------------------

impl FrontendConnection {
    fn be_ssl_no() -> Bytes {
        Bytes::from_static(b"N")
    }

    fn be_auth_cleartext() -> Bytes {
        let mut b = BytesMut::with_capacity(1 + 4 + 4);
        b.put_u8(b'R');
        b.put_u32(8);
        b.put_i32(3);
        b.freeze()
    }

    fn be_auth_ok() -> Bytes {
        let mut b = BytesMut::with_capacity(1 + 4 + 4);
        b.put_u8(b'R');
        b.put_u32(8);
        b.put_i32(0);
        b.freeze()
    }

    fn be_param_status(name: &str, value: &str) -> Bytes {
        let n = name.as_bytes();
        let v = value.as_bytes();
        let payload_len = 4 + n.len() + 1 + v.len() + 1;
        let mut b = BytesMut::with_capacity(1 + payload_len);
        b.put_u8(b'S');
        b.put_u32(payload_len as u32);
        b.extend_from_slice(n);
        b.put_u8(0);
        b.extend_from_slice(v);
        b.put_u8(0);
        b.freeze()
    }

    fn be_ready(status: u8) -> Bytes {
        let mut b = BytesMut::with_capacity(1 + 4 + 1);
        b.put_u8(b'Z');
        b.put_u32(5);
        b.put_u8(status);
        b.freeze()
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
