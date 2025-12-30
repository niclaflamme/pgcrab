use bytes::{BufMut, Bytes, BytesMut};
use secrecy::ExposeSecret;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    select,
};

use crate::config::shards::ShardsConfig;
use crate::config::users::UsersConfig;
use crate::gateway::GatewaySession;
use crate::shared_types::AuthStage;
use crate::wire_protocol::observers::password_message::PasswordMessageFrameObserver;
use crate::wire_protocol::observers::startup::StartupFrameObserver;
use crate::wire_protocol::types::MessageType;
use crate::wire_protocol::utils::peek_frontend;
use crate::{ErrorResponse, shared_types::BackendIdentity};

use super::sequence_tracker::SequenceTracker;

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

    gateway_session: Option<GatewaySession>,

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
            gateway_session: None,
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
                        self.process_sequence(sequence).await;
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

    async fn process_sequence(&mut self, seq_or_msg: BytesMut) {
        match self.stage {
            AuthStage::Startup => self.process_startup_message(seq_or_msg),
            AuthStage::Authenticating => self.process_authenticating_message(seq_or_msg).await,
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
        let Some(found) = peek_frontend(AuthStage::Startup, &message[..]) else {
            let err = ErrorResponse::protocol_violation("bad startup message");
            self.batch_response(&err.to_bytes());
            return;
        };

        match found.message_type {
            MessageType::SSLRequest => {
                // Not supporting TLSyet -> reply 'N' and stay in Startup
                // Client will send real Startup next
                self.batch_response(&Self::be_ssl_no());
            }

            MessageType::GSSENCRequest => {
                // Not supporting GSS yet -> reply 'N' and stay in Startup.
                // Client will send real Startup next
                self.batch_response(&Self::be_ssl_no());
            }

            MessageType::CancelRequest => {
                // TODO: Implement cancel request handling
                // NOTE: No response is expected by the client
            }

            MessageType::Startup => {
                let Ok(startup_frame) = StartupFrameObserver::new(&message) else {
                    let err = ErrorResponse::protocol_violation("bad startup message");
                    self.batch_response(&err.to_bytes());
                    return;
                };

                self.stage = AuthStage::Authenticating;

                let Some(username) = startup_frame.param("user") else {
                    let err = ErrorResponse::protocol_violation("startup missing user");
                    self.batch_response(&err.to_bytes());
                    return;
                };

                let Some(database) = startup_frame.param("database") else {
                    let err = ErrorResponse::protocol_violation("startup missing database");
                    self.batch_response(&err.to_bytes());
                    return;
                };

                self.username = Some(username.to_string());
                self.database = Some(database.to_string());

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
    async fn process_authenticating_message(&mut self, message: BytesMut) {
        let Ok(frame) = PasswordMessageFrameObserver::new(&message) else {
            let error = ErrorResponse::protocol_violation("cannot parse password");
            self.batch_response(&error.to_bytes());
            return;
        };

        match self.authenticate(frame.password()).await {
            Ok(_) => {
                self.stage = AuthStage::Ready;

                // AuthenticationOk
                self.batch_response(&Self::be_auth_ok());

                // ParameterStatus (keep it minimal but sane)
                self.batch_response(&Self::be_param_status("server_encoding", "UTF8"));
                self.batch_response(&Self::be_param_status("client_encoding", "UTF8"));

                // BackendKeyData
                self.batch_response(&Self::be_backend_key_data(self.backend_identity));

                // ReadyForQuery (idle)
                self.batch_response(&Self::be_ready(b'I'));
            }
            Err(e) => {
                let error = ErrorResponse::internal_error(&e);
                self.batch_response(&error.to_bytes());
            }
        }
    }

    async fn authenticate(&mut self, supplied_password: &str) -> Result<(), String> {
        // Never happens
        let Some(username) = self.username.as_ref() else {
            return Err("no username".to_string());
        };

        // Never happens
        let Some(database) = self.database.as_ref() else {
            return Err("no database".to_string());
        };

        let users = UsersConfig::snapshot();

        let maybe_user = users.iter().find(|u| {
            let matches_user = u.client_username == *username;
            let matches_database = u.database_name == *database;

            matches_user && matches_database
        });

        let Some(user) = maybe_user else {
            return Err("authentication failed".to_string());
        };

        let config_password = user.client_password.expose_secret();

        if config_password != supplied_password {
            return Err("authentication failed".to_string());
        }

        // Connect to backend
        // For now we just pick the first shard. TODO: Logic to pick correct shard based on user/db/hashing
        let shards = ShardsConfig::snapshot();
        let Some(shard) = shards.first() else {
            return Err("no database shards configured".to_string());
        };

        // TODO: Authenticate against the backend using shard.user and shard.password

        let session = GatewaySession::connect_to_shard(shard).await?;
        self.gateway_session = Some(session);

        Ok(())
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

    fn be_backend_key_data(identity: BackendIdentity) -> Bytes {
        let mut b = BytesMut::with_capacity(1 + 4 + 8);
        b.put_u8(b'K');
        b.put_u32(12);
        b.put_i32(identity.process_id);
        b.put_i32(identity.secret_key);
        b.freeze()
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
