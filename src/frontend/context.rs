use secrecy::ExposeSecret;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use crate::config::users::UsersConfig;
use crate::gateway::GatewaySession;
use crate::shared_types::{AuthStage, BackendIdentity, StatementSignature};

// -----------------------------------------------------------------------------
// ----- FrontendContext -------------------------------------------------------

#[derive(Debug)]
pub(crate) struct PendingParse {
    pub(crate) signature: Option<StatementSignature>,
    pub(crate) backend_statement_name: Option<String>,
    pub(crate) suppress_response: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct VirtualStatement {
    pub(crate) generation: u64,
    pub(crate) query: Arc<str>,
    pub(crate) param_type_oids: Arc<[i32]>,
    pub(crate) signature: StatementSignature,
    pub(crate) closed: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct PortalBinding {
    pub(crate) backend_portal_name: String,
}

#[derive(Debug)]
pub(crate) struct FrontendContext {
    pub(crate) database: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) backend_identity: BackendIdentity,
    pub(crate) gateway_session: Option<GatewaySession>,
    pub(crate) stage: AuthStage,
    pub(crate) is_admin: bool,
    pub(crate) virtual_statements: HashMap<String, VirtualStatement>,
    pub(crate) virtual_portals: HashMap<String, PortalBinding>,
    pub(crate) pending_parses: VecDeque<PendingParse>,
    pub(crate) pending_syncs: usize,
    close_after_flush: bool,
    upgrade_to_tls: bool,
}

impl FrontendContext {
    pub(crate) fn new() -> Self {
        Self {
            database: None,
            username: None,
            backend_identity: BackendIdentity::random(),
            gateway_session: None,
            stage: AuthStage::Startup,
            is_admin: false,
            virtual_statements: HashMap::new(),
            virtual_portals: HashMap::new(),
            pending_parses: VecDeque::new(),
            pending_syncs: 0,
            close_after_flush: false,
            upgrade_to_tls: false,
        }
    }

    pub(crate) fn request_close(&mut self) {
        self.close_after_flush = true;
    }

    pub(crate) fn should_close(&self) -> bool {
        self.close_after_flush
    }

    pub(crate) fn request_tls_upgrade(&mut self) {
        self.upgrade_to_tls = true;
    }

    pub(crate) fn wants_tls_upgrade(&self) -> bool {
        self.upgrade_to_tls
    }

    pub(crate) fn take_tls_upgrade(&mut self) -> bool {
        std::mem::take(&mut self.upgrade_to_tls)
    }

    pub(crate) async fn authenticate(&mut self, supplied_password: &str) -> Result<(), String> {
        let Some(username) = self.username.as_ref() else {
            return Err("no username".to_string());
        };

        let users = UsersConfig::snapshot();

        let maybe_user = users.iter().find(|u| u.client_username == *username);

        let Some(user) = maybe_user else {
            return Err("authentication failed".to_string());
        };

        let config_password = user.client_password.expose_secret();

        if config_password != supplied_password {
            return Err("authentication failed".to_string());
        }

        self.is_admin = user.admin;

        // TODO: Remove when gateway sessions are used, this would lead to dead code otherwise.
        self.gateway_session = None;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
