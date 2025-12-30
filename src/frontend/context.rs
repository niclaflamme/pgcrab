use secrecy::ExposeSecret;

use crate::config::users::UsersConfig;
use crate::gateway::GatewaySession;
use crate::shared_types::{AuthStage, BackendIdentity};

// -----------------------------------------------------------------------------
// ----- FrontendContext -------------------------------------------------------

#[derive(Debug)]
pub(crate) struct FrontendContext {
    pub(crate) database: Option<String>,
    pub(crate) username: Option<String>,
    pub(crate) backend_identity: BackendIdentity,
    pub(crate) gateway_session: Option<GatewaySession>,
    pub(crate) stage: AuthStage,
    close_after_flush: bool,
}

impl FrontendContext {
    pub(crate) fn new() -> Self {
        Self {
            database: None,
            username: None,
            backend_identity: BackendIdentity::random(),
            gateway_session: None,
            stage: AuthStage::Startup,
            close_after_flush: false,
        }
    }

    pub(crate) fn request_close(&mut self) {
        self.close_after_flush = true;
    }

    pub(crate) fn should_close(&self) -> bool {
        self.close_after_flush
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

        // TODO: Remove when gateway sessions are used, this would lead to dead code otherwise.
        self.gateway_session = None;

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
