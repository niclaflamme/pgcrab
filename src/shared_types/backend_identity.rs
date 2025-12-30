use rand::Rng;

// -----------------------------------------------------------------------------
// ----- BackendIdentity -------------------------------------------------------

/// Represents the Postgres backend "pid + secret key" pair.
/// Postgres assigns these values so clients can send a CancelRequest for an
/// in-flight query on a specific backend connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackendIdentity {
    pub process_id: i32,
    pub secret_key: i32,
}

// -----------------------------------------------------------------------------
// ----- BackendIdentity: Static -----------------------------------------------

impl BackendIdentity {
    pub fn random() -> Self {
        let mut rng = rand::rng();

        BackendIdentity {
            process_id: rng.random(),
            secret_key: rng.random(),
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
