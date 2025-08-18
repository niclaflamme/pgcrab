use rand::Rng;

// -----------------------------------------------------------------------------
// ----- BackendIdentity -------------------------------------------------------

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
