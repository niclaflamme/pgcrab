// -----------------------------------------------------------------------------
// ----- ReadyStatus -----------------------------------------------------------

/// Maps to the ReadyForQuery transaction status byte that Postgres
/// sends after each command. We emit it so clients know whether the connection
/// is idle, in a transaction, or in a failed transaction block.
#[derive(Debug, Clone, Copy)]
pub enum ReadyStatus {
    Idle,
    InTransaction,
    FailedTransaction,
}

// -----------------------------------------------------------------------------
// ----- ReadyStatus: Static ---------------------------------------------------

impl ReadyStatus {
    pub(crate) fn as_byte(self) -> u8 {
        match self {
            ReadyStatus::Idle => b'I',
            ReadyStatus::InTransaction => b'T',
            ReadyStatus::FailedTransaction => b'E',
        }
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
