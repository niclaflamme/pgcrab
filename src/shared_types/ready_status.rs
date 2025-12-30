/// ReadyForQuery transaction status.
#[derive(Debug, Clone, Copy)]
pub enum ReadyStatus {
    Idle,
    // InTransaction,
    // FailedTransaction,
}

impl ReadyStatus {
    pub(crate) fn as_byte(self) -> u8 {
        match self {
            ReadyStatus::Idle => b'I',
        }
    }
}
