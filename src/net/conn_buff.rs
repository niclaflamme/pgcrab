//! net/dual_buffer.rs
//!
//! Two-slab (4 KB each) scratch buffer meant for high-throughput I/O.
//! Primary is the active writer; when full we spill into secondary.
//! Caller flushes a full slab and calls `rebalance` so primary is always the one being filled.

use bytes::BytesMut;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const SLAB_SIZE: usize = 4 * 1024;

// -----------------------------------------------------------------------------
// ----- ConnBuff --------------------------------------------------------------

pub struct ConnBuff {
    primary: BytesMut,
    secondary: BytesMut,
    frame_open: bool,
}

impl ConnBuff {
    pub fn new() -> Self {
        Self {
            primary: BytesMut::with_capacity(SLAB_SIZE),
            secondary: BytesMut::with_capacity(SLAB_SIZE),
            frame_open: false,
        }
    }
}

// -----------------------------------------------------------------------------
// ----- ConnBuff: Public Methods ----------------------------------------------

impl ConnBuff {
    #[inline]
    pub fn begin_frame(&mut self) {
        debug_assert!(!self.frame_open, "frame already open");

        self.frame_open = true;
    }

    /// Append bytes honoring the SLAB_SIZE. Returns how many were written.
    #[inline]
    pub fn push(&mut self, mut input: &[u8]) -> usize {
        debug_assert!(self.frame_open, "call begin_frame() first");

        let before = input.len();
        while !input.is_empty() {
            let room = SLAB_SIZE - self.primary.len();
            if room == 0 {
                let room2 = SLAB_SIZE - self.secondary.len();
                if room2 == 0 {
                    break; // both full → caller must flush
                }
                let take = room2.min(input.len());
                self.secondary.extend_from_slice(&input[..take]);
                input = &input[take..];
            } else {
                let take = room.min(input.len());
                self.primary.extend_from_slice(&input[..take]);
                input = &input[take..];
            }
        }
        before - input.len() // <= return value
    }

    /// Close the frame; decide if we must flush right away.
    #[inline]
    pub fn finish_frame(&mut self) {
        debug_assert!(self.frame_open);
        self.frame_open = false;
        // nothing else to do – needs_flush() now reflects frame boundary
    }

    /// true when caller must flush because both slabs are full
    #[inline]
    pub fn needs_flush(&self) -> bool {
        self.primary.len() == SLAB_SIZE || !self.secondary.is_empty()
    }

    /// pop up to one full slab for writing
    #[inline]
    pub fn take_full(&mut self) -> Option<BytesMut> {
        if self.primary.len() == SLAB_SIZE {
            Some(self.primary.split())
        } else {
            None
        }
    }

    /// after a flush swap if primary empty and secondary not
    #[inline]
    pub fn rebalance(&mut self) {
        if self.primary.is_empty() && !self.secondary.is_empty() {
            std::mem::swap(&mut self.primary, &mut self.secondary);
        }
    }
}

// -----------------------------------------------------------------------------
// ----- ConnBuff: Private Methods ---------------------------------------------

impl ConnBuff {}

// -----------------------------------------------------------------------------
// ----- ConnBuff: Test Helpers ------------------------------------------------

impl ConnBuff {
    #[cfg(test)]
    pub fn primary_len(&self) -> usize {
        self.primary.len()
    }

    #[cfg(test)]
    pub fn secondary_len(&self) -> usize {
        self.secondary.len()
    }
}

// -----------------------------------------------------------------------------
// ----- Tests -----------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// helper that builds a vec of `len` identical bytes
    fn bytes(len: usize) -> Vec<u8> {
        vec![0xAA; len]
    }

    #[test]
    fn push_into_primary_only() {
        let mut buf = ConnBuff::new();
        let n = SLAB_SIZE / 2;
        let written = buf.push(&bytes(n));
        assert_eq!(written, n);
        assert_eq!(buf.primary_len(), n);
        assert_eq!(buf.secondary_len(), 0);
        assert!(!buf.needs_flush());
    }

    #[test]
    fn push_fills_primary_then_secondary() {
        let mut buf = ConnBuff::new();
        let written = buf.push(&bytes(SLAB_SIZE + 100));
        assert_eq!(written, SLAB_SIZE + 100);
        assert_eq!(buf.primary_len(), SLAB_SIZE);
        assert_eq!(buf.secondary_len(), 100);
        assert!(!buf.needs_flush());
    }

    #[test]
    fn stops_when_both_full() {
        let mut buf = ConnBuff::new();
        buf.push(&bytes(SLAB_SIZE * 2)); // fill both
        assert!(buf.needs_flush());

        let extra = buf.push(&[9, 9, 9]); // nothing fits
        assert_eq!(extra, 0);
        assert!(buf.needs_flush());
    }

    #[test]
    fn overflow_write_truncates() {
        let mut buf = ConnBuff::new();
        let attempt = SLAB_SIZE * 2 + 123; // 123 over capacity
        let written = buf.push(&bytes(attempt));
        assert_eq!(written, SLAB_SIZE * 2);
        assert_eq!(buf.primary_len(), SLAB_SIZE);
        assert_eq!(buf.secondary_len(), SLAB_SIZE);
        assert!(buf.needs_flush());
    }

    #[test]
    fn take_full_and_rebalance() {
        let mut buf = ConnBuff::new();
        buf.push(&bytes(SLAB_SIZE * 2)); // both full
        let slab = buf.take_full().expect("full slab");
        assert_eq!(slab.len(), SLAB_SIZE);

        buf.rebalance(); // secondary promoted
        assert_eq!(buf.primary_len(), SLAB_SIZE);
        assert_eq!(buf.secondary_len(), 0);
        assert!(buf.primary_len() == SLAB_SIZE);
    }

    #[test]
    fn partial_flush_does_not_rebalance() {
        let mut buf = ConnBuff::new();
        buf.push(&bytes(SLAB_SIZE + 50)); // spill into secondary
        let _ = buf.take_full().unwrap(); // take primary only
        buf.rebalance(); // secondary promoted

        assert_eq!(buf.primary_len(), 50);
        assert_eq!(buf.secondary_len(), 0);
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
