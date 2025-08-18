use std::collections::VecDeque;

use crate::shared_types::AuthStage;
use crate::wire_protocol_v2::types::MessageType;

// -----------------------------------------------------------------------------
// ----- Constants -------------------------------------------------------------

const MAX_COUNT_BEFORE_FLUSH: usize = 10;
const MAX_BYTES_BEFORE_FLUSH: usize = 4 * 1024;

// -----------------------------------------------------------------------------
// ----- SequenceTracker -------------------------------------------------------

#[derive(Debug)]
pub struct SequenceTracker {
    frames: VecDeque<FrameSummary>,
}

#[derive(Debug)]
pub struct FrameSummary {
    pub message_type: MessageType,
    pub len: usize,
}

// -----------------------------------------------------------------------------
// ----- Internal: FlushBoundary -----------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct FlushBoundary {
    pub frames_to_flush: usize,
    pub bytes_to_flush: usize,
}

// -----------------------------------------------------------------------------
// ----- SequenceTracker: Static -----------------------------------------------

impl SequenceTracker {
    pub fn new() -> Self {
        Self {
            frames: VecDeque::new(),
        }
    }
}

// -----------------------------------------------------------------------------
// ----- SequenceTracker: Public -----------------------------------------------

impl SequenceTracker {
    pub fn push(&mut self, message_type: MessageType, len: usize) {
        self.frames.push_back(FrameSummary { len, message_type });
    }

    pub fn take_until_flush(&mut self, stage: AuthStage) -> Option<usize> {
        let boundary = self.find_flush_boundary(stage)?;
        self.frames.drain(0..boundary.frames_to_flush);
        Some(boundary.bytes_to_flush)
    }

    /// Length of all frames in the tracker, in bytes
    pub fn len(&self) -> usize {
        self.frames.iter().map(|meta| meta.len).sum()
    }

    /// Count the number of frames in the tracker
    pub fn count(&self) -> usize {
        self.frames.len()
    }

    /// Check if the tracker is empty
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

// -----------------------------------------------------------------------------
// ----- SequenceTracker: Private ----------------------------------------------

impl SequenceTracker {
    fn find_flush_boundary(&self, stage: AuthStage) -> Option<FlushBoundary> {
        match stage {
            AuthStage::Startup => self.find_flush_boundary_startup(),
            AuthStage::Authenticating => self.find_flush_boundary_authenticating(),
            AuthStage::Ready => self.find_flush_boundary_ready(),
        }
    }

    fn find_flush_boundary_startup(&self) -> Option<FlushBoundary> {
        Some(FlushBoundary {
            frames_to_flush: 1,
            bytes_to_flush: self.frames.front()?.len,
        })
    }

    fn find_flush_boundary_authenticating(&self) -> Option<FlushBoundary> {
        Some(FlushBoundary {
            frames_to_flush: 1,
            bytes_to_flush: self.frames.front()?.len,
        })
    }

    fn find_flush_boundary_ready(&self) -> Option<FlushBoundary> {
        let mut bytes_to_flush = 0;
        for (index, meta) in self.frames.iter().enumerate() {
            bytes_to_flush += meta.len;
            let is_boundary = match meta.message_type {
                MessageType::Sync => true,
                MessageType::Flush => true,
                MessageType::Terminate => true,
                MessageType::Query => true,
                _ => false,
            };
            let is_too_large =
                bytes_to_flush >= MAX_BYTES_BEFORE_FLUSH || index >= MAX_COUNT_BEFORE_FLUSH;
            if is_boundary || is_too_large {
                return Some(FlushBoundary {
                    frames_to_flush: index + 1,
                    bytes_to_flush,
                });
            }
        }
        None
    }
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
