// Helpers for parsing tagged frontend frames with length prefixes.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TaggedFrame {
    pub len: usize,
    pub total_len: usize,
}

#[derive(Debug)]
pub enum TaggedFrameError {
    UnexpectedTag(u8),
    UnexpectedLength,
    InvalidLength(usize),
}

pub fn peek_tagged_frame(buf: &[u8], tag: u8) -> Option<TaggedFrame> {
    if buf.len() < 5 || buf[0] != tag {
        return None;
    }

    let len = u32::from_be_bytes([buf[1], buf[2], buf[3], buf[4]]) as usize;
    if len < 4 {
        return None;
    }

    let total_len = 1 + len;
    if buf.len() < total_len {
        return None;
    }

    Some(TaggedFrame { len, total_len })
}

pub fn parse_tagged_frame(frame: &[u8], tag: u8) -> Result<TaggedFrame, TaggedFrameError> {
    if frame.len() < 5 {
        return Err(TaggedFrameError::UnexpectedLength);
    }

    if frame[0] != tag {
        return Err(TaggedFrameError::UnexpectedTag(frame[0]));
    }

    let len = u32::from_be_bytes([frame[1], frame[2], frame[3], frame[4]]) as usize;
    if len < 4 {
        return Err(TaggedFrameError::InvalidLength(len));
    }

    let total_len = 1 + len;
    if frame.len() != total_len {
        return Err(TaggedFrameError::UnexpectedLength);
    }

    Ok(TaggedFrame { len, total_len })
}
