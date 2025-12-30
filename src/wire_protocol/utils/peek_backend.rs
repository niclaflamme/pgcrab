pub fn peek_backend(bytes: &[u8]) -> Option<(u8, usize)> {
    if bytes.len() < 5 {
        return None;
    }

    let tag = bytes[0];
    let len = u32::from_be_bytes([bytes[1], bytes[2], bytes[3], bytes[4]]) as usize;
    if len < 4 {
        return None;
    }

    let total = 1 + len;
    if bytes.len() < total {
        return None;
    }

    Some((tag, len))
}

#[cfg(test)]
mod tests {
    use super::peek_backend;

    #[test]
    fn peek_backend_full_frame() {
        let frame = [b'Z', 0, 0, 0, 5, b'I'];
        let (tag, len) = peek_backend(&frame).expect("expected frame");
        assert_eq!(tag, b'Z');
        assert_eq!(len, 5);
    }

    #[test]
    fn peek_backend_rejects_short_buffer() {
        let frame = [b'Z', 0, 0, 0];
        assert!(peek_backend(&frame).is_none());
    }

    #[test]
    fn peek_backend_rejects_invalid_length() {
        let frame = [b'Z', 0, 0, 0, 3, b'I'];
        assert!(peek_backend(&frame).is_none());
    }

    #[test]
    fn peek_backend_requires_full_frame() {
        let frame = [b'Z', 0, 0, 0, 5];
        assert!(peek_backend(&frame).is_none());
    }
}
