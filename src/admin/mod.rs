use bytes::{BufMut, Bytes, BytesMut};

use crate::analytics::{self, ParseCacheStats};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminCommand {
    ShowAnalytics,
}

pub fn parse_cache_stats() -> ParseCacheStats {
    analytics::snapshot()
}

pub fn format_parse_cache_stats(stats: ParseCacheStats) -> String {
    format!(
        "parse_cache_hits={}\nparse_cache_misses={}",
        stats.hits, stats.misses
    )
}

pub fn parse_admin_command(query: &str) -> Option<AdminCommand> {
    let mut trimmed = query.trim();
    if let Some(without_semicolon) = trimmed.strip_suffix(';') {
        trimmed = without_semicolon.trim();
    }

    if trimmed.eq_ignore_ascii_case("SHOW PGCRAB ANALYTICS") {
        return Some(AdminCommand::ShowAnalytics);
    }

    None
}

pub fn command_responses(command: AdminCommand) -> Vec<Bytes> {
    match command {
        AdminCommand::ShowAnalytics => analytics_responses(),
    }
}

fn analytics_responses() -> Vec<Bytes> {
    let stats = analytics::snapshot();
    let rows = [
        ("parse_cache_hits", stats.hits.to_string()),
        ("parse_cache_misses", stats.misses.to_string()),
    ];

    let mut responses = Vec::with_capacity(2 + rows.len());
    responses.push(row_description(&["metric", "value"]));
    for (metric, value) in &rows {
        responses.push(data_row(&[metric, value.as_str()]));
    }
    responses.push(command_complete(&format!("SELECT {}", rows.len())));
    responses
}

fn row_description(columns: &[&str]) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i16(columns.len() as i16);
    for name in columns {
        body.extend_from_slice(name.as_bytes());
        body.put_u8(0);
        body.put_i32(0); // table oid
        body.put_i16(0); // column attr
        body.put_i32(25); // text type oid
        body.put_i16(-1); // type size
        body.put_i32(-1); // type modifier
        body.put_i16(0); // text format
    }

    let mut frame = BytesMut::with_capacity(1 + 4 + body.len());
    frame.put_u8(b'T');
    frame.put_u32((4 + body.len()) as u32);
    frame.extend_from_slice(&body);
    frame.freeze()
}

fn data_row(values: &[&str]) -> Bytes {
    let mut body = BytesMut::new();
    body.put_i16(values.len() as i16);
    for value in values {
        body.put_i32(value.len() as i32);
        body.extend_from_slice(value.as_bytes());
    }
    let mut frame = BytesMut::with_capacity(1 + 4 + body.len());
    frame.put_u8(b'D');
    frame.put_u32((4 + body.len()) as u32);
    frame.extend_from_slice(&body);
    frame.freeze()
}

fn command_complete(tag: &str) -> Bytes {
    let mut body = BytesMut::new();
    body.extend_from_slice(tag.as_bytes());
    body.put_u8(0);
    let mut frame = BytesMut::with_capacity(1 + 4 + body.len());
    frame.put_u8(b'C');
    frame.put_u32((4 + body.len()) as u32);
    frame.extend_from_slice(&body);
    frame.freeze()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_show_analytics_command() {
        let cmd = parse_admin_command("SHOW PGCRAB ANALYTICS;");
        assert_eq!(cmd, Some(AdminCommand::ShowAnalytics));
    }
}
