use bytes::{BufMut, Bytes, BytesMut};

use crate::analytics;
use crate::frontend::context::FrontendContext;
use crate::gateway::GatewayPools;
use crate::parser;
use crate::shared_types::AuthStage;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub len: usize,
    pub capacity: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminCommand {
    ShowAnalytics,
    ShowPools,
    ShowSession,
}

pub fn parse_cache_stats() -> CacheStats {
    let counters = analytics::snapshot();
    let cache = parser::cache_stats();

    CacheStats {
        hits: counters.hits,
        misses: counters.misses,
        evictions: counters.evictions,
        len: cache.len,
        capacity: cache.capacity,
    }
}

pub fn format_parse_cache_stats(stats: CacheStats) -> String {
    format!(
        "parse_cache_hits={}\nparse_cache_misses={}\nparse_cache_evictions={}\nparse_cache_size={}\nparse_cache_capacity={}",
        stats.hits, stats.misses, stats.evictions, stats.len, stats.capacity
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

    if trimmed.eq_ignore_ascii_case("SHOW PGCRAB POOLS") {
        return Some(AdminCommand::ShowPools);
    }

    if trimmed.eq_ignore_ascii_case("SHOW PGCRAB SESSION") {
        return Some(AdminCommand::ShowSession);
    }

    None
}

pub(crate) async fn command_responses(
    command: AdminCommand,
    context: &FrontendContext,
    pools: &GatewayPools,
) -> Vec<Bytes> {
    match command {
        AdminCommand::ShowAnalytics => analytics_responses(),
        AdminCommand::ShowPools => pools_responses(pools).await,
        AdminCommand::ShowSession => session_responses(context),
    }
}

fn analytics_responses() -> Vec<Bytes> {
    let stats = parse_cache_stats();
    let rows = [
        ("parse_cache_hits", stats.hits.to_string()),
        ("parse_cache_misses", stats.misses.to_string()),
        ("parse_cache_evictions", stats.evictions.to_string()),
        ("parse_cache_size", stats.len.to_string()),
        ("parse_cache_capacity", stats.capacity.to_string()),
    ];

    let mut responses = Vec::with_capacity(2 + rows.len());
    responses.push(row_description(&["metric", "value"]));
    for (metric, value) in &rows {
        responses.push(data_row(&[metric, value.as_str()]));
    }
    responses.push(command_complete(&format!("SELECT {}", rows.len())));
    responses
}

async fn pools_responses(pools: &GatewayPools) -> Vec<Bytes> {
    let stats = pools.snapshot().await;
    let row_count = stats.len();
    let columns = [
        "name",
        "host",
        "port",
        "min",
        "max",
        "idle",
        "in_use",
        "available",
    ];

    let mut responses = Vec::with_capacity(2 + stats.len());
    responses.push(row_description(&columns));
    for stat in stats {
        let port = stat.port.to_string();
        let min = stat.min.to_string();
        let max = stat.max.to_string();
        let idle = stat.idle.to_string();
        let in_use = stat.in_use.to_string();
        let available = stat.available.to_string();
        responses.push(data_row(&[
            stat.name.as_str(),
            stat.host.as_str(),
            &port,
            &min,
            &max,
            &idle,
            &in_use,
            &available,
        ]));
    }
    responses.push(command_complete(&format!("SELECT {}", row_count)));
    responses
}

fn session_responses(context: &FrontendContext) -> Vec<Bytes> {
    let stage = auth_stage_label(context.stage);
    let is_admin = context.is_admin.to_string();
    let gateway_session = if context.gateway_session.is_some() {
        "connected"
    } else {
        "none"
    };
    let pool = context.current_pool.as_deref().unwrap_or("none");
    let backend_pid = context.backend_identity.process_id.to_string();
    let backend_key = context.backend_identity.secret_key.to_string();

    let mut responses = Vec::with_capacity(2 + 6);
    responses.push(row_description(&["field", "value"]));
    responses.push(data_row(&["auth_stage", stage]));
    responses.push(data_row(&["is_admin", &is_admin]));
    responses.push(data_row(&["gateway_session", gateway_session]));
    responses.push(data_row(&["pool", pool]));
    responses.push(data_row(&["backend_identity_pid", &backend_pid]));
    responses.push(data_row(&["backend_identity_key", &backend_key]));
    responses.push(command_complete("SELECT 6"));
    responses
}

fn auth_stage_label(stage: AuthStage) -> &'static str {
    match stage {
        AuthStage::Startup => "startup",
        AuthStage::Authenticating => "authenticating",
        AuthStage::Ready => "ready",
    }
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
    use crate::config::shards::ShardRecord;
    use crate::frontend::context::FrontendContext;
    use crate::shared_types::{AuthStage, BackendIdentity};
    use bytes::Bytes;
    use secrecy::SecretString;

    #[test]
    fn parses_show_analytics_command() {
        let cmd = parse_admin_command("SHOW PGCRAB ANALYTICS;");
        assert_eq!(cmd, Some(AdminCommand::ShowAnalytics));
    }

    #[test]
    fn parses_show_pools_command() {
        let cmd = parse_admin_command("show pgcrab pools");
        assert_eq!(cmd, Some(AdminCommand::ShowPools));
    }

    #[test]
    fn parses_show_session_command() {
        let cmd = parse_admin_command("SHOW PGCRAB SESSION;");
        assert_eq!(cmd, Some(AdminCommand::ShowSession));
    }

    #[tokio::test]
    async fn builds_show_pools_response() {
        let pools = GatewayPools::new(vec![ShardRecord {
            shard_name: "alpha".to_string(),
            host: "127.0.0.1".to_string(),
            port: 5432,
            user: "user".to_string(),
            password: SecretString::new("secret".to_string().into_boxed_str()),
            min_connections: 1,
            max_connections: 2,
        }]);
        let context = FrontendContext::new();
        let responses = command_responses(AdminCommand::ShowPools, &context, &pools).await;

        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0][0], b'T');
        for column in [
            "name",
            "host",
            "port",
            "min",
            "max",
            "idle",
            "in_use",
            "available",
        ] {
            assert!(contains_bytes(&responses[0], column.as_bytes()));
        }
        assert_eq!(responses[1][0], b'D');
        assert!(contains_bytes(&responses[1], b"alpha"));
        assert!(contains_bytes(&responses[2], b"SELECT 1"));
    }

    #[tokio::test]
    async fn builds_show_session_response() {
        let pools = GatewayPools::new(Vec::new());
        let mut context = FrontendContext::new();
        context.stage = AuthStage::Ready;
        context.is_admin = true;
        context.current_pool = Some("alpha".to_string());
        context.backend_identity = BackendIdentity {
            process_id: 10,
            secret_key: 20,
        };

        let responses = command_responses(AdminCommand::ShowSession, &context, &pools).await;

        assert_eq!(responses.len(), 8);
        assert_eq!(responses[0][0], b'T');
        assert!(contains_bytes(&responses[1], b"auth_stage"));
        assert!(contains_bytes(&responses[1], b"ready"));
        assert!(contains_bytes(&responses[2], b"is_admin"));
        assert!(contains_bytes(&responses[2], b"true"));
        assert!(contains_bytes(&responses[3], b"gateway_session"));
        assert!(contains_bytes(&responses[3], b"none"));
        assert!(contains_bytes(&responses[4], b"pool"));
        assert!(contains_bytes(&responses[4], b"alpha"));
        assert!(contains_bytes(&responses[5], b"backend_identity_pid"));
        assert!(contains_bytes(&responses[5], b"10"));
        assert!(contains_bytes(&responses[6], b"backend_identity_key"));
        assert!(contains_bytes(&responses[6], b"20"));
        assert!(contains_bytes(&responses[7], b"SELECT 6"));
    }

    fn contains_bytes(haystack: &Bytes, needle: &[u8]) -> bool {
        haystack
            .windows(needle.len())
            .any(|window| window == needle)
    }
}
