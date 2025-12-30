use crate::analytics::{self, ParseCacheStats};

pub fn parse_cache_stats() -> ParseCacheStats {
    analytics::snapshot()
}

pub fn format_parse_cache_stats(stats: ParseCacheStats) -> String {
    format!(
        "parse_cache_hits={}\nparse_cache_misses={}",
        stats.hits, stats.misses
    )
}
