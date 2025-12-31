use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ParseCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
}

static PARSE_CACHE_HIT: AtomicU64 = AtomicU64::new(0);
static PARSE_CACHE_MISS: AtomicU64 = AtomicU64::new(0);
static PARSE_CACHE_EVICTION: AtomicU64 = AtomicU64::new(0);

pub fn inc_parse_cache_hit() {
    PARSE_CACHE_HIT.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_parse_cache_miss() {
    PARSE_CACHE_MISS.fetch_add(1, Ordering::Relaxed);
}

pub fn inc_parse_cache_eviction() {
    PARSE_CACHE_EVICTION.fetch_add(1, Ordering::Relaxed);
}

pub fn snapshot() -> ParseCacheStats {
    ParseCacheStats {
        hits: PARSE_CACHE_HIT.load(Ordering::Relaxed),
        misses: PARSE_CACHE_MISS.load(Ordering::Relaxed),
        evictions: PARSE_CACHE_EVICTION.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
pub(crate) fn reset_parse_cache_counts() {
    PARSE_CACHE_HIT.store(0, Ordering::Relaxed);
    PARSE_CACHE_MISS.store(0, Ordering::Relaxed);
    PARSE_CACHE_EVICTION.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_hits_and_misses() {
        reset_parse_cache_counts();
        inc_parse_cache_hit();
        inc_parse_cache_miss();
        inc_parse_cache_miss();
        inc_parse_cache_eviction();
        let stats = snapshot();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 2);
        assert_eq!(stats.evictions, 1);
    }
}
