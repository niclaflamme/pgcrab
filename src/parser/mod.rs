use std::fmt;
use std::num::NonZeroUsize;
use std::sync::{Arc, OnceLock};

use lru::LruCache;
use parking_lot::RwLock;
use pg_query::ParseResult;
use tracing::{debug, warn};

use crate::analytics;

const DEFAULT_CACHE_CAPACITY: usize = 1024;
static CACHE_CAPACITY: OnceLock<NonZeroUsize> = OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

#[derive(Debug, Clone)]
pub struct ParsedQuery {
    pub statement_type: StatementType,
    pub tables: Vec<String>,
    #[allow(dead_code)]
    pub(crate) ast: Arc<ParseResult>,
}

#[derive(Debug)]
pub struct ParseError {
    message: String,
}

impl ParseError {
    fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ParseError {}

pub fn parse(query: &str) -> Result<ParsedQuery, ParseError> {
    let cache = parser_cache();
    let key = query.as_bytes();
    if let Some(cached) = cache.get(key) {
        analytics::inc_parse_cache_hit();
        debug!(cache = "hit", query_len = query.len(), "parser cache");
        return Ok((*cached).clone());
    }

    analytics::inc_parse_cache_miss();
    debug!(cache = "miss", query_len = query.len(), "parser cache");
    let ast = pg_query::parse(query)
        .map_err(|err| ParseError::new(err.to_string()))
        .map(first_statement_only)?;
    let statement_type = statement_type_for(&ast);
    let mut tables = ast.tables();
    tables.sort();

    let parsed = ParsedQuery {
        statement_type,
        tables,
        ast: Arc::new(ast),
    };

    let cached = cache.insert_if_missing(key.to_vec(), Arc::new(parsed));

    Ok((*cached).clone())
}

fn first_statement_only(ast: ParseResult) -> ParseResult {
    if ast.protobuf.stmts.len() <= 1 {
        return ast;
    }

    let version = ast.protobuf.version;
    let first_stmt = ast
        .protobuf
        .stmts
        .into_iter()
        .next()
        .expect("non-empty statement list after length check");

    let protobuf = pg_query::protobuf::ParseResult {
        version,
        stmts: vec![first_stmt],
    };

    ParseResult::new(protobuf, String::new())
}

fn statement_type_for(ast: &ParseResult) -> StatementType {
    match ast.statement_types().first().copied() {
        Some("SelectStmt") => StatementType::Select,
        Some("InsertStmt") => StatementType::Insert,
        Some("UpdateStmt") => StatementType::Update,
        Some("DeleteStmt") => StatementType::Delete,
        _ => StatementType::Other,
    }
}

#[derive(Debug)]
struct ParserCache {
    entries: RwLock<LruCache<Vec<u8>, Arc<ParsedQuery>>>,
}

impl ParserCache {
    fn new(capacity: NonZeroUsize) -> Self {
        Self {
            entries: RwLock::new(LruCache::new(capacity)),
        }
    }

    fn len(&self) -> usize {
        self.entries.read().len()
    }

    fn capacity(&self) -> usize {
        self.entries.read().cap().get()
    }

    fn get(&self, key: &[u8]) -> Option<Arc<ParsedQuery>> {
        let mut cache = self.entries.write();
        cache.get(key).cloned()
    }

    fn insert_if_missing(&self, key: Vec<u8>, value: Arc<ParsedQuery>) -> Arc<ParsedQuery> {
        let mut cache = self.entries.write();
        if let Some(existing) = cache.get(&key) {
            return existing.clone();
        }

        let was_full = cache.len() == cache.cap().get();
        cache.put(key, value.clone());

        if was_full {
            analytics::inc_parse_cache_eviction();
        }
        value
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub len: usize,
    pub capacity: usize,
}

pub fn cache_stats() -> CacheStats {
    let cache = parser_cache();
    CacheStats {
        len: cache.len(),
        capacity: cache.capacity(),
    }
}

pub fn init_cache(capacity: usize) {
    let requested = NonZeroUsize::new(capacity)
        .unwrap_or_else(|| NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).expect("default capacity"));

    if let Some(existing) = CACHE_CAPACITY.get() {
        if existing.get() != requested.get() {
            warn!(
                previous = existing.get(),
                requested = requested.get(),
                "parser cache capacity already set; keeping existing"
            );
        }
        return;
    }

    let _ = CACHE_CAPACITY.set(requested);
}

fn cache_capacity() -> NonZeroUsize {
    *CACHE_CAPACITY
        .get_or_init(|| NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).expect("default capacity"))
}

fn parser_cache() -> &'static ParserCache {
    static CACHE: OnceLock<ParserCache> = OnceLock::new();
    CACHE.get_or_init(|| ParserCache::new(cache_capacity()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn parse_select() {
        let parsed = parse("SELECT * FROM users").expect("parse select");
        assert_eq!(parsed.statement_type, StatementType::Select);
        assert_eq!(parsed.tables, vec!["users"]);
    }

    #[test]
    fn parse_insert() {
        let parsed =
            parse("INSERT INTO users (id, name) VALUES (1, 'alice')").expect("parse insert");
        assert_eq!(parsed.statement_type, StatementType::Insert);
        assert_eq!(parsed.tables, vec!["users"]);
    }

    #[test]
    fn parse_update() {
        let parsed = parse("UPDATE users SET name = 'bob' WHERE id = 1").expect("parse update");
        assert_eq!(parsed.statement_type, StatementType::Update);
        assert_eq!(parsed.tables, vec!["users"]);
    }

    #[test]
    fn parse_delete() {
        let parsed = parse("DELETE FROM users WHERE id = 1").expect("parse delete");
        assert_eq!(parsed.statement_type, StatementType::Delete);
        assert_eq!(parsed.tables, vec!["users"]);
    }

    #[test]
    fn parse_only_first_statement() {
        let parsed = parse("SELECT * FROM first; UPDATE second SET id = 1").expect("parse multi");
        assert_eq!(parsed.statement_type, StatementType::Select);
        assert_eq!(parsed.tables, vec!["first"]);
    }

    #[test]
    fn cache_hits_reuse_ast() {
        let parsed_one = parse("SELECT * FROM cache_hit").expect("parse cache hit 1");
        let parsed_two = parse("SELECT * FROM cache_hit").expect("parse cache hit 2");
        assert!(Arc::ptr_eq(&parsed_one.ast, &parsed_two.ast));
    }

    #[test]
    fn cache_is_byte_exact() {
        let parsed_one = parse("SELECT * FROM cache_exact").expect("parse cache exact 1");
        let parsed_two = parse("SELECT  * FROM cache_exact").expect("parse cache exact 2");
        assert!(!Arc::ptr_eq(&parsed_one.ast, &parsed_two.ast));
    }

    #[test]
    fn cache_evicts_least_recently_used() {
        analytics::reset_parse_cache_counts();
        let cache = ParserCache::new(NonZeroUsize::new(2).unwrap());

        let first = Arc::new(ParsedQuery {
            statement_type: StatementType::Select,
            tables: vec!["a".to_string()],
            ast: Arc::new(pg_query::parse("SELECT 1").unwrap()),
        });

        let second = Arc::new(ParsedQuery {
            statement_type: StatementType::Select,
            tables: vec!["b".to_string()],
            ast: Arc::new(pg_query::parse("SELECT 2").unwrap()),
        });

        let third = Arc::new(ParsedQuery {
            statement_type: StatementType::Select,
            tables: vec!["c".to_string()],
            ast: Arc::new(pg_query::parse("SELECT 3").unwrap()),
        });

        cache.insert_if_missing(b"one".to_vec(), first.clone());
        cache.insert_if_missing(b"two".to_vec(), second.clone());
        assert_eq!(cache.len(), 2);

        cache.get(b"one");
        cache.insert_if_missing(b"three".to_vec(), third.clone());

        assert_eq!(cache.len(), 2);
        assert!(cache.get(b"one").is_some());
        assert!(cache.get(b"two").is_none());
        assert!(cache.get(b"three").is_some());

        let stats = analytics::snapshot();
        assert_eq!(stats.evictions, 1);
    }
}
