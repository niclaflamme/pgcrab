use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, OnceLock, RwLock};

use pg_query::ParseResult;
use tracing::debug;

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
    let key_hash = hash_bytes(key);
    if let Some(cached) = cache.get(key_hash, key) {
        debug!(cache = "hit", query_len = query.len(), "parser cache");
        return Ok((*cached).clone());
    }

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

    let cached = cache.insert_if_missing(key_hash, key.to_vec(), Arc::new(parsed));

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
struct CacheEntry {
    key: Vec<u8>,
    value: Arc<ParsedQuery>,
}

type CacheKey = [u8; 16];
type CacheMap = HashMap<CacheKey, Vec<CacheEntry>>;

struct ParserCache {
    entries: RwLock<CacheMap>,
}

impl ParserCache {
    fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    fn get(&self, key_hash: CacheKey, key: &[u8]) -> Option<Arc<ParsedQuery>> {
        let guard = self.entries.read().expect("parser cache read lock poisoned");
        let bucket = guard.get(&key_hash)?;
        bucket
            .iter()
            .find(|entry| entry.key == key)
            .map(|entry| entry.value.clone())
    }

    fn insert_if_missing(
        &self,
        key_hash: CacheKey,
        key: Vec<u8>,
        value: Arc<ParsedQuery>,
    ) -> Arc<ParsedQuery> {
        let mut guard = self
            .entries
            .write()
            .expect("parser cache write lock poisoned");
        let bucket = guard.entry(key_hash).or_default();
        if let Some(existing) = bucket.iter().find(|entry| entry.key == key) {
            return existing.value.clone();
        }
        bucket.push(CacheEntry {
            key,
            value: value.clone(),
        });
        value
    }
}

fn parser_cache() -> &'static ParserCache {
    static CACHE: OnceLock<ParserCache> = OnceLock::new();
    CACHE.get_or_init(ParserCache::new)
}

fn hash_bytes(bytes: &[u8]) -> CacheKey {
    md5::compute(bytes).0
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
}
