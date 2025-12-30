use std::fmt;

use pg_query::ParseResult;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    Select,
    Insert,
    Update,
    Delete,
    Other,
}

#[derive(Debug)]
pub struct ParsedQuery {
    pub statement_type: StatementType,
    pub tables: Vec<String>,
    #[allow(dead_code)]
    pub(crate) ast: ParseResult,
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
    let ast = pg_query::parse(query)
        .map_err(|err| ParseError::new(err.to_string()))
        .map(first_statement_only)?;
    let statement_type = statement_type_for(&ast);
    let mut tables = ast.tables();
    tables.sort();

    Ok(ParsedQuery {
        statement_type,
        tables,
        ast,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
