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
    let ast = pg_query::parse(query).map_err(|err| ParseError::new(err.to_string()))?;

    Ok(ParsedQuery {
        statement_type: StatementType::Other,
        tables: Vec::new(),
        ast,
    })
}
