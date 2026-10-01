use crate::{CataError, Result};
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Show { pattern: Option<String> },
    Begin,
    Commit,
    Rollback,
    Query(String),
}
pub(crate) struct Parsed {
    pub command: Command,
    pub truncated: bool,
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Parsed>> {
    if sql.contains('\0') {
        return Err(CataError::sql("22021", "invalid SQL text"));
    }
    let tokens: Vec<_> = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|_| CataError::sql("42601", "invalid SQL"))?
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect();
    let mut commands = Vec::new();
    for statement in tokens
        .split(|token| *token == Token::SemiColon)
        .filter(|s| !s.is_empty())
    {
        let word = |i: usize, expected: &str| {
            matches!(statement.get(i),
            Some(Token::Word(word)) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case(expected))
        };
        let command = if word(0, "SHOW") && word(1, "DATABASES") {
            let pattern = if statement.len() == 2 {
                None
            } else if statement.len() == 4 && word(2, "LIKE") {
                match &statement[3] {
                    Token::SingleQuotedString(pattern) => Some(pattern.clone()),
                    _ => return Err(CataError::sql("42601", "invalid SHOW DATABASES")),
                }
            } else {
                return Err(CataError::sql("42601", "invalid SHOW DATABASES"));
            };
            Command::Show { pattern }
        } else if (statement.len() == 1 || (statement.len() == 2 && word(1, "WORK")))
            && (word(0, "BEGIN") || word(0, "COMMIT") || word(0, "ROLLBACK"))
        {
            if word(0, "BEGIN") {
                Command::Begin
            } else if word(0, "COMMIT") {
                Command::Commit
            } else {
                Command::Rollback
            }
        } else {
            let text = statement
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" ");
            let parsed = Parser::parse_sql(&PostgreSqlDialect {}, &text)
                .map_err(|_| CataError::sql("42601", "invalid SQL"))?;
            if !matches!(parsed.as_slice(), [Statement::Query(_)]) {
                return Err(CataError::sql(
                    "0A000",
                    "only read-only queries are supported by the foundation",
                ));
            }
            Command::Query(text)
        };
        commands.push(Parsed {
            command,
            truncated: false,
        });
    }
    Ok(commands)
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn foundation_never_admits_ddl() {
        for sql in [
            "CREATE DATABASE x",
            "ALTER DATABASE public OWNER TO x",
            "DROP DATABASE public",
            "CREATE TABLE x (id INT)",
            "CREATE USER x",
            "COPY x FROM 'file'",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
        assert_eq!(
            parse("SELECT 1; SHOW DATABASES; SELECT current_database();")
                .unwrap()
                .len(),
            3
        );
    }
}
