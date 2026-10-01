use crate::{CataError, Result};
use datafusion::sql::sqlparser::ast::Statement;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion::sql::sqlparser::tokenizer::{Token, Tokenizer};
use lyra_meta::metadata::normalize_sql_identifier;
use std::collections::HashSet;
use std::time::Duration;

#[derive(Clone, Debug)]
pub(crate) enum Command {
    Create {
        name: String,
        if_not_exists: bool,
        options: Options,
    },
    Alter {
        name: String,
        action: Alter,
    },
    Drop {
        name: String,
        if_exists: bool,
        force: bool,
        timeout: Duration,
    },
    Show {
        pattern: Option<String>,
    },
    Begin,
    Commit,
    Rollback,
    Query(String),
}

#[derive(Clone, Debug, Default)]
pub(crate) struct Options {
    pub owner: Option<String>,
    pub allow_connections: Option<bool>,
    pub connection_limit: Option<i32>,
}

#[derive(Clone, Debug)]
pub(crate) enum Alter {
    Owner(String),
    Options(Options),
    ResetAll,
}

pub(crate) struct Parsed {
    pub command: Command,
    pub truncated: bool,
}

struct Tokens {
    tokens: Vec<Token>,
    offset: usize,
    truncated: bool,
}

impl Tokens {
    fn peek(&self) -> &Token {
        self.tokens.get(self.offset).unwrap_or(&Token::EOF)
    }
    fn next(&mut self) -> Token {
        let token = self.peek().clone();
        self.offset += 1;
        token
    }
    fn consume(&mut self, token: Token) -> bool {
        if self.peek() == &token {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn word(&mut self, word: &str) -> bool {
        if matches!(self.peek(), Token::Word(w) if w.quote_style.is_none() && w.value.eq_ignore_ascii_case(word))
        {
            self.offset += 1;
            true
        } else {
            false
        }
    }
    fn expect(&mut self, word: &str) -> Result<()> {
        if self.word(word) {
            Ok(())
        } else {
            Err(syntax())
        }
    }
    fn name(&mut self) -> Result<String> {
        let Token::Word(word) = self.next() else {
            return Err(syntax());
        };
        let (name, truncated) = normalize_sql_identifier(&word.value, word.quote_style.is_some())?;
        self.truncated |= truncated;
        Ok(name)
    }
    fn literal(&mut self) -> Result<String> {
        match self.next() {
            Token::SingleQuotedString(value) | Token::Number(value, _) => Ok(value),
            Token::Word(word) if word.quote_style.is_none() => Ok(word.value),
            _ => Err(syntax()),
        }
    }
    fn options(&mut self, create: bool) -> Result<Options> {
        let mut options = Options::default();
        let mut seen = HashSet::new();
        while self.peek() != &Token::EOF {
            let Token::Word(word) = self.next() else {
                return Err(syntax());
            };
            if word.quote_style.is_some() {
                return Err(syntax());
            }
            let key = word.value.to_ascii_uppercase();
            if !seen.insert(key.clone()) {
                return Err(syntax());
            }
            if key == "CONNECTION" {
                self.expect("LIMIT")?;
            }
            self.consume(Token::Eq);
            match key.as_str() {
                "OWNER" if create => {
                    options.owner = if self.word("DEFAULT")
                        || self.word("CURRENT_USER")
                        || self.word("SESSION_USER")
                    {
                        None
                    } else {
                        Some(self.name()?)
                    };
                }
                "ALLOW_CONNECTIONS" => {
                    options.allow_connections =
                        Some(match self.literal()?.to_ascii_lowercase().as_str() {
                            "true" | "on" | "1" => true,
                            "false" | "off" | "0" => false,
                            _ => return Err(invalid()),
                        });
                }
                "CONNECTION" => {
                    let negative = self.consume(Token::Minus);
                    let value = self.literal()?.parse::<i32>().map_err(|_| invalid())?;
                    let value = if negative { -value } else { value };
                    if value < -1 {
                        return Err(invalid());
                    }
                    options.connection_limit = Some(value);
                }
                "ENCODING" if create => {
                    if !matches!(
                        self.literal()?.to_ascii_uppercase().as_str(),
                        "UTF8" | "UTF-8" | "6" | "DEFAULT"
                    ) {
                        return Err(unsupported());
                    }
                }
                "LC_COLLATE" | "LC_CTYPE" | "LOCALE" if create => {
                    if !matches!(self.literal()?.as_str(), "C" | "POSIX") {
                        return Err(unsupported());
                    }
                }
                _ => return Err(unsupported()),
            }
        }
        Ok(options)
    }
    fn command(&mut self) -> Result<Command> {
        if self.word("CREATE") {
            self.expect("DATABASE").map_err(|_| unsupported())?;
            let if_not_exists = self.word("IF");
            if if_not_exists {
                self.expect("NOT")?;
                self.expect("EXISTS")?;
            }
            let name = self.name()?;
            self.word("WITH");
            return Ok(Command::Create {
                name,
                if_not_exists,
                options: self.options(true)?,
            });
        }
        if self.word("ALTER") {
            self.expect("DATABASE").map_err(|_| unsupported())?;
            let name = self.name()?;
            let action = if self.word("RENAME") {
                self.expect("TO")?;
                self.name()?;
                return Err(unsupported());
            } else if self.word("OWNER") {
                self.expect("TO")?;
                Alter::Owner(self.name()?)
            } else if self.word("RESET") {
                if !self.word("ALL") {
                    return Err(unsupported());
                }
                Alter::ResetAll
            } else if self.word("SET") {
                return Err(unsupported());
            } else {
                self.word("WITH");
                let options = self.options(false)?;
                if options.allow_connections.is_none() && options.connection_limit.is_none() {
                    return Err(syntax());
                }
                Alter::Options(options)
            };
            return Ok(Command::Alter { name, action });
        }
        if self.word("DROP") {
            self.expect("DATABASE").map_err(|_| unsupported())?;
            let if_exists = self.word("IF");
            if if_exists {
                self.expect("EXISTS")?;
            }
            let name = self.name()?;
            let mut force = false;
            let mut timeout = None;
            let with = self.word("WITH");
            if self.consume(Token::LParen) {
                let mut seen = HashSet::new();
                loop {
                    let Token::Word(option) = self.next() else {
                        return Err(syntax());
                    };
                    if option.quote_style.is_some() {
                        return Err(syntax());
                    }
                    let option = option.value.to_ascii_uppercase();
                    if !seen.insert(option.clone()) {
                        return Err(syntax());
                    }
                    match option.as_str() {
                        "FORCE" => force = true,
                        "TIMEOUT" => {
                            self.consume(Token::Eq);
                            let Token::SingleQuotedString(value) = self.next() else {
                                return Err(syntax());
                            };
                            timeout = Some(parse_timeout(&value)?);
                        }
                        _ => return Err(unsupported()),
                    }
                    if self.consume(Token::RParen) {
                        break;
                    }
                    if !self.consume(Token::Comma) {
                        return Err(syntax());
                    }
                }
            } else if with {
                return Err(syntax());
            }
            if timeout.is_some() && !force {
                return Err(invalid());
            }
            return Ok(Command::Drop {
                name,
                if_exists,
                force,
                timeout: timeout.unwrap_or(Duration::from_secs(30)),
            });
        }
        if self.word("SHOW") && self.word("DATABASES") {
            let pattern = if self.word("LIKE") {
                let Token::SingleQuotedString(value) = self.next() else {
                    return Err(syntax());
                };
                Some(value)
            } else {
                None
            };
            return Ok(Command::Show { pattern });
        }
        self.offset = 0;
        if self.word("BEGIN") || (self.word("START") && self.word("TRANSACTION")) {
            self.word("WORK");
            self.word("TRANSACTION");
            return Ok(Command::Begin);
        }
        self.offset = 0;
        if self.word("COMMIT") || self.word("END") {
            self.word("WORK");
            self.word("TRANSACTION");
            return Ok(Command::Commit);
        }
        if self.word("ROLLBACK") {
            self.word("WORK");
            self.word("TRANSACTION");
            return Ok(Command::Rollback);
        }
        let sql = self
            .tokens
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(" ");
        let statements = Parser::parse_sql(&PostgreSqlDialect {}, &sql).map_err(|_| syntax())?;
        if statements.len() != 1 || !matches!(statements[0], Statement::Query(_)) {
            return Err(unsupported());
        }
        self.offset = self.tokens.len();
        Ok(Command::Query(sql))
    }
}

pub(crate) fn parse(sql: &str) -> Result<Vec<Parsed>> {
    let tokens = Tokenizer::new(&PostgreSqlDialect {}, sql)
        .tokenize()
        .map_err(|_| syntax())?;
    let tokens = tokens
        .into_iter()
        .filter(|t| !matches!(t, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut parsed = Vec::new();
    for part in tokens
        .split(|t| t == &Token::SemiColon)
        .filter(|part| !part.is_empty())
    {
        let mut tokens = Tokens {
            tokens: part.to_vec(),
            offset: 0,
            truncated: false,
        };
        let command = tokens.command()?;
        if tokens.peek() != &Token::EOF {
            return Err(syntax());
        }
        parsed.push(Parsed {
            command,
            truncated: tokens.truncated,
        });
    }
    Ok(parsed)
}

fn parse_timeout(value: &str) -> Result<Duration> {
    let value = value.trim();
    let n = value.bytes().take_while(u8::is_ascii_digit).count();
    let number = value[..n].parse::<u64>().map_err(|_| invalid())?;
    let unit = value[n..].trim();
    let multiplier = match unit {
        "ms" => 1,
        "s" => 1000,
        "min" => 60_000,
        _ => return Err(invalid()),
    };
    let millis = number
        .checked_mul(multiplier)
        .filter(|n| *n > 0)
        .ok_or_else(invalid)?;
    // Tokio's deadline must be representable, even on platforms with a narrow Instant.
    let duration = Duration::from_millis(millis);
    if std::time::Instant::now().checked_add(duration).is_none() {
        return Err(invalid());
    }
    Ok(duration)
}
fn syntax() -> CataError {
    CataError::sql("42601", "invalid database command syntax")
}
fn invalid() -> CataError {
    CataError::sql("22023", "invalid database option value")
}
fn unsupported() -> CataError {
    CataError::sql("0A000", "feature is not supported in LIP-0001")
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn database_grammar_and_boundaries() {
        for sql in [
            "CREATE DATABASE Analytics WITH OWNER lyrasys ENCODING 'UTF8' CONNECTION LIMIT -1",
            "ALTER DATABASE analytics ALLOW_CONNECTIONS false CONNECTION LIMIT 0",
            "DROP DATABASE IF EXISTS x WITH (FORCE, TIMEOUT '30 s')",
            "ALTER DATABASE x RESET ALL",
            "SHOW DATABASES LIKE 'a%'",
            "SELECT ';'; SELECT 1",
        ] {
            assert!(parse(sql).is_ok(), "{sql}");
        }
        for sql in [
            "DROP DATABASE x WITH (TIMEOUT '1s')",
            "DROP DATABASE x (FORCE, FORCE)",
            "DROP DATABASE x (FORCE, TIMEOUT '0ms')",
            "DROP DATABASE x (FORCE, TIMEOUT '18446744073709551615min')",
            "CREATE DATABASE x OWNER a OWNER b",
            "CREATE DATABASE x TEMPLATE template0",
            "CREATE DATABASE x ENCODING 'LATIN1'",
            "ALTER DATABASE x SET timezone = 'UTC'",
            "CREATE USER x",
            "CREATE SCHEMA x",
            "CREATE TABLE x(a int)",
            "USE x",
            "CREATE DATABASE x.y",
            "BEGIN READ WRITE",
        ] {
            assert!(parse(sql).is_err(), "{sql}");
        }
        let parsed = parse(&format!("CREATE DATABASE \"{}\"", "é".repeat(40))).unwrap();
        assert!(parsed[0].truncated);
        let Command::Create { name, .. } = &parsed[0].command else {
            panic!()
        };
        assert_eq!(name.len(), 62);
    }
}
