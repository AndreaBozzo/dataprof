//! SQL validation utilities to prevent injection attacks
//!
//! Keywords are matched as whole words outside string literals (#1167). A
//! substring match refused `created_at` for CREATE, `updated_at` for UPDATE and
//! `is_deleted` for DELETE, which are in most application tables.

use crate::DataProfilerError;
use std::collections::HashSet;

/// Words that name a statement which changes data or runs code, or a sleep
/// used to probe a server. A query or identifier holding one is refused.
const STATEMENT_KEYWORDS: [&str; 17] = [
    "DROP",
    "DELETE",
    "INSERT",
    "UPDATE",
    "TRUNCATE",
    "ALTER",
    "CREATE",
    "GRANT",
    "REVOKE",
    "EXEC",
    "EXECUTE",
    "WAITFOR",
    "SLEEP",
    "EXTRACTVALUE",
    "LOAD_FILE",
    "COPY",
    "ATTACH",
];

/// Words that reach a system catalog or a shell. `PG_` is a prefix: every
/// PostgreSQL catalog and admin function starts with it.
const SYSTEM_WORDS: [&str; 6] = [
    "INFORMATION_SCHEMA",
    "SYS",
    "MASTER",
    "MYSQL",
    "PROGRAM",
    "XP_CMDSHELL",
];

/// The first refused word in `words`, uppercased, if any.
fn refused_word<'a>(words: impl IntoIterator<Item = &'a str>, allow_union: bool) -> Option<String> {
    words.into_iter().find_map(|word| {
        let upper = word.to_uppercase();
        let refused = STATEMENT_KEYWORDS.contains(&upper.as_str())
            || SYSTEM_WORDS.contains(&upper.as_str())
            || upper.starts_with("PG_")
            || (!allow_union && upper == "UNION");
        refused.then_some(upper)
    })
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// The words of `query` outside string literals, with the contents of quoted
/// identifiers split into words too, so a quoted catalog name is still seen.
///
/// A string literal is `'...'` with `''` as an escaped quote, the one form
/// PostgreSQL, MySQL and SQLite all read alike. A quoted region holding a
/// backslash is scanned as words instead: MySQL reads `\'` as an escaped
/// quote and PostgreSQL does not, so where it ends depends on the database.
/// A comment outside a string literal is an error: it could hide the rest of
/// the statement from this check.
fn query_words(query: &str) -> Result<Vec<&str>, DataProfilerError> {
    let mut words = Vec::new();
    let mut index = 0;
    while index < query.len() {
        let rest = &query[index..];
        if rest.starts_with("--") || rest.starts_with("/*") || rest.starts_with('#') {
            return Err(DataProfilerError::sql_validation(
                "Query contains a comment, which is not allowed",
            ));
        }
        let c = rest.chars().next().expect("index is inside the query");
        match c {
            '\'' | '"' | '`' => {
                let Some(len) = quoted_len(&rest[1..], c) else {
                    return Err(DataProfilerError::sql_validation(
                        "Query has an unterminated quoted string or identifier",
                    ));
                };
                let inner = &rest[1..1 + len];
                if inner.contains('\\') {
                    // Scan past the opening quote as if it were not there.
                    index += 1;
                    continue;
                }
                if c != '\'' {
                    words.extend(
                        inner
                            .split(|c: char| !is_word_char(c))
                            .filter(|w| !w.is_empty()),
                    );
                }
                index += len + 2;
            }
            c if is_word_char(c) => {
                let len = rest.find(|c: char| !is_word_char(c)).unwrap_or(rest.len());
                words.push(&rest[..len]);
                index += len;
            }
            c => index += c.len_utf8(),
        }
    }
    Ok(words)
}

/// The length of the quoted body at the start of `text`, up to the closing
/// `quote`; a doubled quote is part of the body.
fn quoted_len(text: &str, quote: char) -> Option<usize> {
    let mut chars = text.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        if c == quote {
            if chars.peek().is_some_and(|&(_, next)| next == quote) {
                chars.next();
            } else {
                return Some(at);
            }
        }
    }
    None
}

/// Validate SQL identifiers (table names, column names) to prevent injection
pub fn validate_sql_identifier(identifier: &str) -> Result<(), DataProfilerError> {
    if identifier.trim().is_empty() {
        return Err(DataProfilerError::sql_validation(
            "SQL identifier cannot be empty",
        ));
    }

    if identifier.len() > 128 {
        return Err(DataProfilerError::sql_validation(
            "SQL identifier too long (max 128 chars)",
        ));
    }

    let allowed_chars: HashSet<char> =
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_."
            .chars()
            .collect();

    if (identifier.starts_with('"') && identifier.ends_with('"'))
        || (identifier.starts_with('`') && identifier.ends_with('`'))
        || (identifier.starts_with('[') && identifier.ends_with(']'))
    {
        let inner = &identifier[1..identifier.len() - 1];
        if inner.is_empty() {
            return Err(DataProfilerError::sql_validation(
                "Quoted identifier cannot be empty",
            ));
        }
        let quote_char = identifier
            .chars()
            .next()
            .ok_or_else(|| DataProfilerError::sql_validation("Invalid identifier format"))?;
        if inner.contains(quote_char)
            || inner.contains(';')
            || inner.contains("--")
            || inner.contains("/*")
            || inner.contains("*/")
        {
            return Err(DataProfilerError::sql_validation(
                "Invalid characters in quoted identifier",
            ));
        }
    } else {
        if !identifier.chars().all(|c| allowed_chars.contains(&c)) {
            return Err(DataProfilerError::sql_validation(&format!(
                "Invalid SQL identifier '{}': only alphanumeric, underscore, and dot allowed",
                identifier
            )));
        }

        if let Some(first_char) = identifier.chars().next()
            && !first_char.is_alphabetic()
            && first_char != '_'
        {
            return Err(DataProfilerError::sql_validation(
                "SQL identifier must start with letter or underscore",
            ));
        }
    }

    let words = identifier
        .split(|c: char| !is_word_char(c))
        .filter(|word| !word.is_empty());
    if let Some(word) = refused_word(words, false) {
        return Err(DataProfilerError::sql_validation(&format!(
            "SQL identifier contains dangerous keyword or pattern: {}",
            word
        )));
    }

    Ok(())
}

/// Validate and sanitize a basic SQL query to ensure it's a SELECT statement
///
/// `UNION` is allowed: it reads only what a plain `SELECT`, which this allows,
/// can read.
pub fn validate_base_query(query: &str) -> Result<String, DataProfilerError> {
    let trimmed = query.trim();

    if trimmed.is_empty() {
        return Err(DataProfilerError::sql_validation("Query cannot be empty"));
    }

    if trimmed.len() > 10000 {
        return Err(DataProfilerError::sql_validation(
            "Query too long (max 10000 chars)",
        ));
    }

    let words = query_words(trimmed)?;
    if !words
        .first()
        .is_some_and(|word| word.eq_ignore_ascii_case("SELECT"))
    {
        return Err(DataProfilerError::sql_validation(
            "Only SELECT queries are allowed for sampling",
        ));
    }

    if let Some(word) = refused_word(words, true) {
        return Err(DataProfilerError::sql_validation(&format!(
            "Query contains dangerous SQL pattern: {}",
            word
        )));
    }

    Ok(trimmed.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_identifier_validation() {
        assert!(validate_sql_identifier("users").is_ok());
        assert!(validate_sql_identifier("user_table").is_ok());
        assert!(validate_sql_identifier("schema.table").is_ok());
        assert!(validate_sql_identifier("\"quoted table\"").is_ok());
        assert!(validate_sql_identifier("`quoted_table`").is_ok());

        assert!(validate_sql_identifier("").is_err());
        assert!(validate_sql_identifier("DROP TABLE").is_err());
        assert!(validate_sql_identifier("users; DROP TABLE users; --").is_err());
        assert!(validate_sql_identifier("table/* comment */").is_err());
        assert!(validate_sql_identifier("123invalid").is_err());
    }

    #[test]
    fn test_base_query_validation() {
        assert!(validate_base_query("SELECT * FROM users").is_ok());
        assert!(validate_base_query("  SELECT id, name FROM products  ").is_ok());

        assert!(validate_base_query("").is_err());
        assert!(validate_base_query("DROP TABLE users").is_err());
        assert!(validate_base_query("SELECT * FROM users; DROP TABLE users").is_err());
    }

    /// Words that only contain a keyword are column and table names (#1167).
    #[test]
    fn keywords_inside_words_and_strings_are_allowed() {
        for query in [
            "SELECT id, created_at, updated_at FROM orders",
            "SELECT is_deleted, system_id, mastery FROM accounts",
            "SELECT * FROM user_updates WHERE note = 'DROP TABLE x; --'",
            "SELECT 'it''s', 'pg_sleep(1) # -- /*' FROM t",
            "SELECT a FROM t UNION ALL SELECT a FROM u",
            "select copy_count from \"Sales\"",
        ] {
            assert!(validate_base_query(query).is_ok(), "{query}");
        }
        for identifier in ["user_updates", "public.created_items", "is_deleted"] {
            assert!(validate_sql_identifier(identifier).is_ok(), "{identifier}");
        }
    }

    #[test]
    fn keywords_as_words_catalogs_and_comments_are_refused() {
        for query in [
            "SELECT 1; DELETE FROM t",
            "SELECT * FROM information_schema.tables",
            "SELECT pg_sleep(10)",
            "SELECT * FROM \"pg_catalog\".\"pg_authid\"",
            "SELECT * FROM mysql.user",
            "SELECT sleep(5)",
            "SELECT 1 -- hidden",
            "SELECT 1 /* hidden */",
            "SELECT 1 # hidden",
            "SELECT 'unterminated",
            // MySQL ends the first string at its second quote, so sleep(10)
            // is code there; a literal holding a backslash is scanned as words.
            r#"SELECT 'a\'', sleep(10), ''''"#,
            r#"SELECT "a\" 'b", sleep(10), ''"#,
            "SELECT $$ DELETE $$",
            // Refused until profiles run in a read-only transaction (#1169).
            "WITH x AS (SELECT 1) SELECT * FROM x",
            "selectx FROM t",
        ] {
            assert!(validate_base_query(query).is_err(), "{query}");
        }
        for identifier in [
            "pg_catalog.pg_authid",
            "information_schema.tables",
            "union",
            "copy",
        ] {
            assert!(validate_sql_identifier(identifier).is_err(), "{identifier}");
        }
    }
}
