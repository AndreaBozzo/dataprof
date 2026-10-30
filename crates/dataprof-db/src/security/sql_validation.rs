//! SQL validation utilities to prevent injection attacks
//!
//! Keywords are matched as whole words outside string literals (#1167). A
//! substring match refused `created_at` for CREATE, `updated_at` for UPDATE and
//! `is_deleted` for DELETE, which are in most application tables.
//!
//! This is the second line of defence. A profiled query runs in a transaction
//! the database itself keeps from writing (#1169), so the words left here are
//! the ones that matter inside a read: statements, in case a driver runs more
//! than one, the system catalogs, writes to files, and calls that stall or read
//! the server.

use crate::DataProfilerError;
use std::collections::HashSet;

/// Words that name a statement which changes data or runs code, or a clause
/// that writes a file (MySQL's `INTO OUTFILE`). A query or identifier holding
/// one is refused.
const STATEMENT_KEYWORDS: [&str; 15] = [
    "DROP", "DELETE", "INSERT", "UPDATE", "TRUNCATE", "ALTER", "CREATE", "GRANT", "REVOKE", "EXEC",
    "EXECUTE", "WAITFOR", "ATTACH", "OUTFILE", "DUMPFILE",
];

/// Words that reach a system catalog or a shell. `PG_` is a prefix: every
/// PostgreSQL catalog and admin function starts with it.
const SYSTEM_WORDS: [&str; 4] = ["INFORMATION_SCHEMA", "SYS", "MYSQL", "XP_CMDSHELL"];

/// Functions that stall the server or read its files. They are refused when
/// called, so a column named `sleep` is not.
const CALLED_FUNCTIONS: [&str; 5] = [
    "SLEEP",
    "BENCHMARK",
    "LOAD_FILE",
    "EXTRACTVALUE",
    "LOAD_EXTENSION",
];

/// The first refused word in `words`, uppercased, if any. Each word is a slice
/// of `text`, which is read past it to tell a function call.
fn refused_word<'a>(
    text: &str,
    words: impl IntoIterator<Item = &'a str>,
    allow_union: bool,
) -> Option<String> {
    words.into_iter().find_map(|word| {
        let upper = word.to_uppercase();
        let refused = STATEMENT_KEYWORDS.contains(&upper.as_str())
            || SYSTEM_WORDS.contains(&upper.as_str())
            || upper.starts_with("PG_")
            || (!allow_union && upper == "UNION")
            || (CALLED_FUNCTIONS.contains(&upper.as_str()) && is_called(text, word));
        refused.then_some(upper)
    })
}

/// Whether `word`, a slice of `text`, is followed by an opening parenthesis,
/// past whitespace and any closing quote of a quoted name.
fn is_called(text: &str, word: &str) -> bool {
    let end = word.as_ptr() as usize - text.as_ptr() as usize + word.len();
    text[end..]
        .trim_start_matches(|c: char| c.is_whitespace() || matches!(c, '"' | '`' | ']'))
        .starts_with('(')
}

/// Whether `text` is a query rather than a table name: its first word is
/// `SELECT` or `WITH`. A table named `selected_items` is a name.
pub fn is_query(text: &str) -> bool {
    text.trim_start()
        .split(|c: char| !is_word_char(c))
        .next()
        .is_some_and(|first| {
            first.eq_ignore_ascii_case("SELECT") || first.eq_ignore_ascii_case("WITH")
        })
}

fn is_word_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_' || c == '$'
}

/// How one database reads quotes and comments, as far as this check needs.
struct Dialect {
    name: &'static str,
    /// A backslash escapes the next character inside `'...'`.
    backslash_escapes: bool,
    /// `E'...'` is a string in which a backslash escapes, while plain `'...'`
    /// follows `backslash_escapes`.
    escape_strings: bool,
    /// `$tag$...$tag$` is a string.
    dollar_quotes: bool,
    /// `"..."` is a string rather than a quoted identifier.
    double_quoted_strings: bool,
    /// `` `...` `` and `[...]` are quoted identifiers.
    backtick_and_bracket_identifiers: bool,
    /// `#` starts a comment.
    hash_comments: bool,
}

/// Every reading the check guards against. PostgreSQL is read twice: with
/// `standard_conforming_strings` off, a backslash escapes in `'...'` too.
const DIALECTS: [Dialect; 4] = [
    Dialect {
        name: "PostgreSQL",
        backslash_escapes: false,
        escape_strings: true,
        dollar_quotes: true,
        double_quoted_strings: false,
        backtick_and_bracket_identifiers: false,
        hash_comments: false,
    },
    Dialect {
        name: "PostgreSQL without standard_conforming_strings",
        backslash_escapes: true,
        escape_strings: true,
        dollar_quotes: true,
        double_quoted_strings: false,
        backtick_and_bracket_identifiers: false,
        hash_comments: false,
    },
    Dialect {
        name: "MySQL",
        backslash_escapes: true,
        escape_strings: false,
        dollar_quotes: false,
        double_quoted_strings: true,
        backtick_and_bracket_identifiers: true,
        hash_comments: true,
    },
    Dialect {
        name: "SQLite",
        backslash_escapes: false,
        escape_strings: false,
        dollar_quotes: false,
        double_quoted_strings: false,
        backtick_and_bracket_identifiers: true,
        hash_comments: false,
    },
];

/// How a query reads in one dialect.
enum Reading<'a> {
    /// The words outside strings, with the words of quoted identifiers.
    Words(Vec<&'a str>),
    /// A quote that never closes: this database refuses the query itself.
    Unterminated,
    /// A comment outside a string, which could hide the rest of the query.
    Comment,
}

/// The words of `query` as `dialect` reads it, outside string literals, with
/// the contents of quoted identifiers split into words too, so a quoted
/// catalog name is still seen.
///
/// The databases disagree on where a string ends: MySQL reads `\'` as an
/// escaped quote and PostgreSQL does not, and PostgreSQL reads `$$'$$` as a
/// string where the others see a quote. A query is therefore read once per
/// dialect, and each reading is right for its own database.
fn read_query<'a>(query: &'a str, dialect: &Dialect) -> Reading<'a> {
    let mut words: Vec<&'a str> = Vec::new();
    let mut index = 0;
    while index < query.len() {
        let rest = &query[index..];
        let previous = query[..index].chars().next_back();
        let after_word = previous.is_some_and(is_word_char);
        if rest.starts_with("--")
            || rest.starts_with("/*")
            || (dialect.hash_comments && rest.starts_with('#'))
        {
            return Reading::Comment;
        }
        let c = rest.chars().next().expect("index is inside the query");
        let (len, checked) = match c {
            '\'' => {
                // `E'...'`: the `E` was just read as a word ending at this quote.
                let escape_string = dialect.escape_strings
                    && after_word
                    && words
                        .last()
                        .is_some_and(|word| word.eq_ignore_ascii_case("e"));
                match quoted_len(&rest[1..], '\'', dialect.backslash_escapes || escape_string) {
                    Some(len) => (len + 2, None),
                    None => return Reading::Unterminated,
                }
            }
            '"' => match quoted_len(
                &rest[1..],
                '"',
                dialect.double_quoted_strings && dialect.backslash_escapes,
            ) {
                Some(len) if dialect.double_quoted_strings => (len + 2, None),
                Some(len) => (len + 2, Some(&rest[1..1 + len])),
                None => return Reading::Unterminated,
            },
            '`' | '[' if dialect.backtick_and_bracket_identifiers => {
                let close = if c == '[' { ']' } else { '`' };
                match quoted_len(&rest[1..], close, false) {
                    Some(len) => (len + 2, Some(&rest[1..1 + len])),
                    None => return Reading::Unterminated,
                }
            }
            '$' if dialect.dollar_quotes && !after_word && dollar_tag(rest).is_some() => {
                // decode-audit: impossible — the guard found the tag.
                let tag = dollar_tag(rest).expect("a dollar-quote tag");
                match rest[tag.len()..].find(tag) {
                    Some(end) => (tag.len() + end + tag.len(), None),
                    None => return Reading::Unterminated,
                }
            }
            c if is_word_char(c) => {
                let len = rest.find(|c: char| !is_word_char(c)).unwrap_or(rest.len());
                words.push(&rest[..len]);
                index += len;
                continue;
            }
            c => (c.len_utf8(), None),
        };
        if let Some(inner) = checked {
            words.extend(
                inner
                    .split(|c: char| !is_word_char(c))
                    .filter(|w| !w.is_empty()),
            );
        }
        index += len;
    }
    Reading::Words(words)
}

/// The length of the quoted body at the start of `text`, up to the closing
/// `quote`. A doubled quote is part of the body, and so is any character after
/// a backslash when `backslash_escapes`.
fn quoted_len(text: &str, quote: char, backslash_escapes: bool) -> Option<usize> {
    let mut chars = text.char_indices().peekable();
    while let Some((at, c)) = chars.next() {
        if backslash_escapes && c == '\\' {
            chars.next();
        } else if c == quote {
            if chars.peek().is_some_and(|&(_, next)| next == quote) {
                chars.next();
            } else {
                return Some(at);
            }
        }
    }
    None
}

/// The opening `$tag$` of a PostgreSQL dollar-quoted string at the start of
/// `text`: an empty tag, or one that starts with a letter or underscore.
fn dollar_tag(text: &str) -> Option<&str> {
    let body = text.strip_prefix('$')?;
    let len = body.find('$')?;
    let tag = &body[..len];
    let valid = tag
        .chars()
        .next()
        .is_none_or(|first| first.is_alphabetic() || first == '_')
        && tag.chars().all(|c| c.is_alphanumeric() || c == '_');
    valid.then(|| &text[..len + 2])
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
    if let Some(word) = refused_word(identifier, words, false) {
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

    // A dialect in which a quote never closes refuses the query itself, so it
    // runs nothing there; every other reading has to pass.
    let mut read = false;
    for dialect in &DIALECTS {
        let words = match read_query(trimmed, dialect) {
            Reading::Words(words) => words,
            Reading::Unterminated => continue,
            Reading::Comment => {
                return Err(DataProfilerError::sql_validation(
                    "Query contains a comment, which is not allowed",
                ));
            }
        };
        read = true;
        if !words.first().is_some_and(|word| {
            word.eq_ignore_ascii_case("SELECT") || word.eq_ignore_ascii_case("WITH")
        }) {
            return Err(DataProfilerError::sql_validation(
                "Only SELECT queries, which may start with a WITH clause, are allowed for sampling",
            ));
        }
        if let Some(word) = refused_word(trimmed, words, true) {
            return Err(DataProfilerError::sql_validation(&format!(
                "Query contains dangerous SQL pattern: {} (as {} reads it)",
                word, dialect.name
            )));
        }
    }
    if !read {
        return Err(DataProfilerError::sql_validation(
            "Query has an unterminated quoted string or identifier",
        ));
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
            // A backslash in a string: a Windows path and a LIKE escape.
            r"SELECT 'C:\data' AS path",
            r"SELECT name FROM t WHERE name LIKE 'a\_b'",
            // PostgreSQL escape and dollar-quoted strings.
            r"SELECT E'it\'s DROP'",
            "SELECT $$it's$$",
            // A read-only CTE, and columns named like a function or a word
            // the database's read-only transaction covers (#1169).
            "WITH recent AS (SELECT 1 AS id) SELECT * FROM recent",
            "SELECT program, master, copy, sleep, benchmark FROM courses",
        ] {
            assert!(validate_base_query(query).is_ok(), "{query}");
        }
        for identifier in ["user_updates", "public.created_items", "is_deleted", "copy"] {
            assert!(validate_sql_identifier(identifier).is_ok(), "{identifier}");
        }
    }

    #[test]
    fn a_query_starts_with_select_or_with_as_a_word() {
        for text in [
            "SELECT 1",
            "  select * from t",
            "WITH x AS (SELECT 1) SELECT 1",
        ] {
            assert!(is_query(text), "{text}");
        }
        for text in ["selected_items", "withdrawals", "public.orders"] {
            assert!(!is_query(text), "{text}");
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
            // is code there, although PostgreSQL reads one longer string.
            r#"SELECT 'a\'', sleep(10), ''''"#,
            r#"SELECT "a\" 'b", sleep(10), ''"#,
            // PostgreSQL reads `$$'$$` as a string, so pg_sleep runs there.
            "SELECT $$'$$, pg_sleep(10), $$'$$",
            // MySQL and SQLite read `$$` as an identifier and DELETE as a
            // word; the check does not know which database runs the query.
            "SELECT $$ DELETE $$",
            "selectx FROM t",
            // A data-modifying CTE, and MySQL writing a file.
            "WITH gone AS (DELETE FROM t RETURNING *) SELECT * FROM gone",
            "SELECT * FROM t INTO OUTFILE '/tmp/t.csv'",
            "SELECT benchmark(100000000, md5('a'))",
            "SELECT `sleep` (5)",
        ] {
            assert!(validate_base_query(query).is_err(), "{query}");
        }
        for identifier in ["pg_catalog.pg_authid", "information_schema.tables", "union"] {
            assert!(validate_sql_identifier(identifier).is_err(), "{identifier}");
        }
    }
}
