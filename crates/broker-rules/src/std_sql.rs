//! Standard rule SQL dialect front end (RQ-02).
//!
//! Operators write rules in the documented rule dialect
//! (`SELECT <fields> FROM <sources> [WHERE <cond>]`, plus `FOREACH`); this
//! module parses that dialect and lowers it onto the streaming engine's AST
//! ([`rekuiper_sql`](https://docs.rs)) so there is one engine and one fast
//! path. Streaming extensions (window clauses) are accepted in the same
//! statement.
//!
//! Lowering notes (each is a deliberate dialect decision):
//! - `FROM` takes quoted MQTT topic filters (`"sensors/+"`), event sources
//!   (`"$events/..."`, matched by nothing until event sources exist) and,
//!   as an extension, bare stream names which constrain nothing.
//! - `payload.a.b` reads the JSON payload object; `payload.arr[N]` is
//!   1-based, so it lowers to a 0-based index of `N - 1`.
//! - `a =~ 'filter'` (topic match) lowers to a `topic_match` call using the
//!   broker's own `TopicFilter::matches` so semantics exactly match MQTT
//!   topic filtering. A non-literal right-hand side is rejected at creation.
//! - `FOREACH <array> [DO <fields>] [INCASE <cond>]` expands one message
//!   into one row per array element with `item` bound to the element.

use rekuiper_sql::{BinaryOperator, Expr, SelectStmt, TimeUnit, UnaryOperator, WindowDef};
use std::fmt;

// ---------------------------------------------------------------------------
// Parse errors with line and column
// ---------------------------------------------------------------------------

/// A dialect parse failure pointing at the offending token (1-based line
/// and column, columns counted in characters).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StdParseError {
    /// 1-based line of the offending token.
    pub line: usize,
    /// 1-based column of the offending token, in characters.
    pub column: usize,
    /// What was wrong.
    pub message: String,
}

impl StdParseError {
    fn new(line: usize, column: usize, message: impl Into<String>) -> Self {
        Self {
            line,
            column,
            message: message.into(),
        }
    }
}

impl fmt::Display for StdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "line {}, column {}: {}",
            self.line, self.column, self.message
        )
    }
}

impl std::error::Error for StdParseError {}

// ---------------------------------------------------------------------------
// Lowered rule
// ---------------------------------------------------------------------------

/// One `FROM` source in the standard dialect.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StdSource {
    /// A quoted MQTT topic filter (`"sensors/+"`), raw text as written.
    Topic(String),
    /// An event source (`"$events/..."`); matched by nothing until event
    /// sources exist (fail closed).
    /// TODO(parity): what `$events/...` names exist and what fields their
    /// rows carry is decided with event sources, not here.
    Event(String),
    /// A bare stream name (streaming-dialect extension); constrains nothing.
    Stream(String),
}

/// The `FOREACH` expansion plan for one rule.
#[derive(Debug, Clone, PartialEq)]
pub struct ForeachPlan {
    /// The array expression, evaluated against the message record.
    pub array: Expr,
    /// `(expr, alias)` projection evaluated per element against the record
    /// plus `item`. `None` means no `DO`: each row is `{"item": element}`.
    pub fields: Option<Vec<(Expr, Option<String>)>>,
    /// Optional per-element `INCASE` guard, evaluated per element.
    pub incase: Option<Expr>,
}

/// A standard-dialect rule lowered onto the streaming AST.
#[derive(Debug, Clone, PartialEq)]
pub struct StdRule {
    /// Lowered statement (`from` is the inert `"stream"` placeholder; real
    /// source matching lives in [`StdRule::sources`]).
    pub stmt: SelectStmt,
    /// The `FROM` sources as written.
    pub sources: Vec<StdSource>,
    /// `FOREACH` expansion, if the statement is a `FOREACH` rule.
    pub foreach: Option<ForeachPlan>,
    /// True when the select list holds a top-level `*` (the engine strips
    /// injected message keys from its expansion so `SELECT *` keeps
    /// projecting exactly the payload fields).
    pub has_wildcard: bool,
    /// Output keys named explicitly by the select list (aliases, bare
    /// identifiers, dotted leaves); these survive the wildcard strip.
    pub explicit_keys: Vec<String>,
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
enum Tok {
    Word(String),
    Str(String),
    Num(serde_json::Number),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    LBracket,
    RBracket,
    Colon,
    Semi,
    Plus,
    Minus,
    Slash,
    Percent,
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Match,
}

#[derive(Debug, Clone)]
struct Spanned {
    tok: Tok,
    line: usize,
    column: usize,
}

struct Lexer {
    chars: Vec<char>,
    pos: usize,
    line: usize,
    col: usize,
    input_len: usize,
}

impl Lexer {
    fn new(raw: &str) -> Self {
        let chars: Vec<char> = raw.chars().collect();
        let input_len = chars.len();
        Self {
            chars,
            pos: 0,
            line: 1,
            col: 1,
            input_len,
        }
    }

    fn peek(&self) -> Option<char> {
        if self.pos < self.input_len {
            Some(self.chars[self.pos])
        } else {
            None
        }
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.peek()?;
        self.pos += 1;
        if c == '\n' {
            self.line += 1;
            self.col = 1;
        } else {
            self.col += 1;
        }
        Some(c)
    }

    fn err(&self, message: impl Into<String>) -> StdParseError {
        StdParseError::new(self.line, self.col, message)
    }

    fn lex_string(
        &mut self,
        quote: char,
        line: usize,
        col: usize,
    ) -> Result<Spanned, StdParseError> {
        let mut out = String::new();
        loop {
            match self.bump() {
                None => {
                    return Err(StdParseError::new(line, col, "unterminated string literal"));
                }
                Some(c) if c == quote => {
                    // Doubled quote inside the same quote kind is an escaped quote.
                    if self.peek() == Some(quote) {
                        self.bump();
                        out.push(quote);
                    } else {
                        break;
                    }
                }
                Some('\\') => match self.bump() {
                    None => {
                        return Err(StdParseError::new(line, col, "unterminated string literal"));
                    }
                    Some('n') => out.push('\n'),
                    Some('t') => out.push('\t'),
                    Some('r') => out.push('\r'),
                    Some(other) => out.push(other),
                },
                Some(c) => out.push(c),
            }
        }
        Ok(Spanned {
            tok: Tok::Str(out),
            line,
            column: col,
        })
    }

    fn lex_number(&mut self, line: usize, col: usize) -> Result<Spanned, StdParseError> {
        let start = self.pos;
        while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
            self.bump();
        }
        if self.peek() == Some('.') {
            let mut ahead = self.pos + 1;
            let mut frac_digits = 0;
            while ahead < self.input_len && self.chars[ahead].is_ascii_digit() {
                ahead += 1;
                frac_digits += 1;
            }
            // Only consume the dot when digits follow (`a.b` is field access).
            if frac_digits > 0 {
                self.bump();
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.bump();
                }
            }
        }
        if matches!(self.peek(), Some('e') | Some('E')) {
            let mut ahead = self.pos + 1;
            if ahead < self.input_len && (self.chars[ahead] == '+' || self.chars[ahead] == '-') {
                ahead += 1;
            }
            let exp_start = ahead;
            while ahead < self.input_len && self.chars[ahead].is_ascii_digit() {
                ahead += 1;
            }
            if ahead > exp_start {
                self.bump();
                if matches!(self.peek(), Some('+') | Some('-')) {
                    self.bump();
                }
                while matches!(self.peek(), Some(c) if c.is_ascii_digit()) {
                    self.bump();
                }
            }
        }
        let text: String = self.chars[start..self.pos].iter().collect();
        if let Ok(n) = text.parse::<i64>() {
            return Ok(Spanned {
                tok: Tok::Num(serde_json::Number::from(n)),
                line,
                column: col,
            });
        }
        if let Ok(n) = text.parse::<f64>() {
            if let Some(n) = serde_json::Number::from_f64(n) {
                return Ok(Spanned {
                    tok: Tok::Num(n),
                    line,
                    column: col,
                });
            }
        }
        Err(StdParseError::new(
            line,
            col,
            format!("invalid number literal '{text}'"),
        ))
    }

    fn tokenize(&mut self) -> Result<Vec<Spanned>, StdParseError> {
        let mut out = Vec::new();
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.bump();
                continue;
            }
            let line = self.line;
            let col = self.col;
            match c {
                '\'' | '"' | '`' => {
                    self.bump();
                    out.push(self.lex_string(c, line, col)?);
                }
                ',' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Comma,
                        line,
                        column: col,
                    });
                }
                '.' => {
                    // A dot followed by a digit starts a fractional number (`.5`).
                    let next = if self.pos + 1 < self.input_len {
                        Some(self.chars[self.pos + 1])
                    } else {
                        None
                    };
                    if matches!(next, Some(n) if n.is_ascii_digit()) {
                        out.push(self.lex_number(line, col)?);
                    } else {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Dot,
                            line,
                            column: col,
                        });
                    }
                }
                '(' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::LParen,
                        line,
                        column: col,
                    });
                }
                ')' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::RParen,
                        line,
                        column: col,
                    });
                }
                '[' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::LBracket,
                        line,
                        column: col,
                    });
                }
                ']' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::RBracket,
                        line,
                        column: col,
                    });
                }
                ':' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Colon,
                        line,
                        column: col,
                    });
                }
                ';' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Semi,
                        line,
                        column: col,
                    });
                }
                '+' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Plus,
                        line,
                        column: col,
                    });
                }
                '-' => {
                    // `->` is nested field access; otherwise minus.
                    if self.pos + 1 < self.input_len && self.chars[self.pos + 1] == '>' {
                        self.bump();
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Dot,
                            line,
                            column: col,
                        });
                    } else {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Minus,
                            line,
                            column: col,
                        });
                    }
                }
                '*' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Star,
                        line,
                        column: col,
                    });
                }
                '/' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Slash,
                        line,
                        column: col,
                    });
                }
                '%' => {
                    self.bump();
                    out.push(Spanned {
                        tok: Tok::Percent,
                        line,
                        column: col,
                    });
                }
                '=' => {
                    self.bump();
                    if self.peek() == Some('~') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Match,
                            line,
                            column: col,
                        });
                    } else if self.peek() == Some('=') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Eq,
                            line,
                            column: col,
                        });
                    } else {
                        out.push(Spanned {
                            tok: Tok::Eq,
                            line,
                            column: col,
                        });
                    }
                }
                '!' => {
                    self.bump();
                    if self.peek() == Some('=') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Neq,
                            line,
                            column: col,
                        });
                    } else {
                        return Err(StdParseError::new(line, col, "expected '=' after '!'"));
                    }
                }
                '<' => {
                    self.bump();
                    if self.peek() == Some('=') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Lte,
                            line,
                            column: col,
                        });
                    } else if self.peek() == Some('>') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Neq,
                            line,
                            column: col,
                        });
                    } else {
                        out.push(Spanned {
                            tok: Tok::Lt,
                            line,
                            column: col,
                        });
                    }
                }
                '>' => {
                    self.bump();
                    if self.peek() == Some('=') {
                        self.bump();
                        out.push(Spanned {
                            tok: Tok::Gte,
                            line,
                            column: col,
                        });
                    } else {
                        out.push(Spanned {
                            tok: Tok::Gt,
                            line,
                            column: col,
                        });
                    }
                }
                c if c.is_ascii_digit() => {
                    out.push(self.lex_number(line, col)?);
                }
                c if c.is_alphabetic() || c == '_' || c == '$' => {
                    let start = self.pos;
                    while matches!(self.peek(), Some(n) if n.is_alphanumeric() || n == '_' || n == '$')
                    {
                        self.bump();
                    }
                    let word: String = self.chars[start..self.pos].iter().collect();
                    out.push(Spanned {
                        tok: Tok::Word(word),
                        line,
                        column: col,
                    });
                }
                _ => {
                    return Err(self.err(format!("unexpected character '{c}'")));
                }
            }
        }
        Ok(out)
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser {
    toks: Vec<Spanned>,
    pos: usize,
    end_line: usize,
    end_col: usize,
}

impl Parser {
    fn peek(&self) -> Option<&Spanned> {
        self.toks.get(self.pos)
    }

    fn next(&mut self) -> Option<Spanned> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn err_at(&self, line: usize, column: usize, message: impl Into<String>) -> StdParseError {
        StdParseError::new(line, column, message)
    }

    fn err_here(&self, message: impl Into<String>) -> StdParseError {
        match self.peek() {
            Some(t) => StdParseError::new(t.line, t.column, message),
            None => StdParseError::new(self.end_line, self.end_col, message),
        }
    }

    fn peek_word_is(&self, kw: &str) -> bool {
        matches!(self.peek(), Some(t) if matches!(&t.tok, Tok::Word(w) if w.eq_ignore_ascii_case(kw)))
    }

    fn eat_word(&mut self, kw: &str) -> bool {
        if self.peek_word_is(kw) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    fn expect_word(&mut self, kw: &str) -> Result<Spanned, StdParseError> {
        if self.peek_word_is(kw) {
            Ok(self.next().expect("peeked word"))
        } else {
            Err(self.err_here(format!("expected keyword {kw}")))
        }
    }

    fn expect(&mut self, want: &Tok, what: &str) -> Result<Spanned, StdParseError> {
        let matches = matches!(
            (self.peek().map(|t| &t.tok), want),
            (Some(Tok::Comma), Tok::Comma)
                | (Some(Tok::Dot), Tok::Dot)
                | (Some(Tok::LParen), Tok::LParen)
                | (Some(Tok::RParen), Tok::RParen)
                | (Some(Tok::LBracket), Tok::LBracket)
                | (Some(Tok::RBracket), Tok::RBracket)
                | (Some(Tok::Colon), Tok::Colon)
                | (Some(Tok::Star), Tok::Star)
        );
        if matches {
            Ok(self.next().expect("peeked token"))
        } else {
            Err(self.err_here(format!("expected {what}")))
        }
    }
}

/// Clause keywords never count as a bare alias or a bare source name.
fn is_reserved(word: &str) -> bool {
    matches!(
        word.to_ascii_uppercase().as_str(),
        "SELECT"
            | "FROM"
            | "WHERE"
            | "FOREACH"
            | "DO"
            | "INCASE"
            | "AS"
            | "AND"
            | "OR"
            | "NOT"
            | "IN"
            | "BETWEEN"
            | "LIKE"
            | "IS"
            | "NULL"
            | "CASE"
            | "WHEN"
            | "THEN"
            | "ELSE"
            | "END"
            | "GROUP"
            | "BY"
            | "HAVING"
            | "ORDER"
            | "LIMIT"
            | "UNION"
            | "ALL"
            | "JOIN"
            | "INNER"
            | "LEFT"
            | "RIGHT"
            | "FULL"
            | "CROSS"
            | "OUTER"
            | "ON"
            | "OVER"
            | "PARTITION"
            | "TRUE"
            | "FALSE"
    )
}

/// Parse one standard-dialect rule statement.
pub fn parse_std_rule(sql: &str) -> Result<StdRule, StdParseError> {
    let mut lexer = Lexer::new(sql);
    let toks = lexer.tokenize()?;
    let (end_line, end_col) = match toks.last() {
        Some(t) => (t.line, t.column + 1),
        None => (1, 1),
    };
    if toks.is_empty() {
        return Err(StdParseError::new(1, 1, "empty rule SQL"));
    }
    let mut p = Parser {
        toks,
        pos: 0,
        end_line,
        end_col,
    };
    let rule = if p.peek_word_is("FOREACH") {
        parse_foreach(&mut p)?
    } else {
        parse_select_rule(&mut p, false)?
    };
    // A single trailing semicolon is tolerated; anything else is rejected
    // so a typo cannot silently change routing.
    if let Some(t) = p.peek() {
        if matches!(t.tok, Tok::Semi) {
            p.next();
        }
    }
    if let Some(t) = p.peek().cloned() {
        return Err(p.err_at(
            t.line,
            t.column,
            "unexpected trailing input; streaming clauses beyond GROUP BY / HAVING use the streaming dialect",
        ));
    }
    Ok(rule)
}

fn parse_foreach(p: &mut Parser) -> Result<StdRule, StdParseError> {
    p.expect_word("FOREACH")?;
    let array = parse_expr(p)?;
    // Optional `DO <fields>`: same item shape as a select list, but a bare
    // `*` is rejected (name the fields or omit `DO` for `{"item": ...}`).
    let fields = if p.peek_word_is("DO") {
        p.next();
        let mut items = Vec::new();
        loop {
            if let Some(t) = p.peek() {
                if matches!(t.tok, Tok::Star) {
                    return Err(p.err_here(
                        "DO * is not supported: name the fields or omit DO to emit {\"item\": element}",
                    ));
                }
            }
            let expr = parse_expr(p)?;
            let alias = parse_optional_alias(p)?;
            items.push((expr, alias));
            match p.peek() {
                Some(t) if matches!(t.tok, Tok::Comma) => {
                    p.next();
                }
                _ => break,
            }
        }
        Some(items)
    } else {
        None
    };
    let incase = if p.peek_word_is("INCASE") {
        p.next();
        Some(parse_expr(p)?)
    } else {
        None
    };
    let mut rule = parse_select_rule(p, true)?;
    if rule.foreach.is_some() {
        return Err(p.err_here("FOREACH must appear at most once"));
    }
    if rule.stmt.window.is_some() {
        // TODO(parity): is `FOREACH ... GROUP BY <window>` meaningful (expand
        // then window, or window then expand)? Rejected until decided.
        return Err(p.err_here("FOREACH cannot be combined with a window clause"));
    }
    // Lower the 1-based array indexing inside the expansion plan too.
    let array = rewrite_1based(&array);
    let fields = fields.map(|items| {
        items
            .into_iter()
            .map(|(e, a)| (rewrite_1based(&e), a))
            .collect()
    });
    let incase = incase.map(|e| rewrite_1based(&e));
    rule.foreach = Some(ForeachPlan {
        array,
        fields,
        incase,
    });
    Ok(rule)
}

fn parse_select_rule(p: &mut Parser, foreach_prefix: bool) -> Result<StdRule, StdParseError> {
    if !foreach_prefix {
        p.expect_word("SELECT")?;
    } else {
        // `FOREACH ... DO ...` already consumed the projection; the
        // remainder starts at `FROM`.
        if p.peek_word_is("SELECT") {
            return Err(p.err_here("FOREACH rules carry no SELECT list; use DO"));
        }
    }
    let mut fields: Vec<Expr> = Vec::new();
    let mut aliases: Vec<Option<String>> = Vec::new();
    let mut has_wildcard = false;
    if !foreach_prefix {
        loop {
            match p.peek() {
                Some(t) if matches!(t.tok, Tok::Star) => {
                    p.next();
                    fields.push(Expr::Wildcard);
                    aliases.push(None);
                    has_wildcard = true;
                }
                Some(_) => {
                    if p.peek_word_is("FROM") {
                        return Err(p.err_here("expected a select field before FROM"));
                    }
                    let expr = parse_expr(p)?;
                    let alias = parse_optional_alias(p)?;
                    fields.push(expr);
                    aliases.push(alias);
                }
                None => return Err(p.err_here("expected a select field, found end of input")),
            }
            match p.peek() {
                Some(t) if matches!(t.tok, Tok::Comma) => {
                    p.next();
                }
                _ => break,
            }
        }
        if fields.is_empty() {
            return Err(p.err_here("SELECT needs at least one field"));
        }
    } else {
        // The FOREACH row shape comes from DO/INCASE; the lowered select
        // keeps a neutral projection that the expansion path never reads.
        fields.push(Expr::Wildcard);
        aliases.push(None);
    }

    p.expect_word("FROM")?;
    let sources = parse_sources(p)?;

    let mut where_clause = None;
    if p.peek_word_is("WHERE") {
        p.next();
        where_clause = Some(parse_expr(p)?);
    }

    // Streaming extension: `GROUP BY` items, where window calls land on the
    // statement's window slot exactly like the streaming dialect.
    let mut group_by = Vec::new();
    let mut window: Option<WindowDef> = None;
    if p.peek_word_is("GROUP") {
        p.next();
        p.expect_word("BY")?;
        loop {
            // Capture the window call's start so window-argument errors
            // point at the offending token (spec: line and column), not a
            // constant. Reason: the lowered `Expr` carries no span, so the
            // position is threaded from the token stream.
            let (item_line, item_col) = match p.peek() {
                Some(t) => (t.line, t.column),
                None => (p.end_line, p.end_col),
            };
            let item = parse_expr(p)?;
            if let Some(w) = lower_window_call(&item, item_line, item_col)? {
                if window.is_some() {
                    return Err(p.err_here("at most one window clause per rule"));
                }
                window = Some(w);
            } else {
                group_by.push(item);
            }
            match p.peek() {
                Some(t) if matches!(t.tok, Tok::Comma) => {
                    p.next();
                }
                _ => break,
            }
        }
        if group_by.is_empty() && window.is_none() {
            return Err(p.err_here("GROUP BY needs at least one item"));
        }
    }

    let mut having = None;
    if p.peek_word_is("HAVING") {
        p.next();
        having = Some(parse_expr(p)?);
    }

    // Lower 1-based payload indexing onto the engine's 0-based indexing.
    let fields: Vec<Expr> = fields.into_iter().map(|e| rewrite_1based(&e)).collect();
    let where_clause = where_clause.map(|e| rewrite_1based(&e));
    let group_by: Vec<Expr> = group_by.into_iter().map(|e| rewrite_1based(&e)).collect();
    let having = having.map(|e| rewrite_1based(&e));

    let explicit_keys = collect_output_keys(&fields, &aliases);
    let stmt = SelectStmt {
        fields,
        field_aliases: aliases,
        from: "stream".to_string(),
        from_alias: None,
        joins: Vec::new(),
        where_clause,
        group_by,
        window,
        window_filter: None,
        window_trigger_condition: None,
        window_partition_by: None,
        having,
        order_by: Vec::new(),
        limit: None,
        set_op: None,
    };
    Ok(StdRule {
        stmt,
        sources,
        foreach: None,
        has_wildcard,
        explicit_keys,
    })
}

fn parse_sources(p: &mut Parser) -> Result<Vec<StdSource>, StdParseError> {
    let mut sources = Vec::new();
    loop {
        let next = p
            .next()
            .ok_or_else(|| p.err_here("expected a FROM source"))?;
        match next.tok {
            Tok::Str(s) => {
                if s.trim().is_empty() {
                    return Err(p.err_at(next.line, next.column, "FROM source must not be empty"));
                }
                if s.starts_with("$events/") {
                    sources.push(StdSource::Event(s));
                } else {
                    sources.push(StdSource::Topic(s));
                }
            }
            Tok::Word(w) => {
                if is_reserved(&w) {
                    return Err(p.err_at(
                        next.line,
                        next.column,
                        format!("expected a FROM source, found keyword '{w}'"),
                    ));
                }
                sources.push(StdSource::Stream(w));
            }
            _ => {
                return Err(p.err_at(
                    next.line,
                    next.column,
                    "expected a FROM source: a quoted topic filter or a stream name",
                ));
            }
        }
        match p.peek() {
            Some(t) if matches!(t.tok, Tok::Comma) => {
                p.next();
                continue;
            }
            _ => break,
        }
    }
    if sources.is_empty() {
        return Err(p.err_here("FROM needs at least one source"));
    }
    Ok(sources)
}

fn parse_optional_alias(p: &mut Parser) -> Result<Option<String>, StdParseError> {
    if p.eat_word("AS") {
        match p.next() {
            Some(t) => match t.tok {
                Tok::Word(w) => return Ok(Some(w)),
                Tok::Str(s) => return Ok(Some(s)),
                _ => {
                    return Err(p.err_at(t.line, t.column, "expected an alias after AS"));
                }
            },
            None => return Err(p.err_here("expected an alias after AS")),
        }
    }
    // Bare alias: a non-reserved word that does not start a new clause.
    if let Some(t) = p.peek().cloned() {
        if let Tok::Word(w) = t.tok {
            if !is_reserved(&w) {
                p.next();
                return Ok(Some(w));
            }
        }
    }
    Ok(None)
}

// ---------------------------------------------------------------------------
// Expressions
// ---------------------------------------------------------------------------

fn parse_expr(p: &mut Parser) -> Result<Expr, StdParseError> {
    parse_or(p)
}

fn parse_or(p: &mut Parser) -> Result<Expr, StdParseError> {
    let mut left = parse_and(p)?;
    while p.peek_word_is("OR") {
        p.next();
        let right = parse_and(p)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Or,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_and(p: &mut Parser) -> Result<Expr, StdParseError> {
    let mut left = parse_not(p)?;
    while p.peek_word_is("AND") {
        p.next();
        let right = parse_not(p)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_not(p: &mut Parser) -> Result<Expr, StdParseError> {
    if p.peek_word_is("NOT") {
        let t = p.next().expect("peeked NOT");
        // `NOT BETWEEN` / `NOT IN` / `NOT LIKE` bind at the range level.
        let saved = p.pos;
        if let Some(t2) = p.peek().cloned() {
            if let Tok::Word(w) = &t2.tok {
                if w.eq_ignore_ascii_case("BETWEEN")
                    || w.eq_ignore_ascii_case("IN")
                    || w.eq_ignore_ascii_case("LIKE")
                {
                    p.pos = saved;
                    return parse_range(p);
                }
            }
        }
        let _ = t;
        let expr = parse_not(p)?;
        return Ok(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        });
    }
    parse_range(p)
}

fn parse_range(p: &mut Parser) -> Result<Expr, StdParseError> {
    let left = parse_comparison(p)?;
    if p.peek_word_is("BETWEEN") {
        p.next();
        let low = parse_comparison(p)?;
        p.expect_word("AND")?;
        let high = parse_comparison(p)?;
        return Ok(Expr::Between {
            expr: Box::new(left),
            low: Box::new(low),
            high: Box::new(high),
            negated: false,
        });
    }
    if p.peek_word_is("NOT") {
        let saved = p.pos;
        p.next();
        if p.peek_word_is("BETWEEN") {
            p.next();
            let low = parse_comparison(p)?;
            p.expect_word("AND")?;
            let high = parse_comparison(p)?;
            return Ok(Expr::Between {
                expr: Box::new(left),
                low: Box::new(low),
                high: Box::new(high),
                negated: true,
            });
        }
        if p.peek_word_is("IN") {
            p.next();
            let list = parse_in_list(p)?;
            return Ok(Expr::InList {
                expr: Box::new(left),
                list,
                negated: true,
            });
        }
        if p.peek_word_is("LIKE") {
            p.next();
            let right = parse_comparison(p)?;
            let like = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            };
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(like),
            });
        }
        p.pos = saved;
        return Ok(left);
    }
    if p.peek_word_is("IN") {
        p.next();
        let list = parse_in_list(p)?;
        return Ok(Expr::InList {
            expr: Box::new(left),
            list,
            negated: false,
        });
    }
    if p.peek_word_is("IS") {
        p.next();
        let negated = p.eat_word("NOT");
        p.expect_word("NULL")?;
        return Ok(Expr::IsNull {
            expr: Box::new(left),
            negated,
        });
    }
    if p.peek_word_is("LIKE") {
        p.next();
        let right = parse_comparison(p)?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Like,
            right: Box::new(right),
        });
    }
    Ok(left)
}

fn parse_in_list(p: &mut Parser) -> Result<Vec<Expr>, StdParseError> {
    p.expect(&Tok::LParen, "'('")?;
    let mut list = Vec::new();
    if matches!(p.peek().map(|t| &t.tok), Some(Tok::RParen)) {
        p.next();
        return Ok(list);
    }
    loop {
        if matches!(p.peek().map(|t| &t.tok), Some(Tok::RParen)) {
            break;
        }
        list.push(parse_expr(p)?);
        match p.peek() {
            Some(t) if matches!(t.tok, Tok::Comma) => {
                p.next();
            }
            _ => break,
        }
    }
    p.expect(&Tok::RParen, "')'")?;
    Ok(list)
}

fn parse_comparison(p: &mut Parser) -> Result<Expr, StdParseError> {
    let left = parse_additive(p)?;
    // `NOT LIKE` after the left operand.
    if p.peek_word_is("NOT") {
        let saved = p.pos;
        p.next();
        if p.peek_word_is("LIKE") {
            p.next();
            let right = parse_additive(p)?;
            let like = Expr::BinaryOp {
                left: Box::new(left),
                op: BinaryOperator::Like,
                right: Box::new(right),
            };
            return Ok(Expr::UnaryOp {
                op: UnaryOperator::Not,
                expr: Box::new(like),
            });
        }
        p.pos = saved;
        return Ok(left);
    }
    if p.peek_word_is("LIKE") {
        p.next();
        let right = parse_additive(p)?;
        return Ok(Expr::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::Like,
            right: Box::new(right),
        });
    }
    let op = match p.peek().map(|t| &t.tok) {
        Some(Tok::Eq) => BinaryOperator::Eq,
        Some(Tok::Neq) => BinaryOperator::Neq,
        Some(Tok::Lt) => BinaryOperator::Lt,
        Some(Tok::Lte) => BinaryOperator::Lte,
        Some(Tok::Gt) => BinaryOperator::Gt,
        Some(Tok::Gte) => BinaryOperator::Gte,
        Some(Tok::Match) => {
            let at = p.peek().cloned().expect("peeked =~");
            p.next();
            return lower_topic_match(left, p, at);
        }
        _ => return Ok(left),
    };
    p.next();
    let right = parse_additive(p)?;
    Ok(Expr::BinaryOp {
        left: Box::new(left),
        op,
        right: Box::new(right),
    })
}

/// Lower `expr =~ 'mqtt/filter'` onto a `topic_match` call using the
/// broker's own topic matcher so semantics exactly match `TopicFilter::matches`.
/// The right side must be a string literal: a dynamic filter has no lowering,
/// so the rule is rejected at creation (fail closed) instead of guessing.
/// TODO(parity): does the documented `=~` also accept a dynamic (column)
/// right-hand side? If so it needs a runtime translation the engine does
/// not expose.
fn lower_topic_match(left: Expr, p: &mut Parser, at: Spanned) -> Result<Expr, StdParseError> {
    let rhs = parse_additive(p)?;
    let filter = match &rhs {
        Expr::Literal(serde_json::Value::String(s)) => s.clone(),
        _ => {
            return Err(p.err_at(
                at.line,
                at.column,
                "the right-hand side of =~ must be a quoted topic filter",
            ));
        }
    };
    validate_mqtt_filter(&filter).map_err(|reason| p.err_at(at.line, at.column, reason))?;
    Ok(Expr::Call {
        name: "topic_match".to_string(),
        args: vec![left, Expr::Literal(serde_json::Value::String(filter))],
    })
}

fn validate_mqtt_filter(filter: &str) -> Result<(), String> {
    if filter.trim().is_empty() {
        return Err("the right-hand side of =~ must not be empty".to_string());
    }
    if let Some(pos) = filter.find('#') {
        if pos != filter.len() - 1 {
            return Err("'#' must be the final level of a topic filter".to_string());
        }
        if pos > 0 && &filter[pos - 1..pos] != "/" {
            return Err("'#' must be prefixed with '/'".to_string());
        }
    }
    Ok(())
}

fn parse_additive(p: &mut Parser) -> Result<Expr, StdParseError> {
    let mut left = parse_multiplicative(p)?;
    loop {
        let op = match p.peek().map(|t| &t.tok) {
            Some(Tok::Plus) => BinaryOperator::Add,
            Some(Tok::Minus) => BinaryOperator::Sub,
            _ => break,
        };
        p.next();
        let right = parse_multiplicative(p)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_multiplicative(p: &mut Parser) -> Result<Expr, StdParseError> {
    let mut left = parse_unary(p)?;
    loop {
        let op = match p.peek().map(|t| &t.tok) {
            Some(Tok::Star) => BinaryOperator::Mul,
            Some(Tok::Slash) => BinaryOperator::Div,
            Some(Tok::Percent) => BinaryOperator::Mod,
            _ => break,
        };
        p.next();
        let right = parse_unary(p)?;
        left = Expr::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        };
    }
    Ok(left)
}

fn parse_unary(p: &mut Parser) -> Result<Expr, StdParseError> {
    if p.peek_word_is("NOT") {
        p.next();
        let expr = parse_unary(p)?;
        return Ok(Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr: Box::new(expr),
        });
    }
    match p.peek().map(|t| &t.tok) {
        Some(Tok::Minus) => {
            p.next();
            let expr = parse_unary(p)?;
            Ok(Expr::UnaryOp {
                op: UnaryOperator::Neg,
                expr: Box::new(expr),
            })
        }
        Some(Tok::Plus) => {
            p.next();
            parse_unary(p)
        }
        _ => parse_postfix(p),
    }
}

fn parse_postfix(p: &mut Parser) -> Result<Expr, StdParseError> {
    let mut expr = parse_primary(p)?;
    loop {
        match p.peek().map(|t| &t.tok) {
            Some(Tok::Dot) => {
                p.next();
                let field = match p.next() {
                    Some(t) => match t.tok {
                        Tok::Word(w) => w,
                        Tok::Str(s) => s,
                        _ => {
                            return Err(p.err_at(
                                t.line,
                                t.column,
                                "expected a field name after '.'",
                            ));
                        }
                    },
                    None => return Err(p.err_here("expected a field name after '.'")),
                };
                expr = Expr::FieldAccess {
                    parent: Box::new(expr),
                    field,
                };
            }
            Some(Tok::LBracket) => {
                p.next();
                // Slice `a[lo:hi]` (streaming extension, bounds forwarded
                // unchanged) versus index `a[i]`.
                // TODO(parity): slices keep 0-based bounds while plain
                // indexes are 1-based; confirm the documented dialect has no
                // slicing before relying on it.
                if matches!(p.peek().map(|t| &t.tok), Some(Tok::Colon)) {
                    p.next();
                    let hi = if matches!(p.peek().map(|t| &t.tok), Some(Tok::RBracket)) {
                        None
                    } else {
                        Some(Box::new(parse_expr(p)?))
                    };
                    p.expect(&Tok::RBracket, "']'")?;
                    expr = Expr::Slice {
                        base: Box::new(expr),
                        lo: None,
                        hi,
                    };
                } else {
                    let first = parse_expr(p)?;
                    if matches!(p.peek().map(|t| &t.tok), Some(Tok::Colon)) {
                        p.next();
                        let hi = if matches!(p.peek().map(|t| &t.tok), Some(Tok::RBracket)) {
                            None
                        } else {
                            Some(Box::new(parse_expr(p)?))
                        };
                        p.expect(&Tok::RBracket, "']'")?;
                        expr = Expr::Slice {
                            base: Box::new(expr),
                            lo: Some(Box::new(first)),
                            hi,
                        };
                    } else {
                        p.expect(&Tok::RBracket, "']'")?;
                        expr = Expr::Index {
                            base: Box::new(expr),
                            index: Box::new(first),
                        };
                    }
                }
            }
            _ => break,
        }
    }
    Ok(expr)
}

fn parse_primary(p: &mut Parser) -> Result<Expr, StdParseError> {
    let next = p
        .next()
        .ok_or_else(|| p.err_here("expected an expression"))?;
    match next.tok {
        Tok::Str(s) => Ok(Expr::Literal(serde_json::Value::String(s))),
        Tok::Num(n) => Ok(Expr::Literal(serde_json::Value::Number(n))),
        Tok::LParen => {
            let expr = parse_expr(p)?;
            p.expect(&Tok::RParen, "')'")?;
            Ok(expr)
        }
        Tok::Word(w) => {
            if w.eq_ignore_ascii_case("true") {
                return Ok(Expr::Literal(serde_json::Value::Bool(true)));
            }
            if w.eq_ignore_ascii_case("false") {
                return Ok(Expr::Literal(serde_json::Value::Bool(false)));
            }
            if w.eq_ignore_ascii_case("null") {
                return Ok(Expr::Literal(serde_json::Value::Null));
            }
            if w.eq_ignore_ascii_case("case") {
                return parse_case(p);
            }
            // Function call (builtin or engine function; unknown names
            // evaluate to Null, matching the streaming path).
            if matches!(p.peek().map(|t| &t.tok), Some(Tok::LParen)) {
                p.next();
                let mut args = Vec::new();
                if !matches!(p.peek().map(|t| &t.tok), Some(Tok::RParen)) {
                    loop {
                        if matches!(p.peek().map(|t| &t.tok), Some(Tok::Star)) {
                            p.next();
                            args.push(Expr::Wildcard);
                        } else {
                            args.push(parse_expr(p)?);
                        }
                        match p.peek() {
                            Some(t) if matches!(t.tok, Tok::Comma) => {
                                p.next();
                            }
                            _ => break,
                        }
                    }
                }
                p.expect(&Tok::RParen, "')'")?;
                if p.peek_word_is("OVER") {
                    return Err(p.err_here("analytic OVER clauses use the streaming dialect"));
                }
                return Ok(Expr::Call { name: w, args });
            }
            Ok(Expr::Identifier(w))
        }
        _ => Err(p.err_at(
            next.line,
            next.column,
            "expected an expression: a field, literal, function call or CASE",
        )),
    }
}

fn parse_case(p: &mut Parser) -> Result<Expr, StdParseError> {
    let operand = if p.peek_word_is("WHEN") {
        None
    } else {
        Some(Box::new(parse_expr(p)?))
    };
    let mut when_clauses = Vec::new();
    while p.peek_word_is("WHEN") {
        p.next();
        let condition = parse_expr(p)?;
        p.expect_word("THEN")?;
        let result = parse_expr(p)?;
        when_clauses.push((condition, result));
    }
    if when_clauses.is_empty() {
        return Err(p.err_here("CASE needs at least one WHEN clause"));
    }
    let else_clause = if p.peek_word_is("ELSE") {
        p.next();
        Some(Box::new(parse_expr(p)?))
    } else {
        None
    };
    p.expect_word("END")?;
    Ok(Expr::Case {
        operand,
        when_clauses,
        else_clause,
    })
}

// ---------------------------------------------------------------------------
// Window calls (streaming extension inside GROUP BY)
// ---------------------------------------------------------------------------

/// Recognize the streaming window calls inside `GROUP BY`, mirroring the
/// streaming dialect's shapes (`TUMBLINGWINDOW(unit, length)`,
/// `HOPPINGWINDOW(unit, length, interval)`,
/// `SLIDINGWINDOW(unit, length[, delay])`, `COUNTWINDOW(size[, interval])`,
/// `SESSIONWINDOW(unit, maxDuration, timeout)`).
/// `item_line`/`item_col` is the window call's start in the source text, so
/// argument errors point at the offending token.
fn lower_window_call(
    item: &Expr,
    item_line: usize,
    item_col: usize,
) -> Result<Option<WindowDef>, StdParseError> {
    let (name, args) = match item {
        Expr::Call { name, args } => (name, args),
        _ => return Ok(None),
    };
    let lowered = name.to_ascii_lowercase();
    match lowered.as_str() {
        "tumblingwindow" | "hoppingwindow" | "slidingwindow" | "countwindow" | "sessionwindow"
        | "statewindow" => {}
        _ => return Ok(None),
    }
    let bad = |msg: &str| StdParseError::new(item_line, item_col, msg.to_string());
    let unit = |e: &Expr| -> Result<TimeUnit, StdParseError> {
        match e {
            Expr::Identifier(n) => {
                time_unit(n).ok_or_else(|| bad("window unit must be dd, hh, mi, ss or ms"))
            }
            Expr::Literal(serde_json::Value::String(s)) => {
                time_unit(s).ok_or_else(|| bad("window unit must be dd, hh, mi, ss or ms"))
            }
            _ => Err(bad("window unit must be dd, hh, mi, ss or ms")),
        }
    };
    let u64_arg = |e: &Expr| -> Result<u64, StdParseError> {
        match e {
            Expr::Literal(v) => {
                if let Some(n) = v.as_u64() {
                    return Ok(n);
                }
                if let Some(n) = v.as_i64() {
                    if n >= 0 {
                        return Ok(n as u64);
                    }
                }
                if let Some(f) = v.as_f64() {
                    if f.is_finite() && f >= 0.0 && f.trunc() == f {
                        return Ok(f as u64);
                    }
                }
                Err(bad("window length must be a non-negative integer"))
            }
            _ => Err(bad("window length must be an integer literal")),
        }
    };
    let window = match lowered.as_str() {
        "tumblingwindow" => {
            if args.len() != 2 {
                return Err(bad("TUMBLINGWINDOW expects (unit, length)"));
            }
            WindowDef::TumblingTime {
                unit: unit(&args[0])?,
                length: u64_arg(&args[1])?,
            }
        }
        "hoppingwindow" => {
            if args.len() != 3 {
                return Err(bad("HOPPINGWINDOW expects (unit, length, interval)"));
            }
            WindowDef::HoppingTime {
                unit: unit(&args[0])?,
                length: u64_arg(&args[1])?,
                interval: u64_arg(&args[2])?,
            }
        }
        "slidingwindow" => {
            if args.len() != 2 && args.len() != 3 {
                return Err(bad("SLIDINGWINDOW expects (unit, length[, delay])"));
            }
            WindowDef::SlidingTime {
                unit: unit(&args[0])?,
                length: u64_arg(&args[1])?,
                delay: if args.len() == 3 {
                    Some(u64_arg(&args[2])?)
                } else {
                    None
                },
            }
        }
        "countwindow" => {
            if args.len() != 1 && args.len() != 2 {
                return Err(bad("COUNTWINDOW expects (size[, interval])"));
            }
            WindowDef::Count {
                size: u64_arg(&args[0])? as usize,
                interval: if args.len() == 2 {
                    Some(u64_arg(&args[1])? as usize)
                } else {
                    None
                },
            }
        }
        "statewindow" => {
            if args.is_empty() || args.len() > 2 {
                return Err(bad("STATEWINDOW expects 1 or 2 arguments"));
            }
            WindowDef::State {
                start_condition: args[0].clone(),
                end_condition: if args.len() == 2 {
                    Some(args[1].clone())
                } else {
                    None
                },
            }
        }
        _ => {
            if args.len() != 3 {
                return Err(bad("SESSIONWINDOW expects (unit, maxDuration, timeout)"));
            }
            let max_duration = u64_arg(&args[1])?;
            let timeout = u64_arg(&args[2])?;
            if max_duration == 0 || timeout == 0 {
                return Err(bad(
                    "SESSIONWINDOW maxDuration and timeout must be positive",
                ));
            }
            WindowDef::Session {
                unit: unit(&args[0])?,
                max_duration,
                timeout,
            }
        }
    };
    Ok(Some(window))
}

fn time_unit(s: &str) -> Option<TimeUnit> {
    match s.to_ascii_lowercase().as_str() {
        "dd" => Some(TimeUnit::Dd),
        "hh" => Some(TimeUnit::Hh),
        "mi" => Some(TimeUnit::Mi),
        "ss" => Some(TimeUnit::Ss),
        "ms" => Some(TimeUnit::Ms),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Output keys
// ---------------------------------------------------------------------------

/// Output keys named explicitly by the select list (aliases, bare
/// identifiers, dotted leaves); these survive the wildcard strip: when the
/// engine expands a top-level `*` it skips injected message keys, while
/// these named keys are always kept.
fn collect_output_keys(fields: &[Expr], aliases: &[Option<String>]) -> Vec<String> {
    let mut keys = Vec::new();
    for (idx, field) in fields.iter().enumerate() {
        // An explicit `AS` (or bare) alias wins, mirroring the engine's
        // projection naming.
        if let Some(alias) = aliases.get(idx).and_then(|a| a.as_ref()) {
            keys.push(alias.clone());
            continue;
        }
        if let Some(key) = explicit_key_of(field) {
            keys.push(key);
        }
    }
    keys
}

/// The engine's output key for a non-aliased field when it names one:
/// a bare identifier keeps its name, a dotted path keeps its leaf (the
/// engine flattens projections to the leaf field name), and an index or
/// slice keeps its base's key. Anything else (literals, calls, arithmetic,
/// `*`) has no explicit key.
fn explicit_key_of(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Wildcard => None,
        Expr::Identifier(name) => Some(name.clone()),
        Expr::FieldAccess { field: leaf, .. } => Some(leaf.clone()),
        Expr::Index { base, .. } => explicit_key_of(base),
        Expr::Slice { base, .. } => explicit_key_of(base),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// 1-based index lowering
// ---------------------------------------------------------------------------

/// Rewrite the dialect's 1-based `arr[N]` onto the engine's 0-based index:
/// a numeric index `e` becomes `e - 1`. A literal index of 0 or below has
/// no 1-based meaning, so the whole access lowers to `Null` (fail closed:
/// the predicate reads false, the projection reads Null).
/// TODO(parity): confirm `arr[0]` reads Null (rather than erroring the
/// rule) in the documented dialect.
pub(crate) fn rewrite_1based(expr: &Expr) -> Expr {
    match expr {
        Expr::Index { base, index } => {
            let base = Box::new(rewrite_1based(base));
            if let Expr::Literal(v) = index.as_ref() {
                if v.is_string() {
                    // Object field lookup: not an array position.
                    return Expr::Index {
                        base,
                        index: Box::new(rewrite_1based(index)),
                    };
                }
                let n = v
                    .as_i64()
                    .or_else(|| v.as_u64().and_then(|n| i64::try_from(n).ok()));
                if let Some(n) = n {
                    if n <= 0 {
                        return Expr::Literal(serde_json::Value::Null);
                    }
                    return Expr::Index {
                        base,
                        index: Box::new(Expr::Literal(serde_json::json!(n - 1))),
                    };
                }
                if let Some(f) = v.as_f64() {
                    if f <= 0.0 {
                        return Expr::Literal(serde_json::Value::Null);
                    }
                }
            }
            Expr::Index {
                base,
                index: Box::new(Expr::BinaryOp {
                    left: Box::new(rewrite_1based(index)),
                    op: BinaryOperator::Sub,
                    right: Box::new(Expr::Literal(serde_json::json!(1))),
                }),
            }
        }
        Expr::Slice { base, lo, hi } => Expr::Slice {
            base: Box::new(rewrite_1based(base)),
            lo: lo.as_ref().map(|e| Box::new(rewrite_1based(e))),
            hi: hi.as_ref().map(|e| Box::new(rewrite_1based(e))),
        },
        Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
            left: Box::new(rewrite_1based(left)),
            op: op.clone(),
            right: Box::new(rewrite_1based(right)),
        },
        Expr::UnaryOp { op, expr } => Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_1based(expr)),
        },
        Expr::Between {
            expr,
            low,
            high,
            negated,
        } => Expr::Between {
            expr: Box::new(rewrite_1based(expr)),
            low: Box::new(rewrite_1based(low)),
            high: Box::new(rewrite_1based(high)),
            negated: *negated,
        },
        Expr::InList {
            expr,
            list,
            negated,
        } => Expr::InList {
            expr: Box::new(rewrite_1based(expr)),
            list: list.iter().map(rewrite_1based).collect(),
            negated: *negated,
        },
        Expr::IsNull { expr, negated } => Expr::IsNull {
            expr: Box::new(rewrite_1based(expr)),
            negated: *negated,
        },
        Expr::FieldAccess { parent, field } => Expr::FieldAccess {
            parent: Box::new(rewrite_1based(parent)),
            field: field.clone(),
        },
        Expr::Call { name, args } => Expr::Call {
            name: name.clone(),
            args: args.iter().map(rewrite_1based).collect(),
        },
        Expr::Case {
            operand,
            when_clauses,
            else_clause,
        } => Expr::Case {
            operand: operand.as_ref().map(|e| Box::new(rewrite_1based(e))),
            when_clauses: when_clauses
                .iter()
                .map(|(c, r)| (rewrite_1based(c), rewrite_1based(r)))
                .collect(),
            else_clause: else_clause.as_ref().map(|e| Box::new(rewrite_1based(e))),
        },
        Expr::Over {
            call,
            partition_by,
            when,
        } => Expr::Over {
            call: Box::new(rewrite_1based(call)),
            partition_by: partition_by.as_ref().map(|e| Box::new(rewrite_1based(e))),
            when: when.as_ref().map(|e| Box::new(rewrite_1based(e))),
        },
        Expr::Wildcard | Expr::WildcardModified { .. } | Expr::Identifier(_) | Expr::Literal(_) => {
            expr.clone()
        }
    }
}
