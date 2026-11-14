//! The expression form. It is deliberately tiny.
//!
//! **References.** A string that starts with `$` is a reference:
//! `$item`, `$item.tree.path`, `$item.error.from`. The first segment names
//! a value, and in a pipe the only one is `item`, the item reaching the
//! operator. Later segments walk into maps. A missing field reads as null. A
//! string that starts with `$$` is the literal text with one `$` removed.
//!
//! **Templates.** Any settings or `with:` value is a template: maps and
//! lists are walked, a reference anywhere is replaced by its value, every
//! other value is literal. A reference replaces the whole string; there is
//! no interpolation inside text.
//!
//! **Conditions** (`filter`): one operand, or two operands joined by `==`
//! or `!=`. An operand is a reference, a quoted string (`"open"` or
//! `'open'`), an integer, `true`, `false` or `null`. One operand alone is
//! true when it is neither null nor false. Equality is JSON equality.
//! There is no `and`, `or`, `not`, arithmetic, or function call.

use std::collections::BTreeMap;

use serde_json::Value;
use thiserror::Error;

/// A `$` string that is not a well-formed reference.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum RefError {
    /// The first segment, the name the reference reads, is not a name.
    #[error(
        "`${body}` is not a reference: a reference is `$name` or `$name.field`, lower snake_case"
    )]
    BadRoot {
        /// The text after the `$`.
        body: String,
    },
    /// A field segment after the first is not a name.
    #[error("`${body}` is not a reference: segment `{segment}` must be lower snake_case")]
    BadSegment {
        /// The text after the `$`.
        body: String,
        /// The segment that is not lower snake_case.
        segment: String,
    },
}

/// A `filter` condition that cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum CondError {
    /// The condition is neither text nor a bool.
    #[error("a condition is text such as `$item.state != \"open\"`")]
    NotText,
    /// A quote opens and never closes.
    #[error("`{text}`: unclosed quote")]
    UnclosedQuote {
        /// The whole condition.
        text: String,
    },
    /// An `=` or `!` that is not part of `==` or `!=`.
    #[error("`{text}`: the only operators are == and !=")]
    BadOperator {
        /// The whole condition.
        text: String,
    },
    /// An operator stands where an operand belongs.
    #[error("expected a value, found `{op}`")]
    ExpectedValue {
        /// The operator found.
        op: String,
    },
    /// An unquoted word that is no reference, number, bool or null.
    #[error("bare word `{word}`: quote text (\"{word}\"), or write a reference ($item.field), an integer, true, false or null")]
    BareWord {
        /// The word as written.
        word: String,
    },
    /// Neither one operand nor two joined by `==` or `!=`.
    #[error("`{text}`: a condition is one value, or two values joined by == or != (no and/or)")]
    BadForm {
        /// The whole condition.
        text: String,
    },
    /// An operand is a malformed reference.
    #[error(transparent)]
    Ref(#[from] RefError),
}

/// Names in the vine language: lower snake_case.
pub fn is_name(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some('a'..='z'))
        && chars.all(|c| matches!(c, 'a'..='z' | '0'..='9' | '_'))
}

/// A reference such as `$item.job`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ref {
    pub root: String,
    pub path: Vec<String>,
}

impl Ref {
    /// Parse the text after the `$`.
    pub fn parse(body: &str) -> Result<Ref, RefError> {
        let mut parts = body.split('.');
        let root = parts.next().unwrap_or_default();
        if !is_name(root) {
            return Err(RefError::BadRoot { body: body.into() });
        }
        let mut path = Vec::new();
        for p in parts {
            if !is_name(p) {
                return Err(RefError::BadSegment {
                    body: body.into(),
                    segment: p.into(),
                });
            }
            path.push(p.to_owned());
        }
        Ok(Ref {
            root: root.to_owned(),
            path,
        })
    }

    pub fn eval(&self, env: &Env) -> Value {
        let mut cur = env.get(&self.root).cloned().unwrap_or(Value::Null);
        for seg in &self.path {
            cur = match cur {
                Value::Object(mut m) => m.remove(seg).unwrap_or(Value::Null),
                _ => Value::Null,
            };
        }
        cur
    }
}

impl std::fmt::Display for Ref {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${}", self.root)?;
        for p in &self.path {
            write!(f, ".{p}")?;
        }
        Ok(())
    }
}

/// The names a template or condition can read, and their values.
pub type Env = BTreeMap<String, Value>;

/// An unresolved value: literal parts and references.
#[derive(Debug, Clone, PartialEq)]
pub enum Template {
    Lit(Value),
    Ref(Ref),
    Obj(BTreeMap<String, Template>),
    List(Vec<Template>),
}

impl Template {
    pub fn parse(v: &Value) -> Result<Template, RefError> {
        Ok(match v {
            Value::String(s) => {
                if let Some(rest) = s.strip_prefix("$$") {
                    Template::Lit(Value::String(format!("${rest}")))
                } else if let Some(body) = s.strip_prefix('$') {
                    Template::Ref(Ref::parse(body)?)
                } else {
                    Template::Lit(v.clone())
                }
            }
            Value::Object(m) => {
                let mut out = BTreeMap::new();
                for (k, v) in m {
                    out.insert(k.clone(), Template::parse(v)?);
                }
                Template::Obj(out)
            }
            Value::Array(items) => Template::List(
                items
                    .iter()
                    .map(Template::parse)
                    .collect::<Result<_, _>>()?,
            ),
            other => Template::Lit(other.clone()),
        })
    }

    pub fn refs(&self) -> Vec<&Ref> {
        let mut out = Vec::new();
        self.collect(&mut out);
        out
    }

    fn collect<'a>(&'a self, out: &mut Vec<&'a Ref>) {
        match self {
            Template::Lit(_) => {}
            Template::Ref(r) => out.push(r),
            Template::Obj(m) => m.values().for_each(|t| t.collect(out)),
            Template::List(l) => l.iter().for_each(|t| t.collect(out)),
        }
    }

    pub fn resolve(&self, env: &Env) -> Value {
        match self {
            Template::Lit(v) => v.clone(),
            Template::Ref(r) => r.eval(env),
            Template::Obj(m) => {
                Value::Object(m.iter().map(|(k, t)| (k.clone(), t.resolve(env))).collect())
            }
            Template::List(l) => Value::Array(l.iter().map(|t| t.resolve(env)).collect()),
        }
    }
}

/// One side of a condition.
#[derive(Debug, Clone, PartialEq)]
pub enum Operand {
    Ref(Ref),
    Lit(Value),
}

impl Operand {
    pub fn eval(&self, env: &Env) -> Value {
        match self {
            Operand::Ref(r) => r.eval(env),
            Operand::Lit(v) => v.clone(),
        }
    }

    fn parse_token(tok: &Token) -> Result<Operand, CondError> {
        match tok {
            Token::Quoted(s) => Ok(Operand::Lit(Value::String(s.clone()))),
            Token::Op(op) => Err(CondError::ExpectedValue { op: op.clone() }),
            Token::Word(w) => {
                if let Some(body) = w.strip_prefix('$') {
                    return Ok(Operand::Ref(Ref::parse(body)?));
                }
                match w.as_str() {
                    "null" => Ok(Operand::Lit(Value::Null)),
                    "true" => Ok(Operand::Lit(Value::Bool(true))),
                    "false" => Ok(Operand::Lit(Value::Bool(false))),
                    _ => match w.parse::<i64>() {
                        Ok(n) => Ok(Operand::Lit(Value::from(n))),
                        Err(_) => Err(CondError::BareWord { word: w.clone() }),
                    },
                }
            }
        }
    }

    pub fn as_ref(&self) -> Option<&Ref> {
        match self {
            Operand::Ref(r) => Some(r),
            Operand::Lit(_) => None,
        }
    }
}

/// A `filter` condition.
#[derive(Debug, Clone, PartialEq)]
pub enum Cond {
    Truthy(Operand),
    Eq(Operand, Operand),
    Ne(Operand, Operand),
}

impl Cond {
    pub fn parse(v: &Value) -> Result<Cond, CondError> {
        let s = match v {
            Value::Bool(b) => return Ok(Cond::Truthy(Operand::Lit(Value::Bool(*b)))),
            Value::String(s) => s,
            _ => return Err(CondError::NotText),
        };
        let toks = tokenize(s)?;
        match toks.as_slice() {
            [a] => Ok(Cond::Truthy(Operand::parse_token(a)?)),
            [a, Token::Op(op), b] => {
                let (a, b) = (Operand::parse_token(a)?, Operand::parse_token(b)?);
                Ok(if op == "==" {
                    Cond::Eq(a, b)
                } else {
                    Cond::Ne(a, b)
                })
            }
            _ => Err(CondError::BadForm { text: s.clone() }),
        }
    }

    pub fn refs(&self) -> Vec<&Ref> {
        match self {
            Cond::Truthy(a) => a.as_ref().into_iter().collect(),
            Cond::Eq(a, b) | Cond::Ne(a, b) => a.as_ref().into_iter().chain(b.as_ref()).collect(),
        }
    }

    pub fn eval(&self, env: &Env) -> bool {
        match self {
            Cond::Truthy(a) => !matches!(a.eval(env), Value::Null | Value::Bool(false)),
            Cond::Eq(a, b) => a.eval(env) == b.eval(env),
            Cond::Ne(a, b) => a.eval(env) != b.eval(env),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Token {
    Word(String),
    Quoted(String),
    Op(String),
}

fn tokenize(s: &str) -> Result<Vec<Token>, CondError> {
    let mut toks = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(&c) = chars.peek() {
        if c.is_whitespace() {
            chars.next();
        } else if c == '"' || c == '\'' {
            chars.next();
            let mut lit = String::new();
            loop {
                match chars.next() {
                    Some(q) if q == c => break,
                    Some(ch) => lit.push(ch),
                    None => return Err(CondError::UnclosedQuote { text: s.into() }),
                }
            }
            toks.push(Token::Quoted(lit));
        } else if c == '=' || c == '!' {
            chars.next();
            if chars.next() != Some('=') {
                return Err(CondError::BadOperator { text: s.into() });
            }
            toks.push(Token::Op(format!("{c}=")));
        } else {
            let mut w = String::new();
            while let Some(&ch) = chars.peek() {
                if ch.is_whitespace() || ch == '=' || ch == '!' || ch == '"' || ch == '\'' {
                    break;
                }
                w.push(ch);
                chars.next();
            }
            toks.push(Token::Word(w));
        }
    }
    Ok(toks)
}

/// The key a `branch` case matches: text as itself, bools and integers as
/// written. Anything else (null, maps, lists) matches no case.
pub fn case_key(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}
