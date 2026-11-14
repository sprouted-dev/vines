//! The shape language: a flat map of field name to a simple type.
//!
//! In YAML a shape is a map. Each value is a type name, with a trailing `?`
//! for an optional field:
//!
//! ```yaml
//! input: { item: id, claimant: text, version: int, note: text? }
//! ```
//!
//! Types: `text`, `id` (non-empty text), `int`, `bool`, `object` (any map),
//! `list` (any list), `any` (anything, including null). Shapes do not nest;
//! `object` and `list` say nothing about what is inside.
//!
//! A required field must be present and not null (unless its type is `any`).
//! An optional field may be absent or null. A closed shape (every shape
//! written in YAML) refuses fields it does not list. An open shape, which
//! only Rust can build, accepts extra fields.

use std::collections::BTreeMap;
use std::fmt;

use serde_json::Value;
use thiserror::Error;

use crate::expr::Template;

/// A shape written in a vine document that cannot be read.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum ShapeError {
    /// The shape is not a map.
    #[error("a shape is a map of field name to type, found {found}")]
    NotAMap {
        /// What was written instead, such as `a list`.
        found: &'static str,
    },
    /// A field name is not lower snake_case.
    #[error("field name `{name}` must be lower snake_case (a-z, 0-9, _)")]
    BadFieldName {
        /// The field name as written.
        name: String,
    },
    /// A field's type is not written as text.
    #[error("field `{name}`: type must be a type name such as `text` or `int?`, found {found}")]
    NotATypeName {
        /// The field.
        name: String,
        /// What was written instead, such as `a number`.
        found: &'static str,
    },
    /// A field's type names no known type.
    #[error(
        "field `{name}`: unknown type `{spec}` (known: text, id, int, bool, object, list, any)"
    )]
    UnknownType {
        /// The field.
        name: String,
        /// The type name as written, without a trailing `?`.
        spec: String,
    },
}

/// A value, or a template of one, that does not fit a shape.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum Misfit {
    /// The value is not a map.
    #[error("expected a map, found {found}")]
    NotAMap {
        /// What it is instead, such as `a list`.
        found: &'static str,
    },
    /// A required field is absent or null.
    #[error("missing required field `{name}` ({ty})")]
    MissingField {
        /// The field.
        name: String,
        /// Its type in the shape.
        ty: Ty,
    },
    /// A field holds a value of another type.
    #[error("field `{name}` should be {ty}, found {found}")]
    WrongType {
        /// The field.
        name: String,
        /// Its type in the shape.
        ty: Ty,
        /// What it holds, such as `number 3` or `a map`.
        found: String,
    },
    /// A closed shape does not list this field.
    #[error("unexpected field `{name}` (not in the shape)")]
    UnexpectedField {
        /// The field.
        name: String,
    },
}

/// Why every value of one shape is not sure to fit another, the target.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum FitError {
    /// An open shape may hold fields a closed target does not list.
    #[error("its shape is open (it may hold fields the output does not list)")]
    Open,
    /// The target requires a field this shape does not have.
    #[error("missing required field `{name}` ({ty})")]
    MissingField {
        /// The field.
        name: String,
        /// Its type in the target.
        ty: Ty,
    },
    /// The target requires a field this shape has as optional.
    #[error("field `{name}` is optional there but required in the output")]
    Optional {
        /// The field.
        name: String,
    },
    /// The field's type here does not fit its type in the target.
    #[error("field `{name}` is {have}, the output wants {want}")]
    WrongType {
        /// The field.
        name: String,
        /// Its type in this shape.
        have: Ty,
        /// Its type in the target.
        want: Ty,
    },
    /// A closed target does not list this field.
    #[error("unexpected field `{name}` (not in the output)")]
    UnexpectedField {
        /// The field.
        name: String,
    },
}

/// A simple field type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ty {
    Text,
    Id,
    Int,
    Bool,
    Object,
    List,
    Any,
}

impl Ty {
    fn parse(s: &str) -> Option<Ty> {
        Some(match s {
            "text" => Ty::Text,
            "id" => Ty::Id,
            "int" => Ty::Int,
            "bool" => Ty::Bool,
            "object" => Ty::Object,
            "list" => Ty::List,
            "any" => Ty::Any,
            _ => return None,
        })
    }

    /// Whether a value fits this type. Null fits only `any`.
    pub fn fits(self, v: &Value) -> bool {
        match self {
            Ty::Text => v.is_string(),
            Ty::Id => v.as_str().is_some_and(|s| !s.is_empty()),
            Ty::Int => v.is_i64() || v.is_u64(),
            Ty::Bool => v.is_boolean(),
            Ty::Object => v.is_object(),
            Ty::List => v.is_array(),
            Ty::Any => true,
        }
    }
}

impl fmt::Display for Ty {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Ty::Text => "text",
            Ty::Id => "id",
            Ty::Int => "int",
            Ty::Bool => "bool",
            Ty::Object => "object",
            Ty::List => "list",
            Ty::Any => "any",
        })
    }
}

/// One field of a shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Field {
    pub ty: Ty,
    pub required: bool,
}

/// A flat record shape.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Shape {
    fields: BTreeMap<String, Field>,
    open: bool,
}

impl Shape {
    /// An empty closed shape: accepts only `{}` until fields are added.
    pub fn new() -> Self {
        Self::default()
    }

    /// An open shape: listed fields are checked, extra fields are accepted.
    pub fn open() -> Self {
        Shape {
            fields: BTreeMap::new(),
            open: true,
        }
    }

    /// Add a required field.
    pub fn required(mut self, name: &str, ty: Ty) -> Self {
        self.fields
            .insert(name.to_owned(), Field { ty, required: true });
        self
    }

    /// Add an optional field.
    pub fn optional(mut self, name: &str, ty: Ty) -> Self {
        self.fields.insert(
            name.to_owned(),
            Field {
                ty,
                required: false,
            },
        );
        self
    }

    pub fn field(&self, name: &str) -> Option<&Field> {
        self.fields.get(name)
    }

    pub fn is_open(&self) -> bool {
        self.open
    }

    /// Parse a shape written in a vine document.
    pub fn parse(v: &Value) -> Result<Shape, ShapeError> {
        let Some(map) = v.as_object() else {
            return Err(ShapeError::NotAMap { found: kind(v) });
        };
        let mut shape = Shape::new();
        for (name, ty) in map {
            if !crate::expr::is_name(name) {
                return Err(ShapeError::BadFieldName { name: name.clone() });
            }
            let Some(spec) = ty.as_str() else {
                return Err(ShapeError::NotATypeName {
                    name: name.clone(),
                    found: kind(ty),
                });
            };
            let (spec, required) = match spec.strip_suffix('?') {
                Some(s) => (s, false),
                None => (spec, true),
            };
            let Some(ty) = Ty::parse(spec) else {
                return Err(ShapeError::UnknownType {
                    name: name.clone(),
                    spec: spec.to_owned(),
                });
            };
            shape.fields.insert(name.clone(), Field { ty, required });
        }
        Ok(shape)
    }

    /// Check a concrete value against the shape.
    pub fn check(&self, v: &Value) -> Result<(), Misfit> {
        let Some(map) = v.as_object() else {
            return Err(Misfit::NotAMap { found: kind(v) });
        };
        for (name, field) in &self.fields {
            match map.get(name) {
                None | Some(Value::Null) if field.required && field.ty != Ty::Any => {
                    return Err(Misfit::MissingField {
                        name: name.clone(),
                        ty: field.ty,
                    });
                }
                None => {}
                Some(Value::Null) if !field.required => {}
                Some(value) if !field.ty.fits(value) => {
                    return Err(Misfit::WrongType {
                        name: name.clone(),
                        ty: field.ty,
                        found: describe(value),
                    });
                }
                Some(_) => {}
            }
        }
        if !self.open {
            if let Some(extra) = map.keys().find(|k| !self.fields.contains_key(*k)) {
                return Err(Misfit::UnexpectedField {
                    name: extra.clone(),
                });
            }
        }
        Ok(())
    }

    /// Whether every value of this shape is sure to fit `target`.
    pub fn fits_into(&self, target: &Shape) -> Result<(), FitError> {
        if self.open && !target.open {
            return Err(FitError::Open);
        }
        for (name, want) in &target.fields {
            match self.fields.get(name) {
                None if want.required && want.ty != Ty::Any => {
                    return Err(FitError::MissingField {
                        name: name.clone(),
                        ty: want.ty,
                    });
                }
                None => {}
                Some(have) => {
                    if want.required && !have.required && want.ty != Ty::Any {
                        return Err(FitError::Optional { name: name.clone() });
                    }
                    let fits = have.ty == want.ty
                        || want.ty == Ty::Any
                        || (have.ty == Ty::Id && want.ty == Ty::Text);
                    if !fits {
                        return Err(FitError::WrongType {
                            name: name.clone(),
                            have: have.ty,
                            want: want.ty,
                        });
                    }
                }
            }
        }
        if !target.open {
            if let Some(extra) = self.fields.keys().find(|k| !target.fields.contains_key(*k)) {
                return Err(FitError::UnexpectedField {
                    name: extra.clone(),
                });
            }
        }
        Ok(())
    }

    /// Check an unresolved template as far as is knowable before running:
    /// field names, required fields, and the types of literal values.
    /// References are checked at run time, once resolved.
    pub fn check_template(&self, t: &Template) -> Result<(), Misfit> {
        let fields = match t {
            Template::Ref(_) => return Ok(()),
            Template::Obj(fields) => fields,
            Template::Lit(v) => return self.check(v),
            Template::List(_) => return Err(Misfit::NotAMap { found: "a list" }),
        };
        for (name, field) in &self.fields {
            let wrong = |found: String| Misfit::WrongType {
                name: name.clone(),
                ty: field.ty,
                found,
            };
            match fields.get(name) {
                None | Some(Template::Lit(Value::Null))
                    if field.required && field.ty != Ty::Any =>
                {
                    return Err(Misfit::MissingField {
                        name: name.clone(),
                        ty: field.ty,
                    });
                }
                None | Some(Template::Ref(_)) => {}
                Some(Template::Lit(Value::Null)) if !field.required => {}
                Some(Template::Lit(v)) => {
                    if !field.ty.fits(v) {
                        return Err(wrong(describe(v)));
                    }
                }
                Some(Template::Obj(_)) => {
                    if !matches!(field.ty, Ty::Object | Ty::Any) {
                        return Err(wrong("a map".into()));
                    }
                }
                Some(Template::List(_)) => {
                    if !matches!(field.ty, Ty::List | Ty::Any) {
                        return Err(wrong("a list".into()));
                    }
                }
            }
        }
        if !self.open {
            if let Some(extra) = fields.keys().find(|k| !self.fields.contains_key(*k)) {
                return Err(Misfit::UnexpectedField {
                    name: extra.clone(),
                });
            }
        }
        Ok(())
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a bool",
        Value::Number(_) => "a number",
        Value::String(_) => "text",
        Value::Array(_) => "a list",
        Value::Object(_) => "a map",
    }
}

fn describe(v: &Value) -> String {
    match v {
        Value::String(s) if s.is_empty() => "empty text".to_owned(),
        Value::String(s) => format!("text {s:?}"),
        Value::Number(n) => format!("number {n}"),
        Value::Bool(b) => format!("bool {b}"),
        other => kind(other).to_owned(),
    }
}
