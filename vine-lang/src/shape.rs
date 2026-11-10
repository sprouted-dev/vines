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

use crate::expr::Template;

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
    pub fn parse(v: &Value) -> Result<Shape, String> {
        let Some(map) = v.as_object() else {
            return Err(format!(
                "a shape is a map of field name to type, found {}",
                kind(v)
            ));
        };
        let mut shape = Shape::new();
        for (name, ty) in map {
            if !crate::expr::is_name(name) {
                return Err(format!(
                    "field name `{name}` must be lower snake_case (a-z, 0-9, _)"
                ));
            }
            let Some(spec) = ty.as_str() else {
                return Err(format!(
                    "field `{name}`: type must be a type name such as `text` or `int?`, found {}",
                    kind(ty)
                ));
            };
            let (spec, required) = match spec.strip_suffix('?') {
                Some(s) => (s, false),
                None => (spec, true),
            };
            let Some(ty) = Ty::parse(spec) else {
                return Err(format!(
                    "field `{name}`: unknown type `{spec}` (known: text, id, int, bool, object, list, any)"
                ));
            };
            shape.fields.insert(name.clone(), Field { ty, required });
        }
        Ok(shape)
    }

    /// Check a concrete value against the shape.
    pub fn check(&self, v: &Value) -> Result<(), String> {
        let Some(map) = v.as_object() else {
            return Err(format!("expected a map, found {}", kind(v)));
        };
        for (name, field) in &self.fields {
            match map.get(name) {
                None | Some(Value::Null) if field.required && field.ty != Ty::Any => {
                    return Err(format!("missing required field `{name}` ({})", field.ty));
                }
                None => {}
                Some(Value::Null) if !field.required => {}
                Some(value) if !field.ty.fits(value) => {
                    return Err(format!(
                        "field `{name}` should be {}, found {}",
                        field.ty,
                        describe(value)
                    ));
                }
                Some(_) => {}
            }
        }
        if !self.open {
            if let Some(extra) = map.keys().find(|k| !self.fields.contains_key(*k)) {
                return Err(format!("unexpected field `{extra}` (not in the shape)"));
            }
        }
        Ok(())
    }

    /// Check an unresolved template as far as is knowable before running:
    /// field names, required fields, and the types of literal values.
    /// References are checked at run time, once resolved.
    pub fn check_template(&self, t: &Template) -> Result<(), String> {
        let fields = match t {
            Template::Ref(_) => return Ok(()),
            Template::Obj(fields) => fields,
            Template::Lit(v) => return self.check(v),
            Template::List(_) => return Err("expected a map, found a list".to_owned()),
        };
        for (name, field) in &self.fields {
            match fields.get(name) {
                None | Some(Template::Lit(Value::Null))
                    if field.required && field.ty != Ty::Any =>
                {
                    return Err(format!("missing required field `{name}` ({})", field.ty));
                }
                None | Some(Template::Ref(_)) => {}
                Some(Template::Lit(Value::Null)) if !field.required => {}
                Some(Template::Lit(v)) => {
                    if !field.ty.fits(v) {
                        return Err(format!(
                            "field `{name}` should be {}, found {}",
                            field.ty,
                            describe(v)
                        ));
                    }
                }
                Some(Template::Obj(_)) => {
                    if !matches!(field.ty, Ty::Object | Ty::Any) {
                        return Err(format!(
                            "field `{name}` should be {}, found a map",
                            field.ty
                        ));
                    }
                }
                Some(Template::List(_)) => {
                    if !matches!(field.ty, Ty::List | Ty::Any) {
                        return Err(format!(
                            "field `{name}` should be {}, found a list",
                            field.ty
                        ));
                    }
                }
            }
        }
        if !self.open {
            if let Some(extra) = fields.keys().find(|k| !self.fields.contains_key(*k)) {
                return Err(format!("unexpected field `{extra}` (not in the shape)"));
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
