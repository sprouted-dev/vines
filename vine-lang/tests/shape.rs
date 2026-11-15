//! What a shape reads, which values and templates fit it, and which shapes
//! fit into another. Rust alone: this file passes with YAML off.
#![cfg(test)]

use vine_lang::expr::Template;
use vine_lang::shape::{FitError, Misfit, ShapeError};
use vine_lang::{json, Shape, Ty, Value};

#[test]
fn a_shape_reads_every_type_name_and_a_trailing_question_mark_as_optional() {
    let read = Shape::parse(&json!({
        "a": "text", "b": "id", "c": "int", "d": "bool",
        "e": "object", "f": "list", "g": "any", "h": "bool?",
    }))
    .unwrap();
    let want = Shape::new()
        .required("a", Ty::Text)
        .required("b", Ty::Id)
        .required("c", Ty::Int)
        .required("d", Ty::Bool)
        .required("e", Ty::Object)
        .required("f", Ty::List)
        .required("g", Ty::Any)
        .optional("h", Ty::Bool);
    assert_eq!(read, want);
    assert!(!read.is_open());
}

#[test]
fn a_shape_that_is_not_a_map_of_type_names_is_refused_with_what_was_found() {
    assert_eq!(
        Shape::parse(&json!(["text"])),
        Err(ShapeError::NotAMap { found: "a list" })
    );
    assert_eq!(
        Shape::parse(&json!({ "n": 3 })),
        Err(ShapeError::NotATypeName {
            name: "n".into(),
            found: "a number"
        })
    );
}

#[test]
fn only_an_open_shape_is_open() {
    assert!(Shape::open().is_open());
    assert!(!Shape::new().is_open());
}

#[test]
fn an_int_is_any_whole_number_a_negative_one_or_one_past_i64() {
    for n in [json!(-1), json!(u64::MAX), json!(3)] {
        assert!(Ty::Int.fits(&n), "{n}");
    }
    assert!(!Ty::Int.fits(&json!(1.5)));
}

#[test]
fn an_optional_field_may_be_null_and_a_required_one_of_type_any_too() {
    let shape = Shape::new()
        .optional("note", Ty::Text)
        .required("extra", Ty::Any);
    assert_eq!(shape.check(&json!({ "note": null, "extra": null })), Ok(()));
    assert_eq!(
        shape.check(&json!({ "note": 3, "extra": null })),
        Err(Misfit::WrongType {
            name: "note".into(),
            ty: Ty::Text,
            found: "number 3".into()
        })
    );
}

#[test]
fn a_misfit_says_what_it_found() {
    let id = Shape::new().required("job", Ty::Id);
    assert_eq!(
        id.check(&json!({ "job": "" })),
        Err(Misfit::WrongType {
            name: "job".into(),
            ty: Ty::Id,
            found: "empty text".into()
        })
    );
    assert_eq!(
        id.check(&json!({ "job": true })),
        Err(Misfit::WrongType {
            name: "job".into(),
            ty: Ty::Id,
            found: "bool true".into()
        })
    );
    for (value, found) in [
        (json!(null), "null"),
        (json!(true), "a bool"),
        (json!(1), "a number"),
        (json!("x"), "text"),
        (json!([]), "a list"),
    ] {
        assert_eq!(id.check(&value), Err(Misfit::NotAMap { found }), "{value}");
    }
}

fn template(v: &Value) -> Template {
    Template::parse(v).unwrap()
}

#[test]
fn a_template_may_leave_an_optional_field_null_but_not_a_required_one() {
    let shape = Shape::new()
        .optional("note", Ty::Text)
        .required("job", Ty::Id);
    let t = template(&json!({ "note": null, "job": "$item.job" }));
    assert_eq!(shape.check_template(&t), Ok(()));
    let t = template(&json!({ "note": null, "job": null }));
    assert_eq!(
        shape.check_template(&t),
        Err(Misfit::MissingField {
            name: "job".into(),
            ty: Ty::Id
        })
    );
}

#[test]
fn a_map_or_list_in_a_template_fits_only_a_field_of_that_kind_or_any() {
    let shape = Shape::new()
        .required("o", Ty::Object)
        .required("l", Ty::List)
        .required("a", Ty::Any);
    let t = template(&json!({ "o": { "x": "$item.x" }, "l": ["$item.x"], "a": {} }));
    assert_eq!(shape.check_template(&t), Ok(()));

    let ints = Shape::new().required("n", Ty::Int);
    assert_eq!(
        ints.check_template(&template(&json!({ "n": { "x": 1 } }))),
        Err(Misfit::WrongType {
            name: "n".into(),
            ty: Ty::Int,
            found: "a map".into()
        })
    );
    assert_eq!(
        ints.check_template(&template(&json!({ "n": [1] }))),
        Err(Misfit::WrongType {
            name: "n".into(),
            ty: Ty::Int,
            found: "a list".into()
        })
    );
}

#[test]
fn a_shape_fits_into_one_with_the_same_types_and_fields_it_need_not_have() {
    let have = Shape::new().required("n", Ty::Int).required("job", Ty::Id);
    let target = Shape::new()
        .required("n", Ty::Int)
        .required("job", Ty::Text)
        .optional("note", Ty::Text)
        .required("extra", Ty::Any);
    assert_eq!(have.fits_into(&target), Ok(()));
}

#[test]
fn a_shape_does_not_fit_where_a_type_differs_or_a_required_field_is_missing() {
    let job_id = Shape::new().required("job", Ty::Id);
    assert_eq!(
        job_id.fits_into(&Shape::new().required("job", Ty::Int)),
        Err(FitError::WrongType {
            name: "job".into(),
            have: Ty::Id,
            want: Ty::Int
        })
    );
    assert_eq!(
        Shape::new().fits_into(&Shape::new().required("n", Ty::Int)),
        Err(FitError::MissingField {
            name: "n".into(),
            ty: Ty::Int
        })
    );
}

#[test]
fn an_optional_field_fits_a_required_one_only_of_type_any() {
    let optional = Shape::new().optional("n", Ty::Int);
    assert_eq!(
        optional.fits_into(&Shape::new().required("n", Ty::Int)),
        Err(FitError::Optional { name: "n".into() })
    );
    assert_eq!(
        optional.fits_into(&Shape::new().required("n", Ty::Any)),
        Ok(())
    );
}
