//! Engine schema is standard Apache Arrow schema.
//!
//! Mapping to and from Arrow is identity after a supported-type check, so
//! operators never introduce a private memory layout.

use arrow::datatypes::{DataType, Field, SchemaRef};

use crate::error::{Error, Result};

/// Map an engine schema to a standard Arrow schema.
///
/// The engine already stores Arrow schema, so this validates that every field
/// uses a supported Arrow type and returns the same schema object.
pub fn to_arrow_schema(schema: SchemaRef) -> Result<SchemaRef> {
    validate_schema(&schema)?;
    Ok(schema)
}

/// Map a standard Arrow schema to an engine schema.
///
/// Same contract as [`to_arrow_schema`]: validate, then keep the Arrow schema.
pub fn from_arrow_schema(schema: SchemaRef) -> Result<SchemaRef> {
    validate_schema(&schema)?;
    Ok(schema)
}

/// Whether `dt` is a type the engine will hold as Arrow memory.
pub fn is_supported_data_type(dt: &DataType) -> bool {
    validate_data_type(dt).is_ok()
}

pub(crate) fn validate_schema(schema: &arrow::datatypes::Schema) -> Result<()> {
    for field in schema.fields() {
        validate_field(field)?;
    }
    Ok(())
}

fn validate_field(field: &Field) -> Result<()> {
    validate_data_type(field.data_type())
}

fn validate_data_type(dt: &DataType) -> Result<()> {
    match dt {
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_)
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Interval(_) => Ok(()),
        DataType::List(field)
        | DataType::LargeList(field)
        | DataType::ListView(field)
        | DataType::LargeListView(field)
        | DataType::FixedSizeList(field, _)
        | DataType::Map(field, _) => validate_field(field),
        DataType::Struct(fields) => {
            for field in fields {
                validate_field(field)?;
            }
            Ok(())
        }
        DataType::Dictionary(key, value) => {
            if !key.is_integer() {
                return Err(Error::UnsupportedType(dt.to_string()));
            }
            validate_data_type(value)
        }
        other => Err(Error::UnsupportedType(other.to_string())),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use arrow::datatypes::{Field, Fields, Schema, TimeUnit};

    use super::*;

    fn schema(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn round_trip(s: SchemaRef) -> SchemaRef {
        let arrow = to_arrow_schema(s).expect("to_arrow_schema");
        from_arrow_schema(arrow).expect("from_arrow_schema")
    }

    #[test]
    fn primitive_and_nested_schemas_round_trip_as_arrow() {
        let nested = schema(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("flag", DataType::Boolean, true),
            Field::new("amount32", DataType::Decimal32(9, 2), true),
            Field::new("amount64", DataType::Decimal64(18, 4), true),
            Field::new("amount", DataType::Decimal128(38, 10), true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new(
                "ts_ntz",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                true,
            ),
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
                true,
            ),
            Field::new(
                "point",
                DataType::Struct(Fields::from(vec![
                    Field::new("x", DataType::Float64, true),
                    Field::new("y", DataType::Float64, true),
                ])),
                true,
            ),
            Field::new(
                "props",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            ),
        ]);

        let out = round_trip(nested.clone());
        assert_eq!(nested.as_ref(), out.as_ref());
        assert!(Arc::ptr_eq(&nested, &out));
    }

    #[test]
    fn schema_metadata_round_trips() {
        let mut metadata = HashMap::new();
        metadata.insert("owner".to_string(), "engine".to_string());
        let s: SchemaRef = Arc::new(Schema::new_with_metadata(
            vec![Field::new("id", DataType::Int64, false)],
            metadata,
        ));
        let out = round_trip(s.clone());
        assert_eq!(s.as_ref(), out.as_ref());
    }

    #[test]
    fn unsupported_arrow_type_is_rejected() {
        let schema = schema(vec![Field::new(
            "runs",
            DataType::RunEndEncoded(
                Arc::new(Field::new("run_ends", DataType::Int32, false)),
                Arc::new(Field::new("values", DataType::Int64, true)),
            ),
            true,
        )]);
        let err = from_arrow_schema(schema).expect_err("RunEndEncoded is unsupported");
        match err {
            Error::UnsupportedType(msg) => assert!(
                msg.contains("RunEndEncoded"),
                "error should name the type, got {msg}"
            ),
            other => panic!("expected UnsupportedType, got {other:?}"),
        }
    }

    #[test]
    fn nested_unsupported_type_is_rejected() {
        let schema = schema(vec![Field::new(
            "nested",
            DataType::List(Arc::new(Field::new(
                "item",
                DataType::RunEndEncoded(
                    Arc::new(Field::new("run_ends", DataType::Int32, false)),
                    Arc::new(Field::new("values", DataType::Int64, true)),
                ),
                true,
            ))),
            true,
        )]);
        assert!(matches!(
            to_arrow_schema(schema),
            Err(Error::UnsupportedType(_))
        ));
    }
}
