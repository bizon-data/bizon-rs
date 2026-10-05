//! bizon's `_ensure_table`: create the table with partitioning and clustering, or, if it exists,
//! append missing columns (never change types, partitioning or clustering).

use serde_json::{json, Value};

use super::rest::{BigQueryRest, RestError};
use super::write::TableRef;
use crate::config::{RecordSchema, TimePartitioning};

const PARTITIONABLE: &[&str] = &["TIMESTAMP", "DATE", "DATETIME"];

#[derive(Debug, PartialEq, Eq)]
pub enum Ensured {
    Created,
    Unchanged,
    /// Columns were appended; appends may see INVALID_ARGUMENT until the new schema propagates.
    AddedColumns(Vec<String>),
}

fn fields(schema: &RecordSchema) -> Vec<Value> {
    schema
        .record_schema
        .iter()
        .map(|c| {
            let mut f = json!({"name": c.name, "type": c.bq_type, "mode": c.mode});
            if let Some(d) = &c.description {
                f["description"] = json!(d);
            }
            if let Some(d) = &c.default_value_expression {
                f["defaultValueExpression"] = json!(d);
            }
            f
        })
        .collect()
}

/// Why the configured partitioning cannot apply to this schema, if it cannot.
fn partition_problem(tp: &TimePartitioning, schema: &RecordSchema) -> Option<String> {
    let field = tp.field.as_deref()?;
    match schema.record_schema.iter().find(|c| c.name == field) {
        None => Some(format!("partition field {field} is not in the schema")),
        Some(c) if !PARTITIONABLE.contains(&c.bq_type.as_str()) => Some(format!("partition field {field} is {}", c.bq_type)),
        Some(c) if tp.kind == "HOUR" && c.bq_type == "DATE" => Some(format!("HOUR partitioning on DATE field {field}")),
        _ => None,
    }
}

pub fn table_body(t: &TableRef, schema: &RecordSchema, partitioning: Option<&TimePartitioning>) -> Result<Value, String> {
    let mut body = json!({
        "tableReference": {"projectId": t.project, "datasetId": t.dataset, "tableId": t.table},
        "schema": {"fields": fields(schema)},
    });
    if let Some(tp) = partitioning {
        if let Some(problem) = partition_problem(tp, schema) {
            return Err(format!(
                "cannot create {}.{}.{} with the configured partitioning: {problem}",
                t.project, t.dataset, t.table
            ));
        }
        let mut p = json!({"type": tp.kind});
        if let Some(f) = &tp.field {
            p["field"] = json!(f);
        }
        body["timePartitioning"] = p;
    }
    if !schema.clustering_keys.is_empty() {
        body["clustering"] = json!({"fields": schema.clustering_keys});
    }
    Ok(body)
}

pub async fn ensure_table(
    rest: &BigQueryRest,
    t: &TableRef,
    schema: &RecordSchema,
    partitioning: Option<&TimePartitioning>,
) -> Result<Ensured, anyhow::Error> {
    // An unusable partition field only matters when creating; an existing table keeps its own spec.
    let body = match table_body(t, schema, partitioning) {
        Ok(b) => b,
        Err(problem) => match rest.get_table(t).await {
            Ok(_) => table_body(t, schema, None).map_err(anyhow::Error::msg)?,
            Err(e) if e.status() == Some(404) => anyhow::bail!(problem),
            Err(e) => return Err(e.into()),
        },
    };
    match rest.insert_table(t, &body).await {
        Ok(_) => return Ok(Ensured::Created),
        Err(RestError::Status { status: 409, .. }) => {}
        Err(e) => return Err(e.into()),
    }
    let existing = rest.get_table(t).await?;
    let mut current: Vec<Value> = existing["schema"]["fields"].as_array().cloned().unwrap_or_default();
    let missing: Vec<Value> = fields(schema)
        .into_iter()
        .filter(|f| !current.iter().any(|c| c["name"] == f["name"]))
        .collect();
    if missing.is_empty() {
        return Ok(Ensured::Unchanged);
    }
    let names = missing.iter().map(|f| f["name"].as_str().unwrap_or_default().to_string()).collect();
    tracing::warn!(table = %t.table, added = ?names, "adding new fields to table schema");
    current.extend(missing);
    rest.patch_schema(t, &current).await?;
    Ok(Ensured::AddedColumns(names))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::SchemaColumn;

    fn col(name: &str, t: &str, mode: &str) -> SchemaColumn {
        serde_yaml::from_str(&format!("{{name: {name}, type: {t}, mode: {mode}}}")).unwrap()
    }

    fn schema() -> RecordSchema {
        let mut inserted: SchemaColumn = col("__inserted_at", "TIMESTAMP", "NULLABLE");
        inserted.default_value_expression = Some("CURRENT_TIMESTAMP()".into());
        RecordSchema {
            destination_id: "p.d.t".into(),
            record_schema: vec![col("id", "INTEGER", "REQUIRED"), col("payload", "JSON", "NULLABLE"), inserted],
            clustering_keys: vec!["id".into()],
        }
    }

    #[test]
    fn create_body_has_partitioning_clustering_and_defaults() {
        let tp = TimePartitioning {
            kind: "DAY".into(),
            field: Some("__inserted_at".into()),
        };
        let t = TableRef::parse("p.d.t").unwrap();
        let b = table_body(&t, &schema(), Some(&tp)).unwrap();
        assert_eq!(b["timePartitioning"], json!({"type": "DAY", "field": "__inserted_at"}));
        assert_eq!(b["clustering"], json!({"fields": ["id"]}));
        assert_eq!(b["schema"]["fields"][2]["defaultValueExpression"], "CURRENT_TIMESTAMP()");
        assert_eq!(b["schema"]["fields"][0]["mode"], "REQUIRED");
    }

    #[test]
    fn unusable_partition_field_is_reported() {
        let t = TableRef::parse("p.d.t").unwrap();
        let tp = TimePartitioning {
            kind: "DAY".into(),
            field: Some("payload".into()),
        };
        assert!(table_body(&t, &schema(), Some(&tp)).unwrap_err().contains("payload is JSON"));
        let ingestion = TimePartitioning {
            kind: "DAY".into(),
            field: None,
        };
        assert_eq!(
            table_body(&t, &schema(), Some(&ingestion)).unwrap()["timePartitioning"],
            json!({"type": "DAY"})
        );
    }
}
