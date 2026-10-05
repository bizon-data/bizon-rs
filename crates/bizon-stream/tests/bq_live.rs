//! Live BigQuery checks for table creation and the load-job path. Opt-in:
//! `BIZON_RS_LIVE_DATASET=project.dataset cargo test --test bq_live -- --ignored`

use bizon_stream::bq::ensure_table::{ensure_table, Ensured};
use bizon_stream::bq::rest::BigQueryRest;
use bizon_stream::bq::write::TableRef;
use bizon_stream::config::{RecordSchema, TimePartitioning};
use serde_json::json;

fn schema(extra: bool) -> RecordSchema {
    let mut yaml = String::from(
        "destination_id: x\nclustering_keys: [id]\nrecord_schema:\n\
         - {name: id, type: INTEGER, mode: REQUIRED}\n\
         - {name: payload, type: JSON, mode: NULLABLE}\n\
         - {name: __inserted_at, type: TIMESTAMP, mode: NULLABLE, default_value_expression: CURRENT_TIMESTAMP()}\n",
    );
    if extra {
        yaml.push_str("- {name: added_later, type: STRING, mode: NULLABLE}\n");
    }
    serde_yaml::from_str(&yaml).unwrap()
}

#[tokio::test]
#[ignore]
async fn create_reconcile_and_load() {
    let Ok(dataset) = std::env::var("BIZON_RS_LIVE_DATASET") else {
        return;
    };
    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let t = TableRef::parse(&format!("{dataset}.ensure_{suffix}")).unwrap();
    let rest = BigQueryRest::new(None).await.unwrap();
    let tp = TimePartitioning {
        kind: "DAY".into(),
        field: Some("__inserted_at".into()),
    };

    assert_eq!(ensure_table(&rest, &t, &schema(false), Some(&tp)).await.unwrap(), Ensured::Created);
    assert_eq!(
        ensure_table(&rest, &t, &schema(false), Some(&tp)).await.unwrap(),
        Ensured::Unchanged
    );
    assert_eq!(
        ensure_table(&rest, &t, &schema(true), Some(&tp)).await.unwrap(),
        Ensured::AddedColumns(vec!["added_later".into()])
    );
    let table = rest.get_table(&t).await.unwrap();
    assert_eq!(table["timePartitioning"]["field"], "__inserted_at");
    assert_eq!(table["clustering"]["fields"], json!(["id"]));

    let big = "x".repeat(9 * 1024 * 1024);
    let row = json!({"id": 1, "payload": {"big": big}, "__inserted_at": "2026-01-01T00:00:00.123456", "unknown": 1});
    let mut ndjson = serde_json::to_vec(&row).unwrap();
    ndjson.push(b'\n');
    let fields = table["schema"]["fields"].as_array().unwrap().clone();
    rest.load_ndjson(&t, "US", &fields, ndjson).await.unwrap();
    let after = rest.get_table(&t).await.unwrap();
    eprintln!("table {} rows after load: {}", t.table, after["numRows"]);
}
