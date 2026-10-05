use std::collections::HashSet;
use std::time::Duration;

use bizon_stream::bq::write::{AppendError, TableRef, TableWriter, WriteClient, WriterOptions};
use bizon_stream::proto::descriptor::{Column, TableDescriptor};
use bizon_stream::proto::encode::{encode_row, Value};
use bytes::Bytes;
use fake_bqwrite::{Behaviour, FakeWrite};
use tonic::Code;

fn descriptor() -> TableDescriptor {
    TableDescriptor::new(&[Column {
        name: "id".into(),
        bq_type: "INTEGER".into(),
        required: true,
    }])
    .unwrap()
}

fn batch(desc: &TableDescriptor, start: i64, n: i64) -> Vec<Bytes> {
    (start..start + n)
        .map(|i| {
            let mut out = Vec::new();
            encode_row(desc, [("id", Value::Int(i))], &mut out).unwrap();
            Bytes::from(out)
        })
        .collect()
}

fn fast_opts() -> WriterOptions {
    WriterOptions {
        backoff_initial: Duration::from_millis(5),
        backoff_max: Duration::from_millis(20),
        retry_budget: Duration::from_secs(5),
        idle_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}

async fn setup(behaviour: Behaviour, opts: WriterOptions) -> (FakeWrite, TableWriter, TableDescriptor) {
    let (fake, addr) = FakeWrite::start(
        Behaviour {
            retain_rows: true,
            ..behaviour
        },
        "127.0.0.1:0".parse().unwrap(),
    )
    .await
    .unwrap();
    let client = WriteClient::connect(Some(&format!("http://{addr}")), None).await.unwrap();
    let desc = descriptor();
    let writer = client.table_writer(&TableRef::parse("p.d.t").unwrap(), desc.proto_schema.clone(), opts);
    (fake, writer, desc)
}

/// Appends `batches` × `per` rows with pipelining and returns the per-batch results.
async fn run(writer: &TableWriter, desc: &TableDescriptor, batches: i64, per: i64) -> Vec<Result<(), AppendError>> {
    let mut acks = Vec::new();
    for b in 0..batches {
        acks.push(writer.append(batch(desc, b * per, per)).await.unwrap());
    }
    let mut out = Vec::new();
    for a in acks {
        out.push(a.await.unwrap());
    }
    out
}

fn ids(fake: &FakeWrite) -> HashSet<Vec<u8>> {
    fake.recorded().acked_rows.iter().map(|b| b.to_vec()).collect()
}

#[tokio::test]
async fn pipelines_on_one_connection() {
    let (fake, writer, desc) = setup(Behaviour::default(), fast_opts()).await;
    let results = run(&writer, &desc, 20, 500).await;
    assert!(results.iter().all(|r| r.is_ok()));
    let r = fake.recorded();
    assert_eq!(r.connections, 1);
    assert_eq!(r.requests, 20);
    assert_eq!(r.acked_rows.len(), 10_000);
    assert!(r.violations.is_empty(), "{:?}", r.violations);
}

#[tokio::test]
async fn reconnects_and_resends_when_server_closes_streams() {
    let behaviour = Behaviour {
        close_after: Some(3),
        ..Default::default()
    };
    let (fake, writer, desc) = setup(behaviour, fast_opts()).await;
    let results = run(&writer, &desc, 20, 500).await;
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    assert_eq!(ids(&fake).len(), 10_000, "every row acked at least once");
    let r = fake.recorded();
    assert!(r.connections >= 7);
    assert!(r.violations.is_empty(), "{:?}", r.violations);
}

#[tokio::test]
async fn retries_transient_errors_in_responses() {
    let behaviour = Behaviour {
        error_every: Some((7, Code::Unavailable, "try again".into())),
        ..Default::default()
    };
    let (fake, writer, desc) = setup(behaviour, fast_opts()).await;
    let results = run(&writer, &desc, 20, 100).await;
    assert!(results.iter().all(|r| r.is_ok()), "{results:?}");
    assert_eq!(ids(&fake).len(), 2_000);
    assert!(fake.recorded().violations.is_empty());
}

#[tokio::test]
async fn row_errors_are_fatal_for_that_request_only() {
    let behaviour = Behaviour {
        row_error_every: Some(5),
        ..Default::default()
    };
    let (_fake, writer, desc) = setup(behaviour, fast_opts()).await;
    let results = run(&writer, &desc, 4, 10).await;
    assert!(results.iter().all(|r| r.is_ok()));
    let results = run(&writer, &desc, 1, 10).await;
    assert!(matches!(results[0], Err(AppendError::RowErrors { count: 1, .. })), "{results:?}");
}

#[tokio::test]
async fn invalid_argument_is_fatal_unless_schema_just_changed() {
    let behaviour = Behaviour {
        error_every: Some((1, Code::InvalidArgument, "Input schema has more fields than BigQuery schema".into())),
        ..Default::default()
    };
    let (fake, writer, desc) = setup(behaviour, fast_opts()).await;
    let results = run(&writer, &desc, 1, 10).await;
    assert!(
        matches!(
            &results[0],
            Err(AppendError::Rejected {
                code: Code::InvalidArgument,
                ..
            })
        ),
        "{results:?}"
    );

    writer.notify_schema_change();
    let ack = writer.append(batch(&desc, 0, 10)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    fake.behaviour.lock().unwrap().error_every = None;
    assert!(ack.await.unwrap().is_ok(), "retried until the schema propagated");
}

#[tokio::test]
async fn gives_up_after_retry_budget() {
    let behaviour = Behaviour {
        error_every: Some((1, Code::Unavailable, "down".into())),
        ..Default::default()
    };
    let opts = WriterOptions {
        retry_budget: Duration::from_millis(100),
        ..fast_opts()
    };
    let (_fake, writer, desc) = setup(behaviour, opts).await;
    let results = run(&writer, &desc, 2, 10).await;
    assert!(
        results.iter().all(|r| matches!(r, Err(AppendError::RetriesExhausted { .. }))),
        "{results:?}"
    );
}

#[tokio::test]
async fn idle_connection_is_closed_and_reopened() {
    let (fake, writer, desc) = setup(Behaviour::default(), fast_opts()).await;
    run(&writer, &desc, 1, 10).await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    run(&writer, &desc, 1, 10).await;
    let r = fake.recorded();
    assert_eq!(r.connections, 2);
    assert!(r.violations.is_empty(), "{:?}", r.violations);
}
