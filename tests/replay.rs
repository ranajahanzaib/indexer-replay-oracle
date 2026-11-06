use iro::{
    config::Manifest,
    fixture::{self, Event},
    report, runner,
    schedule::{self, Op},
    snapshot::{self, Cell, Snapshot, TableState},
};
use proptest::prelude::*;
use std::{collections::BTreeMap, path::Path};
fn manifest(mode: &str) -> Manifest {
    let mut m = Manifest::load(Path::new(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/examples/manifests/correct.toml"
    )))
    .unwrap();
    m.consumer.command[0] = env!("CARGO_BIN_EXE_iro-example-consumer").into();
    m.consumer.command[3] = mode.into();
    m.testing.runs = 3;
    m
}
fn events() -> Vec<Event> {
    fixture::load(&manifest("correct")).unwrap()
}
#[test]
fn fixture_validation_and_identity() {
    let m = manifest("correct");
    let e = events();
    fixture::validate(&e, &m).unwrap();
    let mut changed = e[0].clone();
    changed.payload["amount"] = "999".into();
    assert_eq!(changed.id(), e[0].id());
    changed.log_index += 1;
    assert_ne!(changed.id(), e[0].id());
    let mut bad = e.clone();
    bad.push(e[0].clone());
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad[0].block_hash = "0xbad".into();
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad[1].block_hash = format!("0x{}", "ff".repeat(32));
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad.swap(0, 1);
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad[0].dependencies.push("missing".into());
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad[0].dependencies.push(e[1].id());
    bad[1].dependencies.push(e[0].id());
    assert!(schedule::edges(&bad, "conflict_keys").is_err());
    let mut bad = e.clone();
    bad[1].transaction_hash = bad[0].transaction_hash.clone();
    assert!(fixture::validate(&bad, &m).is_err());
    let mut bad = e.clone();
    bad[1].conflict_keys.clear();
    assert!(fixture::validate(&bad, &m).is_err());
}
#[test]
fn rejects_illegal_schedule() {
    let m = manifest("correct");
    let e = events();
    let mut s = schedule::canonical(&e, 42);
    s.operations.swap(0, 2);
    assert!(schedule::validate(&s, &e, &m).is_err());
    s.operations.pop();
    assert!(schedule::validate(&s, &e, &m).is_err());
}
proptest! {
 #[test] fn seeded_schedules_preserve_dag(seed in any::<u64>(),runs in 0usize..20){let mut m=manifest("correct");m.testing.seed=seed;let e=events();let a=schedule::generate(&e,&m,runs).unwrap();let b=schedule::generate(&e,&m,runs).unwrap();prop_assert_eq!(&a,&b);prop_assert!(schedule::validate(&a,&e,&m).is_ok());let ids:Vec<_>=e.iter().map(|e|e.id()).collect();for op in a.operations {if let Op::Apply{event,..}=op{prop_assert!(ids.contains(&event));}}}
 #[test] fn fingerprints_stable(value in any::<i64>()){let state=Snapshot{version:1,tables:BTreeMap::from([("x".into(),TableState{columns:vec!["id".into()],keys:vec!["id".into()],rows:vec![vec![Cell::Integer(value.to_string())]]})])};let encoded=serde_json::to_vec(&state).unwrap();let decoded:Snapshot=serde_json::from_slice(&encoded).unwrap();prop_assert_eq!(state.fingerprint(),decoded.fingerprint());prop_assert!(snapshot::differences(&state,&decoded).is_empty());}
}
#[tokio::test]
async fn correct_consumer_passes_all_faults() {
    let m = manifest("correct");
    let dir = tempfile::tempdir().unwrap();
    let r = report::verify(m, dir.path()).await.unwrap();
    assert_eq!(r.classification, "PASS", "{r:?}");
    assert!(r.scenarios.iter().any(|s| {
        s.execution
            .observations
            .iter()
            .any(|o| o.outcome == "exit_86")
    }));
}
#[tokio::test]
async fn buggy_failure_minimizes_and_reproduces() {
    let mut m = manifest("buggy");
    m.testing.runs = 1;
    let dir = tempfile::tempdir().unwrap();
    let r = report::verify(m, dir.path()).await.unwrap();
    assert_eq!(r.exit_code(), 1);
    let path = r.scenarios.iter().find_map(|s| s.bundle.as_ref()).unwrap();
    let b = report::Bundle::load(Path::new(path)).unwrap();
    assert_eq!(
        b.reproduce().await.unwrap().classification,
        "STATE_DIVERGENCE"
    );
    assert!(
        b.differences
            .iter()
            .any(|d| d.column == "balance" && d.numeric_delta.is_some())
    );
    let small = iro::minimize::minimize(&b).await.unwrap();
    assert!(small.events.len() < b.events.len());
    assert!(small.schedule.operations.len() < b.schedule.operations.len());
    assert_eq!(
        small.reproduce().await.unwrap().classification,
        "STATE_DIVERGENCE"
    );
    assert!(fixture::validate(&small.events, &small.manifest).is_ok());
    let out = dir.path().join("small");
    small.save(&out).unwrap();
    let loaded = report::Bundle::load(&out).unwrap();
    assert_eq!(
        loaded.reproduce().await.unwrap().classification,
        "STATE_DIVERGENCE"
    );
}
#[tokio::test]
async fn controlled_nondeterminism() {
    let mut m = manifest("nondeterministic");
    m.expected_state = None;
    let dir = tempfile::tempdir().unwrap();
    let r = report::verify(m, dir.path()).await.unwrap();
    assert_eq!(r.classification, "NONDETERMINISTIC_BASELINE");
}
#[tokio::test]
async fn malformed_protocol_and_exit() {
    for (mode, class) in [("malformed", "PROTOCOL_ERROR"), ("exit", "CONSUMER_CRASH")] {
        let m = manifest(mode);
        let e = events();
        let r = runner::execute(&m, &e, &schedule::canonical(&e, 42))
            .await
            .unwrap();
        assert_eq!(r.classification, class);
    }
}
#[tokio::test]
async fn golden_mismatch_is_separate() {
    let mut m = manifest("correct");
    let temp = tempfile::tempdir().unwrap();
    let mut golden: Snapshot =
        serde_json::from_slice(&std::fs::read(m.expected_state.as_ref().unwrap()).unwrap())
            .unwrap();
    golden.tables.get_mut("balances").unwrap().rows[0][1] = Cell::Text("999".into());
    let path = temp.path().join("golden.json");
    std::fs::write(&path, serde_json::to_vec(&golden).unwrap()).unwrap();
    m.expected_state = Some(path);
    let r = report::verify(m, temp.path()).await.unwrap();
    assert_eq!(r.classification, "EXPECTED_STATE_MISMATCH");
}
#[tokio::test]
async fn explicit_restarts_duplicates_crash() {
    let m = manifest("correct");
    let e = events();
    let baseline = runner::execute(&m, &e, &schedule::canonical(&e, 1))
        .await
        .unwrap()
        .snapshot
        .unwrap();
    let mut s = schedule::canonical(&e, 1);
    s.operations.insert(0, Op::Restart);
    s.operations.insert(2, Op::Restart);
    s.operations.push(Op::Apply {
        event: e[0].id(),
        crash: false,
    });
    s.operations.push(Op::Restart);
    s.operations.push(Op::Apply {
        event: e[0].id(),
        crash: true,
    });
    let actual = runner::execute(&m, &e, &s).await.unwrap();
    assert_eq!(actual.classification, "PASS");
    assert_eq!(actual.snapshot, Some(baseline));
}
#[tokio::test]
#[ignore = "requires IRO_POSTGRES_URL local server"]
async fn postgres_correct_and_buggy() {
    for mode in ["correct", "buggy"] {
        let mut m = manifest(mode);
        m.database.kind = "postgres".into();
        m.testing.runs = 2;
        let temp = tempfile::tempdir().unwrap();
        let r = report::verify(m, temp.path()).await.unwrap();
        assert_eq!(
            r.classification,
            if mode == "correct" {
                "PASS"
            } else {
                "STATE_DIVERGENCE"
            }
        );
    }
}

#[tokio::test]
async fn sqlite_snapshot_types_and_retrieval_order() {
    use sqlx::Connection;
    let mut m = manifest("correct");
    m.projection.tables = vec![iro::config::Table {
        name: "values_table".into(),
        columns: vec!["id".into(), "n".into(), "bytes".into(), "exact".into()],
        order_by: vec!["id".into()],
    }];
    let mut states = Vec::new();
    for reverse in [false, true] {
        let w = iro::database::Workspace::create(&m).await.unwrap();
        let mut c = sqlx::SqliteConnection::connect(&w.url).await.unwrap();
        sqlx::query(
            "CREATE TABLE values_table(id INTEGER PRIMARY KEY,n TEXT,bytes BLOB,exact TEXT)",
        )
        .execute(&mut c)
        .await
        .unwrap();
        let order = if reverse { vec![2, 1] } else { vec![1, 2] };
        for id in order {
            sqlx::query(
                "INSERT INTO values_table VALUES (?,NULL,x'00ff','123456789012345678901234567890')",
            )
            .bind(id)
            .execute(&mut c)
            .await
            .unwrap();
        }
        c.close().await.unwrap();
        let s = w.snapshot(&m.projection).await.unwrap();
        assert_eq!(s.tables["values_table"].rows[0][1], Cell::Null);
        assert_eq!(
            s.tables["values_table"].rows[0][2],
            Cell::Binary("00ff".into())
        );
        states.push(s);
        w.cleanup().await.unwrap();
    }
    assert_eq!(states[0], states[1]);
    assert_eq!(states[0].fingerprint(), states[1].fingerprint());
}
#[tokio::test]
async fn snapshot_rejects_ambiguous_keys_and_missing_columns() {
    use sqlx::Connection;
    let mut m = manifest("correct");
    let w = iro::database::Workspace::create(&m).await.unwrap();
    let mut c = sqlx::SqliteConnection::connect(&w.url).await.unwrap();
    sqlx::query("CREATE TABLE balances(account_id TEXT,balance TEXT)")
        .execute(&mut c)
        .await
        .unwrap();
    sqlx::query("INSERT INTO balances VALUES ('alice','1'),('alice','2')")
        .execute(&mut c)
        .await
        .unwrap();
    c.close().await.unwrap();
    assert!(w.snapshot(&m.projection).await.is_err());
    m.projection.tables[0].columns.push("missing".into());
    assert!(w.snapshot(&m.projection).await.is_err());
    w.cleanup().await.unwrap();
}
#[test]
fn dependency_removal_cannot_dangle() {
    let mut m = manifest("correct");
    let mut e = events();
    let parent = e[0].id();
    e[2].dependencies.push(parent);
    fixture::validate(&e, &m).unwrap();
    e.remove(0);
    assert!(fixture::validate(&e, &m).is_err());
    m.ordering.mode = "strict".into();
    assert!(fixture::validate(&e, &m).is_err());
}
#[test]
fn manifest_unknown_options_and_unsafe_postgres_rejected() {
    let text = std::fs::read_to_string("examples/manifests/correct.toml").unwrap();
    assert!(toml::from_str::<Manifest>(&text.replace("seed = 42", "seed = 42\nsede = 5")).is_err());
    let mut m = manifest("correct");
    m.projection.tables[0].name = "balances; DROP TABLE users".into();
    assert!(m.validate().is_err());
}
#[test]
fn golden_fixture_and_identity_vectors() {
    let e = events();
    assert_eq!(
        fixture::fingerprint(&e),
        "0927cfdfa982d4dfb2b9897f77f5552597fc7b79ade806dfa1b7eaeffb3ceaed"
    );
    assert_eq!(
        e[0].id(),
        "7006ee92bb5ed10d0cf6e2ce23ac9d1dafaf93aba85f991df149da32ba3d2fee"
    );
}
#[tokio::test]
#[ignore = "requires IRO_POSTGRES_URL local server"]
async fn postgres_isolation_cleanup_and_types() {
    use sqlx::{Connection, Row};
    let mut m = manifest("correct");
    m.database.kind = "postgres".into();
    let a = iro::database::Workspace::create(&m).await.unwrap();
    let b = iro::database::Workspace::create(&m).await.unwrap();
    let sentinel = iro::database::Workspace::create(&m).await.unwrap();
    assert_ne!(a.schema, b.schema);
    let url = m.postgres_url().unwrap();
    let mut admin = sqlx::PgConnection::connect(&url).await.unwrap();
    let before: Vec<String> =
        sqlx::query_scalar("SELECT nspname::text FROM pg_namespace ORDER BY nspname")
            .fetch_all(&mut admin)
            .await
            .unwrap();
    for (w, amount) in [(&a, "10"), (&b, "20")] {
        let mut c = iro::database::postgres_consumer_connect(&url, w.schema.as_deref().unwrap())
            .await
            .unwrap();
        sqlx::query("CREATE TABLE balances(account_id TEXT PRIMARY KEY,balance TEXT)")
            .execute(&mut c)
            .await
            .unwrap();
        sqlx::query("INSERT INTO balances VALUES ('alice',$1)")
            .bind(amount)
            .execute(&mut c)
            .await
            .unwrap();
        let schema: String = sqlx::query("SELECT current_schema()")
            .fetch_one(&mut c)
            .await
            .unwrap()
            .get(0);
        assert_eq!(Some(schema), w.schema);
        c.close().await.unwrap();
    }
    assert_ne!(
        a.snapshot(&m.projection).await.unwrap(),
        b.snapshot(&m.projection).await.unwrap()
    );
    a.cleanup().await.unwrap();
    b.cleanup().await.unwrap();
    let after: Vec<String> =
        sqlx::query_scalar("SELECT nspname::text FROM pg_namespace ORDER BY nspname")
            .fetch_all(&mut admin)
            .await
            .unwrap();
    for name in before {
        // Other tests may concurrently create/drop their own iro_ workspaces.
        if !name.starts_with("iro_") {
            assert!(after.contains(&name));
        }
    }
    assert!(!after.contains(a.schema.as_ref().unwrap()));
    assert!(!after.contains(b.schema.as_ref().unwrap()));
    assert!(after.contains(sentinel.schema.as_ref().unwrap()));
    sentinel.cleanup().await.unwrap();
    admin.close().await.unwrap();
}

#[tokio::test]
async fn protocol_limits_version_ids_and_timeout() {
    for mode in ["wrongid", "wrongversion", "oversized", "timeout"] {
        let mut m = manifest(mode);
        m.consumer.startup_timeout_seconds = 1;
        m.consumer.max_message_bytes = 2048;
        let e = events();
        let r = runner::execute(&m, &e, &schedule::canonical(&e, 1))
            .await
            .unwrap();
        assert_eq!(
            r.classification,
            if mode == "timeout" {
                "TIMEOUT"
            } else {
                "PROTOCOL_ERROR"
            }
        );
    }
}
#[tokio::test]
async fn missing_required_fault_is_not_success() {
    let mut m = manifest("nohook");
    m.testing.runs = 1;
    let dir = tempfile::tempdir().unwrap();
    let r = report::verify(m, dir.path()).await.unwrap();
    assert_eq!(r.exit_code(), 2);
    assert!(r.unsupported > 0);
    assert!(
        r.scenarios
            .iter()
            .any(|s| s.schedule.id == "duplicate-delivery" && s.execution.classification == "PASS")
    );
}
#[tokio::test]
async fn handler_errors_remain_distinct_from_divergence() {
    let m = manifest("correct");
    let mut e = events();
    e[0].payload["amount"] = "-1".into();
    let r = runner::execute(&m, &e, &schedule::canonical(&e, 1))
        .await
        .unwrap();
    assert_eq!(r.classification, "HANDLER_ERROR");
    assert!(r.snapshot.is_none());
}
#[tokio::test]
async fn report_json_markdown_and_junit_are_real_outputs() {
    let mut m = manifest("correct");
    m.testing.runs = 1;
    let dir = tempfile::tempdir().unwrap();
    let r = report::verify(m, dir.path()).await.unwrap();
    let xml = dir.path().join("junit.xml");
    r.write(dir.path(), Some(&xml)).unwrap();
    let decoded: report::Report =
        serde_json::from_slice(&std::fs::read(dir.path().join("report.json")).unwrap()).unwrap();
    assert_eq!(decoded.classification, "PASS");
    assert_eq!(decoded.scenarios.len(), r.scenarios.len());
    assert!(
        std::fs::read_to_string(xml)
            .unwrap()
            .contains("failures=\"0\"")
    );
    assert!(dir.path().join("report.md").exists());
}

proptest! {
    #[test]
    fn arbitrary_conflict_graphs_preserve_first_delivery_constraints(keys in prop::collection::vec(0u8..4,1..25),seed in any::<u64>()) {
        let template=events()[0].clone();let mut source=Vec::new();
        for (i,key) in keys.iter().enumerate(){let mut e=template.clone();e.transaction_hash=format!("0x{:064x}",i+1);e.transaction_index=i as u64;e.log_index=i as u64;e.conflict_keys=vec![format!("account:{key}")];if i>0&&i%3==0 {e.dependencies=vec![source.last().map(Event::id).unwrap()];}source.push(e);}
        let mut m=manifest("correct");m.testing.seed=seed;fixture::validate(&source,&m).unwrap();let s=schedule::generate(&source,&m,0).unwrap();
        let first:Vec<_>=source.iter().map(|e|s.operations.iter().position(|op|matches!(op,Op::Apply{event,..} if event==&e.id())).unwrap()).collect();
        for i in 0..keys.len(){for j in i+1..keys.len(){if keys[i]==keys[j]{prop_assert!(first[i]<first[j]);}}if i>0&&i%3==0{prop_assert!(first[i-1]<first[i]);}}
    }
}
