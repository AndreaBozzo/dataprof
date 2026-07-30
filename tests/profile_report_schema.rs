use std::io::Write;

use dataprof::{
    DataFrameLibrary, DataSource, ExecutionMetadata, ProfileReport, Profiler, QueryEngine,
    REPORT_SCHEMA_VERSION,
};
use dataprof_core::StreamSourceSystem;
use serde_json::{Value, json};

fn committed_schema() -> Value {
    serde_json::from_str(include_str!("../docs/schema/profile-report.v1.schema.json"))
        .expect("committed profile report schema must be valid JSON")
}

fn validator(schema: &Value) -> jsonschema::Validator {
    jsonschema::draft202012::options()
        .build(schema)
        .expect("committed profile report schema must compile without remote references")
}

fn assert_valid(schema: &Value, instance: &Value) {
    let errors = validator(schema)
        .iter_errors(instance)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    assert!(
        errors.is_empty(),
        "report did not validate against the committed schema:\n{}",
        errors.join("\n")
    );
}

fn minimal_v1_document() -> Value {
    json!({
        "schema_version": REPORT_SCHEMA_VERSION,
        "id": "schema-test",
        "timestamp": "2026-07-30T12:00:00Z",
        "data_source": {
            "type": "file",
            "path": "sample.csv",
            "format": "csv",
            "size_bytes": 12
        },
        "column_profiles": [],
        "execution": {
            "rows_processed": 1,
            "columns_detected": 1,
            "scan_time_ms": 1,
            "error_count": 0,
            "source_exhausted": true,
            "sampling_applied": false
        }
    })
}

#[test]
fn committed_schema_is_current_and_valid_draft_2020_12() {
    let committed = committed_schema();
    let generated = dataprof_runtime::profile_report_schema_document();

    assert_eq!(
        committed, generated,
        "schema drift detected; run `cargo run --example generate_profile_schema`"
    );
    jsonschema::draft202012::meta::validate(&committed)
        .expect("generated document must validate against the Draft 2020-12 meta-schema");
    assert_eq!(
        committed["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
    assert_eq!(
        committed["$id"],
        "https://andreabozzo.github.io/dataprof/schema/profile-report.v1.schema.json"
    );
}

#[test]
fn schema_references_are_self_contained_and_objects_allow_additive_fields() {
    fn inspect(value: &Value) {
        match value {
            Value::Object(object) => {
                if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                    assert!(
                        reference.starts_with("#/$defs/"),
                        "external schema reference is not allowed: {reference}"
                    );
                }
                assert_ne!(
                    object.get("additionalProperties"),
                    Some(&Value::Bool(false)),
                    "v1 objects must accept additive fields"
                );
                object.values().for_each(inspect);
            }
            Value::Array(items) => items.iter().for_each(inspect),
            _ => {}
        }
    }

    inspect(&committed_schema());

    let mut additive = minimal_v1_document();
    additive["future_top_level_field"] = json!({"enabled": true});
    additive["data_source"]["future_source_field"] = json!("preserved by newer writers");
    additive["execution"]["future_execution_field"] = json!(42);
    assert_valid(&committed_schema(), &additive);
}

#[test]
fn real_file_entry_paths_produce_schema_valid_reports() {
    let schema = committed_schema();
    let cases = [
        (".csv", "name,amount\nAlice,10\nBob,20\n"),
        (
            ".json",
            r#"[{"name":"Alice","amount":10},{"name":"Bob","amount":20}]"#,
        ),
        (
            ".jsonl",
            "{\"name\":\"Alice\",\"amount\":10}\n{\"name\":\"Bob\",\"amount\":20}\n",
        ),
    ];

    for (suffix, contents) in cases {
        let mut source = tempfile::Builder::new()
            .suffix(suffix)
            .tempfile()
            .expect("create schema test source");
        source
            .write_all(contents.as_bytes())
            .expect("write schema test source");
        source.flush().expect("flush schema test source");

        let report = Profiler::new()
            .analyze_file(source.path())
            .unwrap_or_else(|error| panic!("profile {suffix} source: {error}"));
        let serialized = serde_json::to_value(report).expect("serialize profile report");
        assert_valid(&schema, &serialized);
    }
}

#[cfg(feature = "parquet")]
#[test]
fn parquet_entry_path_produces_a_schema_valid_report() {
    let report = Profiler::new()
        .analyze_file("examples/test_data/sensors.parquet")
        .expect("profile Parquet fixture");
    assert_valid(
        &committed_schema(),
        &serde_json::to_value(report).expect("serialize Parquet report"),
    );
}

#[test]
fn every_data_source_variant_is_schema_valid() {
    let schema = committed_schema();
    let sources = [
        DataSource::Query {
            engine: QueryEngine::Sqlite,
            statement: "select 1".to_string(),
            database: Some("memory".to_string()),
            execution_id: None,
        },
        DataSource::DataFrame {
            name: "frame".to_string(),
            source_library: DataFrameLibrary::Pandas,
            row_count: 1,
            column_count: 1,
            memory_bytes: Some(8),
        },
        DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "batch-1".to_string(),
            partition: Some(0),
            consumer_group: None,
            source_system: StreamSourceSystem::Kafka,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        },
    ];

    for source in sources {
        let report = ProfileReport::new(source, vec![], ExecutionMetadata::new(1, 0, 1), None);
        assert_valid(
            &schema,
            &serde_json::to_value(report).expect("serialize source report"),
        );
    }
}

#[test]
fn legacy_v1_defaults_validate_and_deserialize() {
    let legacy = minimal_v1_document();
    assert_valid(&committed_schema(), &legacy);

    let report: ProfileReport =
        serde_json::from_value(legacy).expect("legacy v1 report must remain readable");
    assert_eq!(report.execution.ragged_row_count, 0);
    assert!(report.semantic_hint_bindings.is_empty());
}

#[test]
fn schema_rejects_wrong_versions_missing_fields_bad_enums_and_bad_primitives() {
    let schema = committed_schema();
    let validator = validator(&schema);

    let mut wrong_version = minimal_v1_document();
    wrong_version["schema_version"] = json!(REPORT_SCHEMA_VERSION + 1);
    assert!(!validator.is_valid(&wrong_version));

    let mut missing_required = minimal_v1_document();
    missing_required
        .as_object_mut()
        .expect("object")
        .remove("id");
    assert!(!validator.is_valid(&missing_required));

    let mut bad_enum = minimal_v1_document();
    bad_enum["data_source"]["type"] = json!("socket");
    assert!(!validator.is_valid(&bad_enum));

    let mut bad_primitive = minimal_v1_document();
    bad_primitive["execution"]["rows_processed"] = json!("one");
    assert!(!validator.is_valid(&bad_primitive));
}
