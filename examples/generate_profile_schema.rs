//! Regenerate the committed JSON Schema for serialized profile reports.
//!
//! Run from the workspace root:
//!
//! ```text
//! cargo run --example generate_profile_schema
//! ```

use std::fs;
use std::path::PathBuf;

use dataprof_runtime::{REPORT_SCHEMA_VERSION, profile_report_schema_document};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let output = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("docs")
        .join("schema")
        .join(format!(
            "profile-report.v{REPORT_SCHEMA_VERSION}.schema.json"
        ));
    let document = profile_report_schema_document();
    let mut json = serde_json::to_string_pretty(&document)?;
    json.push('\n');

    let parent = output
        .parent()
        .expect("profile report schema output must have a parent directory");
    fs::create_dir_all(parent)?;
    fs::write(&output, json)?;
    println!("wrote {}", output.display());
    Ok(())
}
