//! One Billion Row Challenge (1BRC) solver using dataprof's DataFusion engine.
//!
//! This example reads a semicolon-delimited, headerless CSV file of weather
//! station measurements and outputs the result in the canonical 1BRC format:
//!
//!   {station1=min/mean/max, station2=min/mean/max, ...}
//!
//! # Usage
//!
//! ```bash
//! cargo run --release --features datafusion --example one_billion_rows -- measurements.txt
//! ```
//!
//! The input file is expected to have lines like:
//!   Hamburg;12.0
//!   Bulawayo;8.9

use anyhow::Result;
use arrow::array::{AsArray, Float64Array};
use arrow::datatypes::{DataType, Field, Schema};
use dataprof::engines::{CsvReadOptions, DataFusionLoader};
use std::env;
use std::time::Instant;

#[tokio::main]
async fn main() -> Result<()> {
    let path = env::args()
        .nth(1)
        .unwrap_or_else(|| "measurements.txt".to_string());

    eprintln!("1BRC — reading {}", path);
    let start = Instant::now();

    // Initialise with a large batch size for throughput
    let loader = DataFusionLoader::new().with_batch_size(65536);

    // Register the 1BRC file: semicolon delimiter, no header, explicit schema
    let schema = Schema::new(vec![
        Field::new("station", DataType::Utf8, false),
        Field::new("temperature", DataType::Float64, false),
    ]);
    let options = CsvReadOptions::default()
        .delimiter(b';')
        .has_header(false)
        .schema(&schema);
    loader
        .register_csv_with_options("measurements", &path, options)
        .await?;

    // Run the aggregation — DataFusion parallelises this automatically
    let query = "
        SELECT
            station,
            MIN(temperature) AS min_temp,
            AVG(temperature) AS mean_temp,
            MAX(temperature) AS max_temp
        FROM measurements
        GROUP BY station
        ORDER BY station
    ";

    let batches = loader.execute_sql(query).await?;

    // Format output: {station=min/mean/max, ...}
    let mut entries: Vec<String> = Vec::new();

    for batch in &batches {
        let stations = batch.column(0).as_string::<i32>();
        let mins = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("min_temp column");
        let means = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("mean_temp column");
        let maxs = batch
            .column(3)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("max_temp column");

        for i in 0..batch.num_rows() {
            entries.push(format!(
                "{}={:.1}/{:.1}/{:.1}",
                stations.value(i),
                mins.value(i),
                means.value(i),
                maxs.value(i),
            ));
        }
    }

    println!("{{{}}}", entries.join(", "));

    eprintln!("Finished in {:.2?}", start.elapsed());
    Ok(())
}
