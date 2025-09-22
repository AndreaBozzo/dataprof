/// Domain-specific dataset generation for comprehensive performance testing
use serde_json::json;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use crate::testing::{DatasetConfig, DatasetSize, DatasetPattern};

/// Domain-specific dataset types
#[derive(Debug, Clone, Copy)]
pub enum DatasetDomain {
    Csv,      // Standard CSV files
    Json,     // JSON Lines and JSON arrays
    Database, // Database exports (CSV format but with DB-style data)
    Streaming, // Large streaming data patterns
    RealWorld, // Real-world data patterns with edge cases
}

/// JSON dataset variants
#[derive(Debug, Clone, Copy)]
pub enum JsonVariant {
    Lines,    // JSON Lines (.jsonl)
    Array,    // JSON Array
    Nested,   // Nested JSON objects
    Mixed,    // Mixed types in JSON
}

/// Database simulation patterns
#[derive(Debug, Clone, Copy)]
pub enum DatabasePattern {
    Transactions,  // Transaction-like data
    TimeSeries,   // Time series data
    UserProfiles, // User profile data
    Analytics,    // Analytics/metrics data
    Normalized,   // Normalized database export
}

/// Domain dataset generator
pub struct DomainDatasetGenerator;

impl DomainDatasetGenerator {
    /// Generate CSV domain datasets (enhanced from base patterns)
    pub fn generate_csv_domain(
        config: &DatasetConfig,
        domain_pattern: DatabasePattern,
        output_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(output_path)?;
        let mut writer = BufWriter::new(file);

        match domain_pattern {
            DatabasePattern::Transactions => {
                Self::generate_transaction_csv(&mut writer, config)?;
            }
            DatabasePattern::TimeSeries => {
                Self::generate_timeseries_csv(&mut writer, config)?;
            }
            DatabasePattern::UserProfiles => {
                Self::generate_userprofile_csv(&mut writer, config)?;
            }
            DatabasePattern::Analytics => {
                Self::generate_analytics_csv(&mut writer, config)?;
            }
            DatabasePattern::Normalized => {
                Self::generate_normalized_csv(&mut writer, config)?;
            }
        }

        writer.flush()?;
        Ok(())
    }

    /// Generate JSON domain datasets
    pub fn generate_json_domain(
        config: &DatasetConfig,
        variant: JsonVariant,
        output_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(output_path)?;
        let mut writer = BufWriter::new(file);

        match variant {
            JsonVariant::Lines => {
                Self::generate_jsonlines(&mut writer, config)?;
            }
            JsonVariant::Array => {
                Self::generate_json_array(&mut writer, config)?;
            }
            JsonVariant::Nested => {
                Self::generate_nested_json(&mut writer, config)?;
            }
            JsonVariant::Mixed => {
                Self::generate_mixed_json(&mut writer, config)?;
            }
        }

        writer.flush()?;
        Ok(())
    }

    /// Generate realistic streaming data (large, consistent patterns)
    pub fn generate_streaming_domain(
        config: &DatasetConfig,
        pattern: DatasetPattern,
        output_path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(output_path)?;
        let mut writer = BufWriter::new(file);

        // Generate streaming-optimized data with consistent schemas
        // Use string matching to avoid any enum variant conflicts
        let pattern_str = format!("{:?}", pattern);
        match pattern_str.as_str() {
            "Basic" => {
                Self::generate_streaming_basic(&mut writer, config)?;
            }
            "Numeric" => {
                Self::generate_streaming_numeric(&mut writer, config)?;
            }
            "Mixed" => {
                Self::generate_streaming_mixed(&mut writer, config)?;
            }
            _ => {
                return Err(format!("Pattern {} not supported for streaming domain (only Basic, Numeric, Mixed are supported)", pattern_str).into());
            }
        }

        writer.flush()?;
        Ok(())
    }

    // === CSV Domain Generators ===

    fn generate_transaction_csv(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(
            writer,
            "transaction_id,user_id,amount,currency,timestamp,status,payment_method,merchant_id"
        )?;

        let rows = config.default_rows();
        for i in 0..rows {
            let transaction_id = format!("txn_{:08}", i);
            let user_id = format!("user_{}", i % 10000); // 10K unique users
            let amount = (i as f64 * 1.7) % 1000.0 + 10.0; // $10-$1010
            let currency = match i % 5 {
                0 => "USD",
                1 => "EUR",
                2 => "GBP",
                3 => "JPY",
                _ => "CAD",
            };
            let timestamp = format!("2023-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                1 + (i % 12),
                1 + (i % 28),
                i % 24,
                (i * 7) % 60,
                (i * 13) % 60
            );
            let status = match i % 10 {
                0..=7 => "completed",
                8 => "pending",
                _ => "failed",
            };
            let payment_method = match i % 4 {
                0 => "credit_card",
                1 => "debit_card",
                2 => "paypal",
                _ => "bank_transfer",
            };
            let merchant_id = format!("merchant_{}", i % 1000); // 1K merchants

            writeln!(
                writer,
                "{},{},{:.2},{},{},{},{},{}",
                transaction_id, user_id, amount, currency, timestamp, status, payment_method, merchant_id
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_timeseries_csv(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "timestamp,sensor_id,value,unit,quality,location")?;

        let rows = config.default_rows();
        let base_time = 1640995200; // 2022-01-01 00:00:00 UTC

        for i in 0..rows {
            let timestamp = base_time + (i as i64 * 60); // 1 minute intervals
            let sensor_id = format!("sensor_{:03}", i % 100); // 100 sensors
            let value = 20.0 + (i as f64 * 0.1).sin() * 10.0 + (i as f64 * 0.01) % 5.0; // Sine wave with noise
            let unit = match i % 4 {
                0 => "celsius",
                1 => "humidity",
                2 => "pressure",
                _ => "voltage",
            };
            let quality = if i % 100 == 0 { "poor" } else if i % 20 == 0 { "fair" } else { "good" };
            let location = format!("zone_{}", (i % 10) + 1);

            writeln!(
                writer,
                "{},{},{:.3},{},{},{}",
                timestamp, sensor_id, value, unit, quality, location
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_userprofile_csv(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(
            writer,
            "user_id,username,email,age,country,registration_date,last_login,premium,total_spent"
        )?;

        let rows = config.default_rows();
        let countries = ["US", "UK", "DE", "FR", "JP", "CA", "AU", "IT", "ES", "BR"];

        for i in 0..rows {
            let user_id = format!("user_{:06}", i);
            let username = format!("user{}", i);
            let email = format!("user{}@example.com", i);
            let age = 18 + (i % 65);
            let country = countries[i % countries.len()];
            let registration_date = format!("2020-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28));
            let last_login = if i % 20 == 0 {
                "null".to_string()
            } else {
                format!("2023-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28))
            };
            let premium = i % 10 == 0;
            let total_spent = if premium { (i as f64 * 2.5) % 5000.0 } else { (i as f64 * 0.8) % 500.0 };

            writeln!(
                writer,
                "{},{},{},{},{},{},{},{},{:.2}",
                user_id, username, email, age, country, registration_date, last_login, premium, total_spent
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_analytics_csv(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(
            writer,
            "date,page_views,unique_visitors,bounce_rate,avg_session_duration,conversion_rate,revenue"
        )?;

        let rows = config.default_rows();
        let _base_date = "2023-01-01"; // Reserved for future use

        for i in 0..rows {
            let date = format!("2023-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28));
            let page_views = 1000 + (i * 17) % 50000;
            let unique_visitors = page_views / 3 + (i * 13) % 1000;
            let bounce_rate = 0.3 + (i as f64 * 0.001) % 0.4; // 30-70%
            let avg_session_duration = 120.0 + (i as f64 * 0.5) % 300.0; // 2-7 minutes
            let conversion_rate = 0.02 + (i as f64 * 0.0001) % 0.08; // 2-10%
            let revenue = unique_visitors as f64 * conversion_rate * (50.0 + (i as f64 * 0.1) % 200.0);

            writeln!(
                writer,
                "{},{},{},{:.3},{:.1},{:.4},{:.2}",
                date, page_views, unique_visitors, bounce_rate, avg_session_duration, conversion_rate, revenue
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_normalized_csv(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Simulates normalized database export with foreign keys
        writeln!(writer, "id,customer_id,product_id,order_date,quantity,unit_price,discount")?;

        let rows = config.default_rows();
        for i in 0..rows {
            let id = i + 1;
            let customer_id = (i % 5000) + 1; // 5K customers
            let product_id = (i % 1000) + 1; // 1K products
            let order_date = format!("2023-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28));
            let quantity = (i % 10) + 1;
            let unit_price = 10.0 + (i as f64 * 1.7) % 990.0; // $10-$1000
            let discount = if i % 10 == 0 { 0.1 } else if i % 20 == 0 { 0.05 } else { 0.0 };

            writeln!(
                writer,
                "{},{},{},{},{},{:.2},{:.2}",
                id, customer_id, product_id, order_date, quantity, unit_price, discount
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    // === JSON Domain Generators ===

    fn generate_jsonlines(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rows = config.default_rows();

        for i in 0..rows {
            let record = json!({
                "id": i,
                "timestamp": format!("2023-{:02}-{:02}T{:02}:{:02}:{:02}Z",
                    1 + (i % 12), 1 + (i % 28), i % 24, (i * 7) % 60, (i * 13) % 60),
                "user_id": format!("user_{}", i % 1000),
                "action": match i % 5 {
                    0 => "login",
                    1 => "view_page",
                    2 => "purchase",
                    3 => "logout",
                    _ => "search"
                },
                "value": (i as f64 * 1.5) % 100.0,
                "metadata": {
                    "source": "web",
                    "version": "1.2.3"
                }
            });

            writeln!(writer, "{}", record)?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_json_array(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rows = config.default_rows();
        writeln!(writer, "[")?;

        for i in 0..rows {
            let record = json!({
                "product_id": i,
                "name": format!("Product {}", i),
                "price": 10.0 + (i as f64 * 1.3) % 990.0,
                "category": match i % 4 {
                    0 => "electronics",
                    1 => "clothing",
                    2 => "books",
                    _ => "home"
                },
                "in_stock": i % 5 != 0,
                "tags": vec![
                    format!("tag_{}", i % 10),
                    format!("category_{}", i % 4)
                ]
            });

            if i == rows - 1 {
                writeln!(writer, "  {}", record)?;
            } else {
                writeln!(writer, "  {},", record)?;
            }

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }

        writeln!(writer, "]")?;
        Ok(())
    }

    fn generate_nested_json(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rows = config.default_rows();

        for i in 0..rows {
            let record = json!({
                "order_id": format!("order_{:08}", i),
                "customer": {
                    "id": i % 1000,
                    "name": format!("Customer {}", i % 1000),
                    "email": format!("customer{}@example.com", i % 1000),
                    "address": {
                        "street": format!("{} Main St", i % 9999),
                        "city": match i % 5 {
                            0 => "New York",
                            1 => "Los Angeles",
                            2 => "Chicago",
                            3 => "Houston",
                            _ => "Phoenix"
                        },
                        "zip": format!("{:05}", 10000 + (i % 90000))
                    }
                },
                "items": (0..(i % 5 + 1)).map(|j| {
                    json!({
                        "product_id": (i + j) % 100,
                        "quantity": j + 1,
                        "price": 10.0 + ((i + j) as f64 * 1.1) % 100.0
                    })
                }).collect::<Vec<_>>(),
                "total": (10.0 + (i as f64 * 2.3)) % 500.0,
                "status": "completed"
            });

            writeln!(writer, "{}", record)?;

            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_mixed_json(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rows = config.default_rows();

        for i in 0..rows {
            let record = match i % 3 {
                0 => json!({ // Event type
                    "type": "event",
                    "event_id": i,
                    "name": "user_action",
                    "properties": {
                        "action": "click",
                        "element": "button"
                    },
                    "timestamp": i as u64 * 1000
                }),
                1 => json!({ // User type
                    "type": "user",
                    "user_id": i,
                    "profile": {
                        "age": 20 + (i % 60),
                        "country": "US",
                        "premium": i % 10 == 0
                    },
                    "last_seen": (i as u64 * 1000) + 3600
                }),
                _ => json!({ // Product type
                    "type": "product",
                    "product_id": i,
                    "details": {
                        "name": format!("Product {}", i),
                        "price": (i as f64 * 1.7) % 1000.0,
                        "available": i % 7 != 0
                    },
                    "reviews_count": i % 100
                })
            };

            writeln!(writer, "{}", record)?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    // === Streaming Domain Generators ===

    fn generate_streaming_basic(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Optimized for streaming: consistent schema, no surprises
        writeln!(writer, "id,timestamp,sensor_value,status")?;

        let rows = config.default_rows();
        for i in 0..rows {
            writeln!(
                writer,
                "{},{},{:.3},{}",
                i,
                1640995200 + (i as i64 * 60), // Unix timestamp
                (i as f64 * 0.1).sin() * 100.0, // Predictable sine wave
                if i % 100 == 0 { "error" } else { "ok" }
            )?;

            // More frequent flushing for streaming
            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_streaming_numeric(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "timestamp,value1,value2,value3,calculated")?;

        let rows = config.default_rows();
        for i in 0..rows {
            let v1 = (i as f64 * 0.01).sin();
            let v2 = (i as f64 * 0.02).cos();
            let v3 = (i as f64 * 0.005).tan();
            let calculated = v1 + v2 + v3;

            writeln!(
                writer,
                "{},{:.6},{:.6},{:.6},{:.6}",
                1640995200 + (i as i64),
                v1, v2, v3, calculated
            )?;

            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    fn generate_streaming_mixed(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "timestamp,session_id,event_type,value,user_agent")?;

        let rows = config.default_rows();
        let user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36"
        ];

        for i in 0..rows {
            writeln!(
                writer,
                "{},session_{},{},{:.2},{}",
                1640995200 + (i as i64 * 10), // 10 second intervals
                i % 1000, // 1000 concurrent sessions
                match i % 4 {
                    0 => "page_view",
                    1 => "click",
                    2 => "scroll",
                    _ => "exit"
                },
                (i as f64 * 1.1) % 100.0,
                user_agents[i % user_agents.len()]
            )?;

            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }
}

/// Easy access to domain-specific datasets
pub struct DomainDatasets;

impl DomainDatasets {
    /// Generate transaction CSV dataset
    pub fn transactions(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, DatasetPattern::Mixed);
        let temp_path = std::env::temp_dir().join(format!("transactions_{}_{}.csv",
            format!("{:?}", size).to_lowercase(),
            std::process::id()
        ));

        DomainDatasetGenerator::generate_csv_domain(&config, DatabasePattern::Transactions, &temp_path)?;
        Ok(temp_path)
    }

    /// Generate time series CSV dataset
    pub fn timeseries(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, DatasetPattern::Numeric);
        let temp_path = std::env::temp_dir().join(format!("timeseries_{}_{}.csv",
            format!("{:?}", size).to_lowercase(),
            std::process::id()
        ));

        DomainDatasetGenerator::generate_csv_domain(&config, DatabasePattern::TimeSeries, &temp_path)?;
        Ok(temp_path)
    }

    /// Generate JSON Lines dataset
    pub fn jsonlines(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, DatasetPattern::Mixed);
        let temp_path = std::env::temp_dir().join(format!("events_{}_{}.jsonl",
            format!("{:?}", size).to_lowercase(),
            std::process::id()
        ));

        DomainDatasetGenerator::generate_json_domain(&config, JsonVariant::Lines, &temp_path)?;
        Ok(temp_path)
    }

    /// Generate nested JSON dataset
    pub fn nested_json(size: DatasetSize) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, DatasetPattern::Mixed);
        let temp_path = std::env::temp_dir().join(format!("orders_{}_{}.json",
            format!("{:?}", size).to_lowercase(),
            std::process::id()
        ));

        DomainDatasetGenerator::generate_json_domain(&config, JsonVariant::Nested, &temp_path)?;
        Ok(temp_path)
    }

    /// Generate streaming-optimized dataset
    pub fn streaming(size: DatasetSize, pattern: DatasetPattern) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, pattern);
        let temp_path = std::env::temp_dir().join(format!("streaming_{:?}_{}_{}.csv",
            pattern,
            format!("{:?}", size).to_lowercase(),
            std::process::id()
        ));

        DomainDatasetGenerator::generate_streaming_domain(&config, pattern, &temp_path)?;
        Ok(temp_path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_transaction_dataset_generation() {
        let path = DomainDatasets::transactions(DatasetSize::Micro).unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("transaction_id,user_id,amount"));
        assert!(content.contains("txn_"));
        assert!(content.contains("USD"));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_jsonlines_generation() {
        let path = DomainDatasets::jsonlines(DatasetSize::Micro).unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("\"id\":"));
        assert!(content.contains("\"action\":"));

        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_streaming_dataset_generation() {
        let path = DomainDatasets::streaming(DatasetSize::Micro, DatasetPattern::Numeric).unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("timestamp,value1,value2"));

        let _ = fs::remove_file(&path);
    }
}