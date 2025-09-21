/// Unified dataset generation for standardized benchmarking
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Dataset size categories
#[derive(Debug, Clone, Copy)]
pub enum DatasetSize {
    Micro,  // <1MB - Unit test level (100-5K rows)
    Small,  // 1-10MB - Integration test level (5K-100K rows)
    Medium, // 10-100MB - Performance test level (100K-1M rows)
    Large,  // 100MB-1GB - Stress test level (1M+ rows)
}

/// Dataset patterns for different testing scenarios
#[derive(Debug, Clone, Copy)]
pub enum DatasetPattern {
    Basic,   // Simple structured data (id, name, age, score)
    Mixed,   // Mixed data types (strings, numbers, booleans, dates)
    Numeric, // Number-heavy data for SIMD testing
    Wide,    // Many columns (50+ cols, few rows)
    Deep,    // Few columns, many rows
    Unicode, // International character sets
    Messy,   // Real-world inconsistencies
}

/// Dataset generation configuration
#[derive(Debug, Clone)]
pub struct DatasetConfig {
    pub size: DatasetSize,
    pub pattern: DatasetPattern,
    pub name: String,
    pub rows: Option<usize>,
    pub cols: Option<usize>,
    pub seed: u64, // For reproducible generation
}

impl DatasetConfig {
    pub fn new(size: DatasetSize, pattern: DatasetPattern) -> Self {
        Self {
            size,
            pattern,
            name: format!("{:?}_{:?}", size, pattern).to_lowercase(),
            rows: None,
            cols: None,
            seed: 42, // Default seed for reproducibility
        }
    }

    /// Get default row count for size category
    pub fn default_rows(&self) -> usize {
        if let Some(rows) = self.rows {
            return rows;
        }

        match self.size {
            DatasetSize::Micro => match self.pattern {
                DatasetPattern::Wide => 1000,
                _ => 5000,
            },
            DatasetSize::Small => match self.pattern {
                DatasetPattern::Wide => 10000,
                _ => 50000,
            },
            DatasetSize::Medium => match self.pattern {
                DatasetPattern::Wide => 50000,
                _ => 500000,
            },
            DatasetSize::Large => match self.pattern {
                DatasetPattern::Wide => 100000,
                _ => 2000000,
            },
        }
    }

    /// Get default column count for pattern
    pub fn default_cols(&self) -> usize {
        if let Some(cols) = self.cols {
            return cols;
        }

        match self.pattern {
            DatasetPattern::Basic => 4,
            DatasetPattern::Mixed => 10,
            DatasetPattern::Numeric => 7,
            DatasetPattern::Wide => 50,
            DatasetPattern::Deep => 4,
            DatasetPattern::Unicode => 3,
            DatasetPattern::Messy => 8,
        }
    }
}

/// Standard dataset generator
pub struct DatasetGenerator;

impl DatasetGenerator {
    /// Generate dataset to specific path
    pub fn generate_to_path(
        config: &DatasetConfig,
        path: &Path,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        match config.pattern {
            DatasetPattern::Basic => Self::generate_basic(&mut writer, config)?,
            DatasetPattern::Mixed => Self::generate_mixed(&mut writer, config)?,
            DatasetPattern::Numeric => Self::generate_numeric(&mut writer, config)?,
            DatasetPattern::Wide => Self::generate_wide(&mut writer, config)?,
            DatasetPattern::Deep => Self::generate_deep(&mut writer, config)?,
            DatasetPattern::Unicode => Self::generate_unicode(&mut writer, config)?,
            DatasetPattern::Messy => Self::generate_messy(&mut writer, config)?,
        }

        writer.flush()?;
        Ok(())
    }

    /// Generate basic structured data
    fn generate_basic(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "id,name,age,score")?;

        let rows = config.default_rows();
        for i in 0..rows {
            writeln!(
                writer,
                "{},Person_{},{},{:.1}",
                i,
                i,
                25 + (i % 50),
                (i as f64 * 1.5) % 100.0
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate mixed data types
    fn generate_mixed(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(
            writer,
            "id,name,email,age,salary,created_at,active,score,category,description"
        )?;

        let rows = config.default_rows();
        for i in 0..rows {
            let name = format!("User_{}", i);
            let email = format!("user{}@example.com", i);
            let age = 18 + (i % 65);
            let salary = 30000 + (i % 100000);
            let created_at = format!("2023-{:02}-{:02}", 1 + (i % 12), 1 + (i % 28));
            let active = i % 3 == 0;
            let score = (i as f64 * 1.7) % 100.0;
            let category = match i % 5 {
                0 => "Premium",
                1 => "Standard",
                2 => "Basic",
                3 => "Enterprise",
                _ => "Trial",
            };
            let description = format!("Description for user {} with content", i);

            writeln!(
                writer,
                "{},{},{},{},{},{},{},{:.2},{},{}",
                i, name, email, age, salary, created_at, active, score, category, description
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate numeric-heavy data for SIMD testing
    fn generate_numeric(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "id,value1,value2,value3,value4,value5,calculated")?;

        let rows = config.default_rows();
        for i in 0..rows {
            let v1 = (i as f64 * 1.1) % 1000.0;
            let v2 = (i as f64 * 2.3) % 1000.0;
            let v3 = (i as f64 * 3.7) % 1000.0;
            let v4 = (i as f64 * 5.1) % 1000.0;
            let v5 = (i as f64 * 7.9) % 1000.0;
            let calculated = v1 + v2 + v3 + v4 + v5;

            writeln!(
                writer,
                "{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3}",
                i, v1, v2, v3, v4, v5, calculated
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate wide format (many columns)
    fn generate_wide(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cols = config.default_cols();
        let headers: Vec<String> = (0..cols).map(|i| format!("col_{}", i)).collect();
        writeln!(writer, "{}", headers.join(","))?;

        let rows = config.default_rows();
        for i in 0..rows {
            let values: Vec<String> = (0..cols).map(|j| format!("value_{}_{}", i, j)).collect();
            writeln!(writer, "{}", values.join(","))?;

            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate deep format (few columns, many rows)
    fn generate_deep(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "id,name,value,timestamp")?;

        let rows = config.default_rows();
        for i in 0..rows {
            writeln!(
                writer,
                "{},Name_{},{:.3},2023-01-{:02}",
                i,
                i,
                (i as f64 * 1.7) % 1000.0,
                1 + (i % 30)
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate unicode data
    fn generate_unicode(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "name,city,country")?;

        let unicode_data = [
            ("João", "São Paulo", "Brasil"),
            ("田中", "東京", "日本"),
            ("Γιάννης", "Αθήνα", "Ελλάδα"),
            ("François", "Paris", "France"),
            ("Ahmed", "Cairo", "مصر"),
        ];

        let rows = config.default_rows();
        for i in 0..rows {
            let (name, city, country) = unicode_data[i % unicode_data.len()];
            writeln!(writer, "{},{},{}", name, city, country)?;

            if i % 1000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }

    /// Generate messy real-world data
    fn generate_messy(
        writer: &mut BufWriter<File>,
        config: &DatasetConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        writeln!(writer, "id,name,age,salary,notes,status,created,score")?;

        let rows = config.default_rows();
        for i in 0..rows {
            let name = if i % 20 == 0 {
                ""
            } else {
                &format!("User {}", i)
            };
            let age = if i % 15 == 0 {
                "".to_string()
            } else {
                (18 + (i % 65)).to_string()
            };
            let salary = if i % 25 == 0 {
                "N/A".to_string()
            } else {
                format!("{}", 30000 + (i % 100000))
            };
            let notes = match i % 10 {
                0 => "Normal user",
                1 => "VIP customer, handle with care!",
                2 => "",
                3 => "Has \"quotes\" in notes",
                4 => "Comma, separated, values",
                _ => "Standard notes",
            };
            let status = match i % 7 {
                0 => "active",
                1 => "ACTIVE",
                2 => "Active",
                3 => "inactive",
                4 => "pending",
                5 => "",
                _ => "unknown",
            };
            let created = if i % 30 == 0 {
                "invalid-date"
            } else {
                "2023-01-15"
            };
            let score = if i % 12 == 0 {
                "".to_string()
            } else {
                format!("{:.1}", (i as f64 * 1.5) % 100.0)
            };

            writeln!(
                writer,
                "{},{},{},{},\"{}\",{},{},{}",
                i, name, age, salary, notes, status, created, score
            )?;

            if i % 10000 == 0 {
                writer.flush()?;
            }
        }
        Ok(())
    }
}

/// Standard dataset collections for easy access
pub struct StandardDatasets;

impl StandardDatasets {
    /// Generate dataset to a temporary file path
    pub fn generate_temp_file(
        size: DatasetSize,
        pattern: DatasetPattern,
    ) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let config = DatasetConfig::new(size, pattern);
        let temp_path = std::env::temp_dir().join(format!(
            "dataprof_bench_{}_{}.csv",
            config.name,
            std::process::id()
        ));
        DatasetGenerator::generate_to_path(&config, &temp_path)?;
        Ok(temp_path)
    }

    /// Get micro dataset
    pub fn micro(pattern: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let pattern = Self::parse_pattern(pattern)?;
        Self::generate_temp_file(DatasetSize::Micro, pattern)
    }

    /// Get small dataset
    pub fn small(pattern: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let pattern = Self::parse_pattern(pattern)?;
        Self::generate_temp_file(DatasetSize::Small, pattern)
    }

    /// Get medium dataset
    pub fn medium(pattern: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let pattern = Self::parse_pattern(pattern)?;
        Self::generate_temp_file(DatasetSize::Medium, pattern)
    }

    /// Get large dataset
    pub fn large(pattern: &str) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let pattern = Self::parse_pattern(pattern)?;
        Self::generate_temp_file(DatasetSize::Large, pattern)
    }

    fn parse_pattern(pattern: &str) -> Result<DatasetPattern, Box<dyn std::error::Error>> {
        match pattern.to_lowercase().as_str() {
            "basic" => Ok(DatasetPattern::Basic),
            "mixed" => Ok(DatasetPattern::Mixed),
            "numeric" => Ok(DatasetPattern::Numeric),
            "wide" => Ok(DatasetPattern::Wide),
            "deep" => Ok(DatasetPattern::Deep),
            "unicode" => Ok(DatasetPattern::Unicode),
            "messy" => Ok(DatasetPattern::Messy),
            _ => Err(format!("Unknown pattern: {}", pattern).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_basic_dataset_generation() {
        let config = DatasetConfig::new(DatasetSize::Micro, DatasetPattern::Basic);
        let temp_path = std::env::temp_dir().join("test_basic_dataset.csv");
        DatasetGenerator::generate_to_path(&config, &temp_path).unwrap();

        assert!(temp_path.exists());
        let content = fs::read_to_string(&temp_path).unwrap();
        assert!(content.contains("id,name,age,score"));
        assert!(content.lines().count() > 100);

        // Cleanup
        let _ = fs::remove_file(&temp_path);
    }

    #[test]
    fn test_mixed_dataset_generation() {
        let path = StandardDatasets::micro("mixed").unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("id,name,email"));
        assert!(content.contains("@example.com"));

        // Cleanup
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn test_numeric_dataset_generation() {
        let path = StandardDatasets::small("numeric").unwrap();
        assert!(path.exists());

        let content = fs::read_to_string(&path).unwrap();
        assert!(content.contains("value1,value2,value3"));

        // Cleanup
        let _ = fs::remove_file(&path);
    }
}
