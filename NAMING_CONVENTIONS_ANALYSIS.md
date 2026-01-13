# DataProf Naming Conventions Analysis & Standardization Guide

## Executive Summary

The DataProf codebase has **three major naming inconsistencies** that reduce API clarity and maintainability:

1. **analyze_* vs profile_* naming** - Inconsistent verb choice for similar operations
2. **Builder method prefixes** - Some methods use `with_` prefix, others don't
3. **Config/Thresholds suffixes** - Inconsistent naming for configuration types

---

## Issue 1: analyze_* vs profile_* Patterns

### Current State

The codebase mixes two naming conventions for similar analysis operations:

#### Using `analyze_*`:
```rust
// Rust public API
pub fn analyze_csv(file_path: &Path) -> Result<Vec<ColumnProfile>>
pub fn analyze_csv_with_verbosity(file_path: &Path, verbosity: u8) -> Result<Vec<ColumnProfile>>
pub fn analyze_csv_robust(file_path: &Path) -> Result<QualityReport>
pub fn analyze_csv_with_sampling(file_path: &Path) -> Result<QualityReport>
pub fn analyze_csv_fast(file_path: &Path) -> Result<Vec<ColumnProfile>>
pub fn analyze_json(file_path: &Path) -> Result<Vec<ColumnProfile>>
pub fn analyze_json_with_quality(file_path: &Path) -> Result<QualityReport>
pub fn analyze_parquet_with_quality(file_path: &Path) -> Result<QualityReport>
pub fn analyze_parquet_with_config(file_path: &Path, config: &ParquetConfig) -> Result<QualityReport>
pub fn analyze_column(name: &str, data: &[String]) -> ColumnProfile

// Python API
analyze_csv_file(path: str)
analyze_csv_with_quality(path: str)
analyze_csv_with_logging(path: str)
analyze_csv_dataframe(path: str)
analyze_json_file(path: str)
analyze_json_with_quality(path: str)
analyze_parquet_file(path: str)
analyze_parquet_with_quality_py(path: str)
batch_analyze_directory(path: str, recursive: bool)
batch_analyze_glob(pattern: str)
```

#### Using `profile_*`:
```rust
// Rust public API
pub async fn profile_database(config: DatabaseConfig, query: &str) -> Result<QualityReport>
pub async fn profile_database_async(...) // Python binding

// Python API
profile_database_async(...)
profile(file_path) // High-level wrapper
ProfileReport  // Class

// In CHANGELOG (historical references)
profile_database_with_ml()
iso_quality_profile_strict()
iso_quality_profile_lenient()
```

### Analysis

**Problem:**
- `analyze_*` is used for **file/data analysis** (returns ColumnProfile, QualityReport)
- `profile_*` is used for **database profiling** and **quality profiles**
- No clear semantic distinction - both terms mean similar things
- Users are confused: Is it "analyze my CSV" or "profile my CSV"?
- The `profile_database()` stands out as inconsistent with file operations

### Recommendation

**Standardize on `analyze_*` for all data analysis operations**

**Rationale:**
- More common in data profiling tools (pandas, Great Expectations use "analyze" patterns)
- Already dominant in the codebase (>30 uses vs ~5 for profile_)
- More specific: "analyze" = extract profiles; "profile" = could be monitoring
- Provides clearer API surface

**Changes Required:**
```rust
// Rename to be consistent
profile_database()           -> analyze_database()
profile_database_with_ml()   -> analyze_database_with_ml()
profile_database_async()     -> analyze_database_async()
profile()                    -> analyze()

// Keep as-is (these are quality profile definitions, not analysis operations)
iso_quality_profile_strict()
iso_quality_profile_lenient()
```

---

## Issue 2: Builder Method Naming Inconsistency

### Current State

Builder methods are inconsistently prefixed with `with_`:

#### Methods WITH `with_` prefix:
```rust
// Core builders
pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self
pub fn with_smart_progress(mut self) -> Self
pub fn with_batch_size(batch_size: usize) -> Self
pub fn with_auto_recovery(mut self, enabled: bool) -> Self
pub fn with_retry_config(mut self, config: RetryConfig) -> Self
pub fn with_seed(capacity: usize, seed: u64) -> Self
pub fn with_limits(max_unique: usize, max_sample: usize) -> Self
pub fn with_memory_limit(limit_mb: usize) -> Self
pub fn with_config(config: BatchConfig) -> Self
pub fn with_progress(config: BatchConfig, progress_manager: ProgressManager) -> Self
pub fn with_memory_tracking(/* ... */) -> Self
pub fn with_forced_format(format: OutputFormat) -> Self
pub fn with_logging(mut self, enabled: bool) -> Self
pub fn with_fallback(mut self, enabled: bool) -> Self
pub fn with_performance_logging(mut self, enabled: bool) -> Self
pub fn with_thresholds(thresholds: IsoQualityThresholds) -> Self
```

#### Methods WITHOUT `with_` prefix (same purpose):
```rust
pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self       // DataProfiler
pub fn sampling(mut self, strategy: SamplingStrategy) -> Self    // IncrementalProfiler
pub fn progress_callback<F>(mut self, callback: F) -> Self       // IncrementalProfiler
pub fn output_format(mut self, format: &str) -> Self             // DataprofConfigBuilder
pub fn colored(mut self, enabled: bool) -> Self                  // DataprofConfigBuilder
pub fn verbosity(mut self, level: u8) -> Self                    // DataprofConfigBuilder
pub fn show_progress(mut self, enabled: bool) -> Self            // DataprofConfigBuilder
pub fn html_auto_generate(mut self, enabled: bool) -> Self       // DataprofConfigBuilder
pub fn engine(mut self, engine: &str) -> Self                    // DataprofConfigBuilder
pub fn parallel(mut self, enabled: bool) -> Self                 // DataprofConfigBuilder
pub fn max_concurrent(mut self, max: usize) -> Self              // DataprofConfigBuilder
pub fn max_memory_mb(mut self, mb: usize) -> Self                // DataprofConfigBuilder
pub fn auto_streaming_threshold_mb(mut self, mb: f64) -> Self    // DataprofConfigBuilder
```

### Analysis

**Problem:**
- **54% of builder methods use `with_`** prefix, but 46% don't
- No clear rule: Why is `chunk_size()` called without `with_` but `with_memory_limit()` with it?
- `with_` prefix is useful for boolean/complex configuration, but not applied consistently
- Makes API harder to discover - users don't know expected method names
- Example: Related methods look different:
  ```rust
  profiler.with_memory_limit(256)  // Why "with_"?
  profiler.chunk_size(1024)        // Why not "with_chunk_size"?
  ```

### Recommendation

**Strategy: Use `with_` prefix only for boolean flags and complex/multi-field configurations**

**Rationale:**
- `with_` conveys "add this optional behavior/setting"
- Simple scalar setters (size, format, level) don't need prefix
- Clearer intent: `enabled()` vs `with_logging()` - both are clear
- Follows Rust builder conventions (e.g., tokio::task::Builder)

**Changes Required:**

Remove `with_` prefix:
```rust
// Rename (remove with_)
with_batch_size()            -> batch_size()
with_seed()                  -> seed()
with_limits()                -> limits()
with_memory_limit()          -> memory_limit()
with_forced_format()         -> format()
```

Keep `with_` prefix (good use cases):
```rust
// These stay - they're enabling complex behaviors
with_enhanced_progress()     // ✓ Complex feature
with_smart_progress()        // ✓ Complex feature
with_auto_recovery()         // ✓ Boolean behavior
with_retry_config()          // ✓ Complex configuration object
with_progress()              // ✓ Multiple params + behavior
with_memory_tracking()       // ✓ Enables tracking system
with_logging()               // ✓ Enables feature
with_fallback()              // ✓ Enables feature
with_performance_logging()   // ✓ Enables feature
with_thresholds()            // ✓ Complex object
```

---

## Issue 3: Config Suffix Inconsistency

### Current State

Configuration and threshold types use inconsistent naming patterns:

#### Actual types found:
```rust
pub struct DataprofConfig              // "Config"
pub struct IsoQualityThresholds        // "Thresholds"
pub struct OutputConfig                // "Config"
pub struct EngineConfig                // "Config"
pub struct BatchConfig                 // "Config"
pub struct ParquetConfig               // "Config"
pub struct DatabaseConfig              // "Config"
pub struct RetryConfig                 // "Config"

// Related but differently named:
pub struct SamplingStrategy            // "Strategy" (not Config)
pub struct ChunkSize                   // Direct noun (not Config)
pub enum Engine                        // Enum (not Config)
pub struct ProgressManager             // "Manager" (not Config)
```

### Analysis

**Problem:**
- Most config uses `*Config` suffix (good!)
- BUT `IsoQualityThresholds` breaks the pattern - uses "Thresholds"
- `SamplingStrategy` uses "Strategy" - why not `SamplingConfig`?
- `ChunkSize` is just a direct type - inconsistent with config pattern
- When you have:
  ```rust
  quality_config: OutputConfig,
  quality_thresholds: IsoQualityThresholds,  // ← Different naming!
  ```
  It's unclear if these are the same kind of thing

### Recommendation

**Standardize on `*Config` suffix for all configuration and threshold types**

**Changes Required:**
```rust
// Rename
IsoQualityThresholds  -> IsoQualityConfig  // or IsoQualityThresholdsConfig

// Consider renaming (if semantically appropriate):
SamplingStrategy      -> SamplingConfig (if it's configurable; else leave as Strategy enum)
```

**Special Cases:**
- `ChunkSize` - Keep as-is (it's an enum, not a config struct)
- `SamplingStrategy` - Can stay as-is (it's a strategy enum, not config)
- `Engine` - Keep as-is (it's a functional enum, not config)

---

## Implementation Priority

### Phase 1: High Priority (API clarity)
1. **Standardize `analyze_*` naming**
   - Rename `profile_database*` → `analyze_database*`
   - Estimated impact: 5-10 functions, 1-2 hours
   - Deprecation path: Keep old names as deprecated aliases

2. **Fix builder method prefixes**
   - Remove `with_` from scalar methods
   - Estimated impact: 5 methods, 30 minutes
   - Deprecation path: Keep old names with `#[deprecated]`

### Phase 2: Medium Priority (consistency)
3. **Rename `IsoQualityThresholds` → `IsoQualityConfig`**
   - Estimated impact: 3-5 files, 20 minutes
   - This is more mechanical; fewer breaking changes

### Phase 3: Documentation
4. **Update examples and guides**
5. **Update CHANGELOG with migration notes**

---

## Migration Guide for Users

### Rust
```rust
// OLD
let report = profile_database(config, query).await?;
profiler.with_memory_limit(256);

// NEW
let report = analyze_database(config, query).await?;
profiler.memory_limit(256);
```

### Python
```python
# OLD
report = dataprof.profile_database_async(...)
result = dataprof.analyze_csv_with_quality("data.csv")

# NEW
report = dataprof.analyze_database_async(...)  # More consistent
result = dataprof.analyze_csv_with_quality("data.csv")  # Unchanged
```

---

## Testing Impact

Files that need updating:
- `src/api/mod.rs` - Builder methods
- `src/engines/streaming/*.rs` - Builder methods
- `src/core/config.rs` - Builder methods
- `src/parsers/*.rs` - analyze_ functions
- `src/python/*.rs` - Python bindings
- `src/database/connectors/*.rs` - profile_database() → analyze_database()
- Tests across all modules
- Documentation examples
- CHANGELOG.md

---

## Summary Table

| Issue | Current State | Recommended | Reason |
|-------|---------------|-------------|--------|
| `analyze_*` vs `profile_*` | Mixed usage | Standardize on `analyze_*` | More common, more consistent |
| Builder `with_` prefix | Inconsistent (54/46 split) | Prefix for boolean/complex only | Clearer intent, follows conventions |
| Config suffixes | Mixed (`*Config`, `*Thresholds`, `*Strategy`) | Standardize on `*Config` | Single, clear pattern |

