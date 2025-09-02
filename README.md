# DataProfiler CLI - Un Tool Rust Innovativo per l'Analisi dei Dati

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![Release](https://github.com/AndreaBozzo/dataprof/workflows/Release/badge.svg)](https://github.com/AndreaBozzo/dataprof/releases)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

## 🎯 Visione del Progetto

DataProfiler è un CLI tool **in sviluppo** scritto in Rust che fornisce analisi istantanea e profonda dei dati, con un focus su **usabilità**,
**performance** e **insights azionabili**. A differenza di tool esistenti come qsv che sono suite complete, DataProfiler si concentra
esclusivamente sul profiling con output ricchi e immediatamente utilizzabili.

## 🚀 Caratteristiche Innovative

### 1. **Smart Profiling con Pattern Recognition**
- **Pattern Detection Automatico**: Riconosce automaticamente pattern nei dati (email, telefoni, codici fiscali, IBAN, etc.)
- **Business Rules Inference**: Deduce regole di business dai dati (es. "il campo prezzo è sempre > 0 e < 10000")
- **Anomaly Detection**: Identifica outlier e anomalie usando algoritmi statistici avanzati
- **Data Quality Score**: Assegna un punteggio di qualità per ogni campo e per il dataset completo

### 2. **Visualizzazione nel Terminale**
- **Mini-grafici ASCII**: Istogrammi, box plot e distribuzioni direttamente nel terminale
- **Heatmap di correlazioni**: Visualizza correlazioni tra campi numerici
- **Timeline visualization**: Per dati temporali, mostra pattern nel tempo
- **Color-coded output**: Usa colori per evidenziare problemi e insights

### 3. **Multi-formato con Performance Ottimizzata**
- Supporto nativo per CSV, TSV, JSON, JSONL, Parquet, Excel
- **Streaming mode**: Analizza file enormi senza caricarli in memoria
- **Sampling intelligente**: Per file molto grandi, usa sampling statisticamente significativo
- **Parallel processing**: Sfrutta tutti i core per analisi velocissime

### 4. **Export Ricchi e Actionable**
- **Report HTML interattivo**: Con grafici D3.js e tabelle filtrabili
- **JSON Schema generation**: Genera schema validazione con regole dedotte
- **SQL DDL generation**: Crea automaticamente CREATE TABLE statements
- **Data Dictionary Markdown**: Documentazione auto-generata
- **PowerBI/Tableau metadata**: Export diretto per tool BI

### 5. **Integrazione con il tuo Workflow**
- **Git-friendly output**: Report comparabili per tracking changes
- **CI/CD integration**: Exit codes significativi e output parsabile
- **Watch mode**: Monitora file per changes e ri-profila automaticamente
- **Pipeline mode**: Si integra facilmente in data pipeline esistenti

## 📊 Funzionalità Core

### Analisi Statistica Avanzata
```bash
# Profiling base con statistiche descrittive
dataprof analyze data.csv

# Profiling dettagliato con pattern recognition
dataprof analyze data.csv --deep

# Solo summary veloce
dataprof summary data.csv

# Confronto tra due dataset
dataprof compare before.csv after.csv
```

### Output Esempio
```
📊 DataProfiler Report - sales_data.csv
═══════════════════════════════════════

📈 Dataset Overview:
├─ Records: 1,234,567
├─ Fields: 15
├─ Size: 125.4 MB
├─ Encoding: UTF-8
└─ Quality Score: 92/100 ✅

🔍 Field Analysis:

[1] customer_id (String)
├─ Unique: 234,567 (19.0%)
├─ Pattern: "CUS-[0-9]{6}" (99.8% match)
├─ Missing: 12 (0.001%)
└─ 📊 Distribution:
    CUS-100000-200000: ████████████ 45%
    CUS-200001-300000: ████████     35%
    CUS-300001-400000: █████        20%

[2] order_date (Date)
├─ Format: YYYY-MM-DD (100%)
├─ Range: 2023-01-01 to 2024-12-31
├─ Missing: 0 (0%)
├─ Anomalies: 3 future dates detected ⚠️
└─ 📈 Trend:
    2023 Q1: ████     25%
    2023 Q2: ██████   35%
    2023 Q3: ████     22%
    2023 Q4: ███      18%

[3] amount (Decimal)
├─ Min: 0.01 | Max: 9,999.99 | Avg: 156.78
├─ Std Dev: 245.67
├─ Outliers: 234 values > 3σ (0.02%)
├─ Currency Pattern: $X,XXX.XX detected
└─ 📊 Distribution:
    $0-50:      ████████████ 45%
    $51-100:    ██████       25%
    $101-500:   █████        20%
    $500+:      ██           10%

🔗 Correlations Detected:
├─ order_date ↔ seasonal_campaign (0.78)
├─ customer_tier ↔ avg_order_value (0.65)
└─ shipping_method ↔ delivery_time (-0.82)

⚠️  Data Quality Issues:
├─ [MEDIUM] 3 future dates in order_date
├─ [LOW] 12 missing customer_ids
└─ [INFO] Consider indexing on customer_id (high cardinality)

💡 Insights:
├─ Seasonal pattern detected: 35% spike in Q2
├─ Customer segmentation opportunity: 3 distinct value clusters
└─ Potential data entry issue: amounts ending in .99 (45%)
```

## 🛠️ Architettura Tecnica

### Stack Tecnologico
- **Parser Multi-formato**:
  - `arrow-rs` per Parquet/Arrow
  - `polars` per operazioni DataFrame velocissime
  - `calamine` per Excel
  - `simd-json` per JSON parsing ultra-veloce

- **Analisi Statistica**:
  - `statrs` per statistiche avanzate
  - `ndarray` per operazioni matriciali
  - Custom algorithms per pattern detection

- **Visualizzazione**:
  - `ratatui` per TUI interattiva
  - `plotters` per grafici
  - `comfy-table` per tabelle formattate

- **Performance**:
  - `rayon` per parallelizzazione
  - `dashmap` per concurrent hashmaps
  - `memmap2` per file grandi

### Struttura del Progetto
```
dataprof/
├── src/
│   ├── main.rs              # CLI entry point
│   ├── analyzer/            # Core analysis engine
│   │   ├── mod.rs
│   │   ├── statistical.rs   # Statistical analysis
│   │   ├── patterns.rs      # Pattern recognition
│   │   ├── quality.rs       # Data quality scoring
│   │   └── correlations.rs  # Correlation analysis
│   ├── parsers/            # File format parsers
│   │   ├── csv.rs
│   │   ├── parquet.rs
│   │   ├── json.rs
│   │   └── excel.rs
│   ├── visualizers/        # Output formatters
│   │   ├── terminal.rs     # Terminal output
│   │   ├── html.rs         # HTML reports
│   │   └── charts.rs       # Chart generation
│   ├── exporters/          # Export formats
│   │   ├── schema.rs       # JSON Schema
│   │   ├── sql.rs          # SQL DDL
│   │   └── metadata.rs     # BI tool metadata
│   └── utils/              # Utilities
│       ├── sampling.rs     # Smart sampling
│       ├── streaming.rs    # Streaming processing
│       └── progress.rs     # Progress bars
├── tests/
├── benches/                # Performance benchmarks
└── examples/               # Usage examples
```

## 🚦 Roadmap di Sviluppo

### Fase 1: MVP (2-3 settimane)
1. **Core Engine**
   - [ ] Parser CSV con streaming
   - [ ] Analisi statistica base (min, max, mean, std)
   - [ ] Type inference
   - [ ] Output tabellare nel terminale

2. **Pattern Detection Base**
   - [ ] Email, telefoni, date
   - [ ] Numeric patterns
   - [ ] Missing data analysis

### Fase 2: Features Avanzate (4-6 settimane)
1. **Multi-formato**
   - [ ] Parquet support
   - [ ] JSON/JSONL support
   - [ ] Excel support

2. **Visualizzazioni**
   - [ ] ASCII charts
   - [ ] Correlation matrix
   - [ ] HTML report generation

3. **Quality & Patterns**
   - [ ] Anomaly detection
   - [ ] Business rules inference
   - [ ] Data quality scoring

### Fase 3: Professional Features (6-8 settimane)
1. **Performance**
   - [ ] Parallel processing
   - [ ] Smart sampling per file enormi
   - [ ] Memory-mapped processing

2. **Integrations**
   - [ ] JSON Schema export
   - [ ] SQL DDL generation
   - [ ] BI tools metadata
   - [ ] CI/CD mode

3. **Advanced**
   - [ ] Watch mode
   - [ ] Comparison mode
   - [ ] Custom rules engine

## 💻 Esempi di Utilizzo

### Caso 1: Analisi Veloce per Data Scientist
```bash
# Analisi rapida di un CSV
dataprof analyze sales_2024.csv --output-format terminal

# Export schema per validazione
dataprof analyze sales_2024.csv --export-schema sales_schema.json

# Genera DDL per database
dataprof analyze sales_2024.csv --export-sql postgres > create_table.sql
```

### Caso 2: Pipeline di Data Quality
```bash
# In uno script di CI/CD
if dataprof check data.csv --rules quality_rules.yaml; then
    echo "Data quality check passed"
    # Procedi con ETL
else
    echo "Data quality issues found"
    exit 1
fi
```

### Caso 3: Monitoring Continuo
```bash
# Watch mode per monitorare changes
dataprof watch incoming_data/ --on-change "notify-slack.sh"
```

### Caso 4: Report per Business Users
```bash
# Genera report HTML interattivo
dataprof analyze sales.csv --deep --output sales_report.html

# Genera data dictionary
dataprof document sales.csv --format markdown > data_dictionary.md
```

## 🔧 Configurazione

```yaml
# .dataprof.yaml
analysis:
  sampling:
    enabled: true
    max_rows: 1_000_000
    confidence: 0.95

  patterns:
    custom:
      - name: "Italian Fiscal Code"
        regex: "^[A-Z]{6}[0-9]{2}[A-Z][0-9]{2}[A-Z][0-9]{3}[A-Z]$"
      - name: "Order ID"
        regex: "^ORD-[0-9]{8}$"

  quality:
    thresholds:
      missing_data_warning: 0.05
      missing_data_error: 0.10
      cardinality_check: true

output:
  terminal:
    colors: true
    charts: true
    max_width: 120

  html:
    template: "corporate"
    include_raw_data: false
```

## 🎯 Vantaggi Competitivi

1. **Focus sul Profiling**: Non cerca di fare tutto come qsv, ma fa una cosa molto bene
2. **Output Business-Ready**: Report che possono essere condivisi direttamente con stakeholder
3. **Developer-Friendly**: Integrazione facile in pipeline esistenti
4. **Performance**: Sfrutta Rust per analisi velocissime anche su dataset enormi
5. **Intelligenza Built-in**: Pattern recognition e anomaly detection automatici

## 🏁 Getting Started

### Quick Start per Development
```bash
# Clone e setup
git clone https://github.com/tuousername/dataprof
cd dataprof

# Build
cargo build --release

# Run tests
cargo test

# Try it out
./target/release/dataprof analyze examples/sales_data.csv
```

### Prime Features da Implementare
1. **CSV Parser con Streaming** (2-3 giorni)
2. **Statistical Analysis Base** (2-3 giorni)
3. **Terminal Output Formatter** (1-2 giorni)
4. **Pattern Detection per Email/Phone** (2-3 giorni)
5. **Basic HTML Report** (2-3 giorni)

## 📚 Risorse e Ispirazioni

- **Performance**: Prendere spunto da qsv per le ottimizzazioni
- **Patterns**: Libreria di regex comuni per data italiana/europea
- **Visualizations**: Guardare `bottom` e `htop` per TUI best practices
- **Reports**: Ispirarsi a pandas-profiling per completezza

## 🤝 Contributi alla Community

Una volta stabile, il progetto potrebbe:
- Diventare una libreria riusabile oltre che CLI
- Integrarsi con progetti esistenti come Polars
- Fornire bindings per Python/R
- Creare plugin per IDE/editor

Questo progetto ha il potenziale per diventare il go-to tool per data profiling in Rust, colmando il gap tra tool command-line basilari e suite complete ma complesse.

## 🔧 Sviluppo e CI/CD

### Workflow GitHub Actions

Il progetto include workflow automatizzati per garantire qualità e facilità di distribuzione:

- **🧪 CI Workflow** (`.github/workflows/ci.yml`):
  - Test su Rust stable e MSRV (1.70.0)
  - Controlli formatazione (`cargo fmt`)
  - Linting avanzato (`cargo clippy`)
  - Test automatici su tutte le piattaforme
  - Security audit con `cargo audit`
  - Code coverage con `cargo-llvm-cov`

- **🚀 Release Workflow** (`.github/workflows/release.yml`):
  - Build automatico per Linux, Windows, macOS (x86_64 + ARM)
  - Creazione release GitHub con binari
  - Pubblicazione su crates.io
  - Release notes automatiche

- **⚡ Performance Workflow** (`.github/workflows/optimize.yml`):
  - Cache warming settimanale delle dipendenze
  - Profiling dei tempi di build
  - Check aggiornamenti dipendenze
  - Benchmark automatici

### Setup Sviluppo Locale

```bash
# Clone e setup
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof

# Install pre-commit hooks (opzionale)
pip install pre-commit
pre-commit install

# Build veloce per sviluppo
cargo build

# Test completi
cargo test

# Linting e formatting
cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

### Configurazioni Incluse

- **`.rustfmt.toml`**: Stile codice consistente
- **`.clippy.toml`**: Linting personalizzato per data processing
- **`.pre-commit-config.yaml`**: Hook automatici per qualità codice
