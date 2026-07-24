# dataprof-metrics

Statistical analysis and data-quality metrics for the `dataprof` workspace.

This crate owns column analysis, type and pattern inference, statistical
summaries, approximate cardinality, validators, and ISO-oriented quality
assessments. It does not read input formats or choose profiling engines.

## Public surface

Common entry points include `MetricsCalculator`, `analyze_column`,
`detect_patterns`, `infer_type`, `QualityAssessment`,
`CardinalityEstimator`, and the numeric, text, and datetime summary functions.

## Features

This crate has no feature flags.

## Development

```bash
cargo test -p dataprof-metrics
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
