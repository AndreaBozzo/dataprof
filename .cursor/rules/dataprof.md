# dataprof Agent Workflow

Use this rule when working with tabular data, data-quality checks, or drift investigations in a project that uses `dataprof`.

## Workflow

1. Start with `dp.analyze_structure(path)` for a cheap first pass over columns, row shape, and obvious structural issues.
2. Run `dp.profile(path, metrics=["schema", "statistics", "quality"])` when you need full profiling metrics.
3. Export compact context with `report.to_markdown()`, `report.quality_summary()`, or selected fields from `report.to_dict()`.
4. Use `report.compare(other_report)` for before/after drift, pipeline changes, or data-cleaning validation.
5. Prefer summaries and quality metrics over raw row dumps.

## Guardrails

- Do not paste large raw datasets into chat.
- Do not infer data quality from the first few visible rows.
- Mention the source path, metrics requested, and any sampling or max-row limit when reporting results.
- If data may be sensitive, keep the analysis local and share only aggregates.
