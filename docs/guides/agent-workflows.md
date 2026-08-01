# Agent Workflows

These snippets teach coding agents how to use `dataprof` without dumping raw rows into chat. Copy the format that matches your agent runner.

## AGENTS.md snippet

```markdown
## dataprof workflow

When analyzing tabular data with dataprof:

1. Call `dp.capabilities()` before profiling Parquet, a URL, or a database -- optional features are compiled in, not always present.
2. Start with `dp.analyze_structure(path)` for a cheap first pass over columns, row shape, and obvious structural issues.
3. Use `dp.profile(path)` for full profiling. It computes every metric pack by default; pass `metrics=[...]` only to narrow it to a subset of `schema`, `statistics`, `patterns`, `quality`.
4. Before reporting any number, check `report.sampling_applied`, `report.truncation_reason`, `report.source_exhausted`, `report.low_sample_warning`, `report.error_count`, and `report.ragged_row_count`. A truncated or sampled read still returns confident-looking numbers.
5. Export compact context with `report.to_llm_context()` -- the only export that enforces redaction -- or `report.to_markdown()`, `report.quality_summary()`, or the top-level fields of `report.to_dict()` (`source`, `source_type`, `execution`, `quality`); its `columns` entry grows with table width.
6. Use `report.compare(other_report)` for before/after drift, pipeline changes, or data-cleaning validation.
7. Prefer schema summaries, quality metrics, and selected column details over raw row dumps.

`None` means "not analyzed" and empty means "analyzed, found nothing" -- never present a `None` score as perfect or replace it with zero.

Always report the source path, metrics requested, and any sampling or max-row limit.
```

## Cursor rule

Copy `.cursor/rules/dataprof.mdc` into a project that uses Cursor. It contains the same workflow in Cursor rule form. Keep the `.mdc` extension and its frontmatter -- a plain `.md` file in `.cursor/rules/` is ignored by Cursor.

## Claude Code skill

Copy this repo's `.claude/skills/dataprof/` directory so that it lands at `.claude/skills/dataprof/SKILL.md` in your own project, or at `~/.claude/skills/dataprof/SKILL.md` to make it available everywhere. Keep the `dataprof/` directory -- a bare `.claude/skills/SKILL.md` will not load.

It packages the structure -> profile -> summarize -> compare workflow as on-demand knowledge, and it loads from that path in this repo too.

## Agent-safe output

`ProfileReport.to_llm_context()` is the preferred summary for chat or MCP-style agent surfaces. By default it emits dataset shape, caveats, quality flags, schema, and detected pattern names, but no raw cell values.

`include_samples=True` is an explicit opt-in for non-sensitive numeric extrema only. If a column has a detected sensitive pattern, such as email, phone, identifier, financial, geographic, network, or file-path data, `to_llm_context()` still withholds the concrete values and reports only the pattern name and counts.

## Why this order

`analyze_structure()` is the cheap first look. It helps an agent avoid over-reading a dataset before it knows the shape. `profile()` is the full metrics pass. `to_llm_context()` is the safest chat-facing summary; `to_markdown()`, `quality_summary()`, and the top-level fields of `to_dict()` are useful when a more structured export is needed. `compare()` is the right tool when the question is about drift or whether a cleaning step helped.
