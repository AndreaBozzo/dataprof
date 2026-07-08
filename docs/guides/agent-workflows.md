# Agent Workflows

These snippets teach coding agents how to use `dataprof` without dumping raw rows into chat. Copy the format that matches your agent runner.

## AGENTS.md snippet

```markdown
## dataprof workflow

When analyzing tabular data with dataprof:

1. Start with `dp.analyze_structure(path)` for a cheap first pass over columns, row shape, and obvious structural issues.
2. Use `dp.profile(path)` for full profiling. It computes every metric pack by default; pass `metrics=[...]` only to narrow it to a subset of `schema`, `statistics`, `patterns`, `quality`.
3. Export compact context with `report.to_markdown()`, `report.quality_summary()`, or the top-level fields of `report.to_dict()` (`source`, `source_type`, `execution`, `quality`) -- its `columns` entry grows with table width.
4. Use `report.compare(other_report)` for before/after drift, pipeline changes, or data-cleaning validation.
5. Prefer schema summaries, quality metrics, and selected column details over raw row dumps.

Always report the source path, metrics requested, and any sampling or max-row limit.
```

## Cursor rule

Copy `.cursor/rules/dataprof.mdc` into a project that uses Cursor. It contains the same workflow in Cursor rule form. Keep the `.mdc` extension and its frontmatter -- a plain `.md` file in `.cursor/rules/` is ignored by Cursor.

## Claude Code skill

Copy `.claude/skills/dataprof/` into your own project's `.claude/skills/`, or into `~/.claude/skills/` to make it available everywhere. It packages the structure -> profile -> summarize -> compare workflow as on-demand knowledge, and it loads from that path in this repo too.

## Why this order

`analyze_structure()` is the cheap first look. It helps an agent avoid over-reading a dataset before it knows the shape. `profile()` is the full metrics pass. `to_markdown()`, `quality_summary()`, and the top-level fields of `to_dict()` keep the output compact enough for an LLM. `compare()` is the right tool when the question is about drift or whether a cleaning step helped.
