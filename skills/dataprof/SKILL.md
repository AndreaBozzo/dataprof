---
name: dataprof
description: Profile tabular data with dataprof before debugging schema, quality, drift, or data-cleaning questions.
---

# dataprof

Use this skill when the user asks you to understand an unfamiliar dataset, inspect data quality, compare two dataset versions, or prepare compact evidence for a data-cleaning or pipeline decision.

## Workflow

1. Identify the dataset path and format.
2. Run a cheap structure pass first:

   ```python
   import dataprof as dp

   structure = dp.analyze_structure("data.csv")
   ```

3. Run the full profile when structural inspection is not enough:

   ```python
   report = dp.profile("data.csv", metrics=["schema", "statistics", "quality"])
   ```

4. Summarize for the user or another agent with compact outputs:

   ```python
   report.to_markdown()
   report.quality_summary()
   report.to_dict()
   ```

5. Compare reports for before/after drift:

   ```python
   before = dp.profile("data_before.csv")
   after = dp.profile("data_after.csv")
   delta = before.compare(after)
   ```

## Guardrails

- Prefer aggregates, schema summaries, quality metrics, and selected column details over raw row dumps.
- Do not paste large raw datasets into the conversation.
- State the source path, metrics, sampling, and max-row limits used.
- If the dataset may be sensitive, keep the work local and share only derived summaries.

## Useful APIs

- `dp.analyze_structure(path, max_rows=None)`
- `dp.profile(source, metrics=[...])`
- `report.to_markdown()`
- `report.quality_summary()`
- `report.to_dict()`
- `report.compare(other_report)`
