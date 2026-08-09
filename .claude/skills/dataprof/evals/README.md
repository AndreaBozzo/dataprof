# Skill evaluations

Four scenarios that check the skill changes agent behavior, each aimed at a
step that is easy to skip. They are graded by reading the agent's answer, not by
asserting on dataprof's output — the library already has its own tests. What is
under test here is whether an agent *reports honestly*.

| Scenario | Skill step under test |
| --- | --- |
| `ragged-read-is-reported` | Step 4 — trust signals checked before numbers are reported |
| `sensitive-values-stay-local` | Step 5 — `to_llm_context()` by default, redaction end to end |
| `drift-uses-compare` | Step 6 — `compare()` rather than two hand-read profiles |
| `high-score-is-not-clean` | Reading the output honestly — a high score that is not evidence |

## Running them

There is no built-in runner. Each scenario is a prompt plus a rubric:

1. **Baseline.** Run the query in a session with the skill disabled. Record what
   the agent did. This is the number the skill has to beat; without it you are
   guessing.
2. **With the skill.** Run the same query in a fresh session with the skill
   available.
3. **Grade.** Every line in `expected_behavior` should hold; any line in
   `fails_if` means the scenario failed.
4. **Repeat per model.** A skill that works on Opus can under-specify for
   Haiku. Run all four scenarios on each model you intend to use.

Paths in `query` are relative to this directory.

## The fixtures

`fixtures/ragged_orders.csv` — 7 data rows, of which 2 have the wrong field
count, one `amount` is non-numeric, and one `ordered_at` is in the future.
Profiling yields `ragged_row_count=2` and `future_dates_count=1`, while the
column statistics still look perfectly presentable. That gap is the point.

`fixtures/customers_pii.csv` — email, phone, and IBAN columns. dataprof detects
`Email`, `Phone (IT)`, and `IBAN`, and `to_llm_context()` withholds the values
even when `include_samples=True`. The scenario checks the agent does not route
around that with `to_dict()` or a raw read.

`fixtures/inventory_before.csv` / `inventory_after.csv` — a cleaning step that
genuinely improves quality (duplicate rows 1 to 0, missing values 22.2% to 0%,
score 80.9 to 97.9) while dropping half the rows. An agent that reports only the
score improvement has missed the row loss.

`fixtures/payments_mixed_amount.csv` — 10 payment rows where 4 of the
`amount_eur` values are placeholders (`pending`, `n/a`, `see invoice`,
`awaiting PO`). Past the 80% numeric threshold, type inference gives up and calls
the column `string`. Consistency now scores that column on its dominant lexical
class, so it contributes 6 of 10 values and the file scores **97.9/100** rather
than the 100.0/100 it reported before #544 was fixed. That is still high enough
to read as clean, and `to_llm_context()` still shows no per-column flag (#561), so
a money column carrying text remains a finding the score alone will not make for
you.

## When these change

Add a scenario whenever an agent gets something wrong in real use — that
observation is worth more than an imagined case. If a fixture's numbers move
because profiling changed, update the cited values here and in
`scenarios.json`; a rubric quoting stale numbers grades against fiction.
