## What changed

<!-- The behaviour or problem, in a sentence or two. What does a user get that
     they did not have before, or what stops going wrong? -->

**Related issue:** Closes #

## How it was verified

<!-- Replace the example below with the commands you actually ran and what they
     reported. Run the checks that match what you touched; a docs change does
     not need the workspace. -->

```console
$ cargo test -p dataprof-core
test result: ok. 156 passed; 0 failed
```

<details>
<summary>Commands by area (expand for the usual ones)</summary>

**Rust** — CI pins Rust 1.98, so prefer `cargo +1.98` when your local toolchain differs:

```bash
cargo test -p <crate-you-changed>
cargo fmt --all
cargo clippy --all --all-targets -- -D warnings
```

**Python:**

```bash
uv run maturin develop
uv run pytest python/tests/<file-you-changed>.py -q
uv run ruff format python/ .github/scripts/ .claude/skills/dataprof/scripts/
uv run ruff check python/ .github/scripts/ .claude/skills/dataprof/scripts/
uv run ty check python/
```

**Docs or examples** — build the examples you touched; CI runs them:

```bash
cargo run --example <example-you-changed>
```

**Serialized report schema** — only if you changed a `ProfileReport` field or its
Serde attributes. See the
[report schema release checklist](https://github.com/AndreaBozzo/dataprof/blob/master/docs/CONTRIBUTING.md#report-schema-release-checklist):

```bash
cargo run --example generate_profile_schema
cargo test --test profile_report_schema
```

</details>

## Anything reviewers should know

<!-- Delete the lines that do not apply. -->

- **Breaking change:** what breaks, and what a user does instead.
- **Feature flags:** which flags gate this, and what happens without them.
- **Generated schema:** the regenerated artifact is included in this PR.

---

<!-- One behaviour change, or one documentation/example slice, per PR. If an
     issue lists several scenarios, implementing one and leaving follow-ups is
     welcome. -->

New here? [docs/CONTRIBUTING.md](https://github.com/AndreaBozzo/dataprof/blob/master/docs/CONTRIBUTING.md)
covers setup and the review process, and
[AGENTS.md](https://github.com/AndreaBozzo/dataprof/blob/master/AGENTS.md)
is the condensed version of the same conventions.
