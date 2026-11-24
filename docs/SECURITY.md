# Security Policy

## Supported Versions

We actively support the following versions of dataprof:

| Version | Supported          |
| ------- | ------------------ |
| 0.14.x  | :white_check_mark: |
| 0.13.x  | Security fixes     |
| < 0.13  | :x:                |

## Reporting a Vulnerability

We take the security of dataprof seriously. If you discover a security vulnerability, please report it responsibly.

### How to Report

1. Do NOT create a public GitHub issue for security vulnerabilities
2. Use GitHub's private security advisory feature to report issues
3. Contact the maintainer through GitHub for urgent matters

### What to Include

When reporting a security vulnerability, please include:

- Description of the vulnerability
- Steps to reproduce the issue
- Potential impact and severity
- Any proposed fixes or mitigations
- Your contact information for follow-up

### Response Timeline

- Acknowledgment: We will acknowledge receipt within 48 hours
- Initial Assessment: We will provide assessment within 5 business days
- Status Updates: Progress updates every 7 days
- Resolution: We aim to resolve critical vulnerabilities within 30 days

### Disclosure Policy

- We will work with you to understand and resolve the issue quickly
- We will acknowledge your responsible disclosure publicly (with your permission)
- We will coordinate public disclosure timing with you

## Automated Checks

These run in GitHub Actions on this repository; each workflow file is public
and its runs are listed under the repository's Actions tab.

| Check | What it covers | When | Workflow |
| --- | --- | --- | --- |
| CodeQL | Static analysis of the Rust, Python, JavaScript/TypeScript and GitHub Actions code | Every pull request, every push to `master`, weekly | [`codeql.yml`](../.github/workflows/codeql.yml) |
| `cargo deny` | RustSec advisories, licences and sources of every crate, for the default and the all-features build | Every pull request except documentation-only ones, every push to `master`, weekly | [`security.yml`](../.github/workflows/security.yml) |
| Trivy | Filesystem scan for known vulnerabilities, high and critical | Same as `cargo deny` | [`security.yml`](../.github/workflows/security.yml) |
| Fuzzing | `cargo-fuzz` targets for the CSV and JSON readers, engine parity and saved-report loading, with an AddressSanitizer run | Pull requests and pushes that touch the Rust code, weekly | [`fuzz.yml`](../.github/workflows/fuzz.yml) |
| Semgrep | Static analysis by the Semgrep AppSec Platform | Pull requests | Semgrep GitHub app; no workflow file |
| Dependabot | Update pull requests for Cargo and uv dependencies (weekly) and GitHub Actions (monthly) | Scheduled | [`dependabot.yml`](../.github/dependabot.yml) |

Every third-party GitHub Action is pinned to a full commit SHA. Python wheels
are published to PyPI from the release workflow through trusted publishing, so
no PyPI token is stored in the repository.

## Dependency Auditing

A weekly scheduled run, and every pull request that can affect the dependency
graph, audit it with `cargo deny`. Documentation-only pull requests are skipped
by the workflow's `paths-ignore`; they cannot reach `Cargo.toml` or `Cargo.lock`,
and the weekly run covers advisories published against dependencies that have
not changed. The audit runs twice:

```bash
cargo deny check                # the default build
cargo deny --all-features check # every feature a user can enable
```

The second command exists because optional features are still *published*
features. The database connectors pull in the whole SQLx client stack, and the
async and Parquet features pull in a TLS stack; auditing only the default build
reported green while a shipped feature graph carried an unreviewed advisory.

`cargo audit` reads its own configuration, so the same dispositions are recorded
in both `deny.toml` and `.cargo/audit.toml`. Adding an advisory to one without
the other leaves the second tool failing on a decision that has already been
made.

### Accepted advisories

An advisory is only ignored when it has been read and judged not to apply, with
the reasoning recorded here and a date at which it is re-examined. "It is
inconvenient" is not a reason.

| Advisory | Crate | Disposition | Next review |
| --- | --- | --- | --- |
| [RUSTSEC-2026-0543](https://rustsec.org/advisories/RUSTSEC-2026-0543) | `chrono` 0.4.45 | Unmaintained, not a vulnerability; used directly and by arrow-rs and SQLx. See below | 2027-02-01 |
| [RUSTSEC-2026-0555](https://rustsec.org/advisories/RUSTSEC-2026-0555) | `chrono-tz` 0.10.4 | Unmaintained, not a vulnerability; transitive via arrow-array only. See below | 2027-02-01 |

#### Resolved: RUSTSEC-2023-0071 and RUSTSEC-2026-0459

Releases up to 0.14.x build SQLx 0.8 and carry two advisories that were
accepted with a recorded reason: RUSTSEC-2023-0071 (Marvin attack in `rsa`
0.9), which did not apply because the MySQL client only encrypts with the
server's public key, and RUSTSEC-2026-0459 (SQLite page cache in
`libsqlite3-sys` 0.30.1), which applied to the `sqlite` feature when profiling
an untrusted database file together with its `-wal` file. The `sqlite` feature
is not in the default build or the Python wheels.

SQLx 0.9.1 resolves both: it takes the fixed `rsa` 0.10 and allows the fixed
`libsqlite3-sys` 0.39 and later. Releases after 0.14.x build SQLx 0.9.1, and
neither advisory is in their dependency graph. On 0.14.x, do not profile an
untrusted SQLite database that comes with a `-wal` file.

#### RUSTSEC-2026-0543 and RUSTSEC-2026-0555 (`chrono`, `chrono-tz` unmaintained)

Both advisories mark the crates unmaintained
([chronotope/chrono#1768](https://github.com/chronotope/chrono/issues/1768)).
Neither reports a vulnerability, and no fixed version exists. They first failed
the audit on 2026-11-17.

dataprof parses, normalizes and formats dates with `chrono` in most of its
crates. Its dependencies use it too: arrow-rs (`arrow-arith`, `arrow-array`,
`arrow-cast`, `arrow-csv`, `arrow-json`, `parquet`) and SQLx. `chrono-tz`
reaches the graph only through `arrow-array`; dataprof does not depend on it.
Moving dataprof's own date code to `jiff` or `time` would therefore leave both
crates in the graph.

This will be revisited if a vulnerability is reported against either crate,
if `chrono` gains a maintainer, if arrow-rs moves off it, or by 2027-02-01,
whichever comes first.

## Security Best Practices

When using dataprof:

- Keep your Rust toolchain updated
- Use the latest version of dataprof
- Be cautious when analyzing untrusted data files
- Review generated HTML reports before sharing them
- Use appropriate file permissions for sensitive data

## Security Features

dataprof includes several security considerations:

- Local file profiling makes no network connections; data is processed locally only
- Remote URL profiling (`async-streaming` / `parquet-async` features) and database
  profiling (`postgres`, `mysql`, `sqlite` features) do make outbound network
  connections to the configured endpoints — review URLs and connection strings
  before profiling untrusted sources
- HTML reports contain only analysis results, not raw data
- No persistent storage of analyzed data
- Memory-safe Rust implementation

Thank you for helping keep dataprof secure!
