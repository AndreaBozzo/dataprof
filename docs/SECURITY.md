# Security Policy

## Supported Versions

We actively support the following versions of dataprof:

| Version | Supported          |
| ------- | ------------------ |
| 0.9.x   | :white_check_mark: |
| 0.8.x   | Security fixes     |
| < 0.8   | :x:                |

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

## Dependency Auditing

Every pull request and a weekly scheduled run audit the dependency graph with
`cargo deny`, twice:

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
| [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071) | `rsa` 0.9.x | Not applicable — see below | 2026-10-01 |
| [RUSTSEC-2024-0436](https://rustsec.org/advisories/RUSTSEC-2024-0436) | `paste` 1.0.15 | Unmaintained, not a vulnerability; transitive via arrow-rs, no replacement available | 2026-10-01 |

#### RUSTSEC-2023-0071 (Marvin attack in `rsa`)

The advisory describes a timing sidechannel through which an attacker who can
measure RSA **private-key** operations may recover the private key. It has no
fixed release ([RustCrypto/RSA#626](https://github.com/RustCrypto/RSA/issues/626)).

dataprof reaches `rsa` only through `sqlx-mysql`, and only when the `mysql`
feature is enabled. That crate's entire use of it is in `connection/auth.rs`,
which imports `RsaPublicKey` and encrypts the password with OAEP during
`sha256_password` / `caching_sha2_password` authentication. It never constructs
an `RsaPrivateKey`, and never decrypts or signs.

So the advisory does not apply, for two independent reasons:

1. **No private-key operation exists to attack.** dataprof is a MySQL client. It
   encrypts with the server's public key and holds no RSA private key of its
   own. The Marvin attack targets the party performing private-key operations —
   here, the MySQL server, whose RSA implementation is not this crate.
2. **The path is normally not taken at all.** dataprof builds SQLx with
   `runtime-tokio-rustls`, and `encrypt_rsa` returns before touching RSA when
   the connection is TLS.

This will be revisited if a fixed `rsa` ships, if SQLx drops the dependency, or
by 2026-10-01, whichever comes first.

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
