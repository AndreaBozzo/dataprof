# Security Policy

## Supported Versions

We actively support the following versions of DataProfiler:

| Version | Supported          |
| ------- | ------------------ |
| 0.1.x   | :white_check_mark: |

## Reporting a Vulnerability

We take the security of DataProfiler seriously. If you discover a security vulnerability, please report it responsibly.

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

## Security Best Practices

When using DataProfiler:

- Keep your Rust toolchain updated
- Use the latest version of DataProfiler
- Be cautious when analyzing untrusted data files
- Review generated HTML reports before sharing them
- Use appropriate file permissions for sensitive data

## Security Features

DataProfiler includes several security considerations:

- No network connections are made during analysis
- Data is processed locally only
- HTML reports contain only analysis results, not raw data
- No persistent storage of analyzed data
- Memory-safe Rust implementation

Thank you for helping keep DataProfiler secure!
