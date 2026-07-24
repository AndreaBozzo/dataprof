# dataprof-db

Database connectivity and query profiling support for the `dataprof` workspace.

This crate owns connector configuration and traits, PostgreSQL, MySQL, and
SQLite implementations, SQL validation and credential safety, database
sampling, retries, and batched streaming helpers. It does not own generic
profile metrics or Python bindings.

## Public surface

The primary entry points are `DatabaseConfig`, `DatabaseConnector`,
`create_connector`, `analyze_database`, and the backend connector types.

## Features

- `postgres`: PostgreSQL support.
- `mysql`: MySQL/MariaDB support.
- `sqlite`: SQLite support.
- `all-db`: all three backends.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-db --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
