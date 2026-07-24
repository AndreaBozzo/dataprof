# dataprof-python

PyO3 extension bindings for the `dataprof` Python package.

This crate owns Python function registration, configuration and report
wrappers, Arrow export, progress callbacks, and optional async and database
bindings. Core profiling behavior remains in the Rust facade and its component
crates. Python users install the `dataprof` package rather than depending on
this crate directly.

## Public surface

The extension module exposes file and column analysis, partial analysis,
pattern discovery, report wrappers, and feature-gated async and database APIs.

## Features

- `python`: synchronous extension marker.
- `python-async` / `async-streaming`: async Python APIs.
- `parquet-async`: async Parquet APIs.
- `database`: shared database bindings.
- `postgres`, `mysql`, `sqlite`: individual database backends.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-python --all-features
```

See the
[`dataprof` Python and Rust overview](https://github.com/AndreaBozzo/dataprof/blob/master/README.md)
and the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md).
