# turbograph Rust API

Rust handle for the turbograph tiered VFS — wraps Kuzu UDFs with an async Rust API.

## Extension-loaded tests

Tests require the turbograph Kuzu extension to be statically linked:

```bash
cd rust_api
LBUG_STATIC_EXTENSIONS=turbograph TURBOGRAPH_DIR=$PWD/.. cargo test --features extension-tests
```

Without the feature flag, `cargo check` and `cargo test` work but skip extension-dependent tests.
