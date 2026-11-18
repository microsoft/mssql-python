# mssql-python 1.18.0

This release improves fetching and repeated-query performance, adds custom application names, and strengthens pooled connection and Arrow reader reliability.

## Release Highlights

### Faster Fetching and Repeated Queries (#847, #858, #852, #891, #888, #886)

- **What changed:** Reuse row-mapping metadata and cursor-scoped fetch state, reduce output-converter overhead, cache pyformat conversion, and retain compatible prepared-statement bindings.
- **Who benefits:** Applications converting rows to dictionaries, fetching results one row at a time, using output converters, or repeatedly executing parameterized SQL.
- **Impact:** Less repeated preparation, conversion, and allocation work while retaining compatibility fallbacks and existing result semantics.

> PR #847 | PR #858 | PR #852 | PR #891 | PR #888 | PR #886 | Thanks @dxdc for the pyformat contribution.

```bash
pip install --upgrade mssql-python
```

[View mssql-python 1.18.0 on PyPI](https://pypi.org/project/mssql-python/1.18.0/)

[View mssql-python on Conda (Microsoft channel)](https://anaconda.org/channels/Microsoft/packages/mssql-python/overview)

## Enhancements

- Custom Application Names ([#884](https://github.com/microsoft/mssql-python/pull/884))

What changed: Connection strings now accept `APP` and `Application Name`, sharing the effective application name across synchronous, async, and bulk-copy paths.
Who benefits: Applications using server-side monitoring or telemetry to distinguish workloads.
Impact: Custom names appear as `MSSQL-Python|<user name>`. The async and bulk-copy default changes from `mssql-python` to `MSSQL-Python`; exact-name telemetry filters may need updating.

> PR [#884](https://github.com/microsoft/mssql-python/pull/884) | GitHub Issue [#484](https://github.com/microsoft/mssql-python/issues/484)

- Faster Row-to-Dictionary Conversion ([#847](https://github.com/microsoft/mssql-python/pull/847))

What changed: Reuse row-mapping metadata and native mapping infrastructure when converting fetched rows to dictionaries.
Who benefits: Applications that frequently call `dict(row._mapping)`, particularly on wide results.
Impact: Row-to-dictionary conversion spends less work rebuilding mapping information.

> PR [#847](https://github.com/microsoft/mssql-python/pull/847)

- Faster Single-Row Fetching ([#858](https://github.com/microsoft/mssql-python/pull/858))

What changed: Reuse cursor-scoped native state for eligible `fetchone()`, `fetchmany(1)`, `fetchval()`, and iteration paths, retaining Python fallbacks.
Who benefits: Applications that consume results one row at a time.
Impact: Eligible fetches avoid repeated Python-side dispatch and preparation while preserving custom behavior.

> PR [#858](https://github.com/microsoft/mssql-python/pull/858)

- Faster Output-Converter Batching ([#852](https://github.com/microsoft/mssql-python/pull/852))

What changed: Reduce conversion and row-construction overhead when fetching batches with output converters.
Who benefits: Applications using output converters with `fetchall()` or batch-oriented `fetchmany()`.
Impact: Eligible callbacks receive their bytes without unnecessary intermediate string conversion, preserving callback order and NULL handling.

> PR [#852](https://github.com/microsoft/mssql-python/pull/852)

- Faster Size-One Text Fetches ([#891](https://github.com/microsoft/mssql-python/pull/891))

What changed: Avoid rebuilding text-fetch preparation after ordinary row-count updates during `fetchmany(1)`.
Who benefits: Applications fetching text results one row at a time.
Impact: Repeated size-one fetches reuse the established native path without prefetching rows.

> PR [#891](https://github.com/microsoft/mssql-python/pull/891)

- Faster Pyformat Conversion ([#888](https://github.com/microsoft/mssql-python/pull/888))

What changed: Convert pyformat placeholders in a single pass and cache converted statements.
Who benefits: Applications repeatedly executing pyformat SQL, including wide SQLAlchemy insert batches.
Impact: Repeated statements avoid rescanning and replacing the SQL once for each parameter.

> PR [#888](https://github.com/microsoft/mssql-python/pull/888) - Thanks [@dxdc](https://github.com/dxdc) for the contribution!

- Prepared Statement Reuse ([#886](https://github.com/microsoft/mssql-python/pull/886))

What changed: Retain parameter bindings when re-executing the same prepared SQL with compatible parameter definitions.
Who benefits: Applications repeatedly executing parameterized statements on the same cursor.
Impact: Drivers can reuse the prepared statement with `sp_execute` instead of preparing it again on every call.

> PR [#886](https://github.com/microsoft/mssql-python/pull/886) | GitHub Issue [#885](https://github.com/microsoft/mssql-python/issues/885)

- Faster Rust-Backed Execution ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Reduce SQL-text scanning, transcoding, and RPC parameter-serialization overhead in Rust-backed execution paths.
Who benefits: Applications using the opt-in Rust ODBC provider or the native async core, especially with parameter-heavy statements.
Impact: Eligible workloads avoid repeated text passes, per-parameter allocations, and unnecessary async serialization work. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

- macOS Strict TLS Improvements ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Rust-backed `Encrypt=Strict` connections on macOS use Apple's Network.framework TLS engine.
Who benefits: Applications using Rust-backed Strict connections on supported macOS hosts.
Impact: Connections can negotiate TLS 1.3, with post-quantum key exchange offered on macOS 26 and later. Non-Strict connections retain their existing TLS path. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

- Rust ODBC Table-Valued Parameters ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Add table-valued parameter binding, materialized tables, and buffered data-at-execution support to the opt-in Rust ODBC provider.
Who benefits: Workloads using table-valued parameters through that provider.
Impact: The Rust ODBC provider can send structured table parameters, including supported NULL and streamed cell values. This does not change the default ODBC provider. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

## Bug Fixes

- Synapse Pooled Connection Close ([#889](https://github.com/microsoft/mssql-python/pull/889))

What changed: Remove the unsupported READ COMMITTED reset from the autocommit-on pool-return rollback batch.
Who benefits: Applications using pooled connections to Azure Synapse dedicated SQL pools.
Impact: Closing a connection no longer fails because that cleanup batch attempts an unsupported isolation-level reset.

> PR [#889](https://github.com/microsoft/mssql-python/pull/889) | GitHub Issue [#882](https://github.com/microsoft/mssql-python/issues/882)

- Explicit Pooled Checkout Timeouts ([#897](https://github.com/microsoft/mssql-python/pull/897))

What changed: Pooled checkout respects an explicit positive connection timeout when it is shorter than the login-derived validation timeout, then restores the original value after successful validation.
Who benefits: Applications that configure a short connection timeout to bound stale pooled-socket checks.
Impact: Validation no longer replaces that explicit bound with a longer login-derived timeout.

> PR [#897](https://github.com/microsoft/mssql-python/pull/897) | GitHub Issue [#893](https://github.com/microsoft/mssql-python/issues/893)

- Arrow Reader Cross-Thread Shutdown ([#892](https://github.com/microsoft/mssql-python/pull/892))

What changed: Coordinate close and cancellation with complete Arrow batch operations, and prevent stale readers from affecting subsequent cursor results.
Who benefits: Applications closing Arrow readers from another thread or reusing their parent cursor.
Impact: Shutdown avoids the idle-cancellation race while preserving unrelated errors and protecting later result sets.

> PR [#892](https://github.com/microsoft/mssql-python/pull/892) | GitHub Issue [#890](https://github.com/microsoft/mssql-python/issues/890)

- Correct Pyformat Placeholder Handling ([#888](https://github.com/microsoft/mssql-python/pull/888))

What changed: Preserve placeholder-like text inside SQL literals, quoted or bracketed identifiers, and comments; handle escaped percent signs consistently.
Who benefits: Applications using pyformat SQL containing quoted text, comments, or escaped percent signs.
Impact: Conversion no longer rewrites those protected regions or creates mismatched parameter values for escaped placeholders.

> PR [#888](https://github.com/microsoft/mssql-python/pull/888) - Thanks [@dxdc](https://github.com/dxdc) for the contribution!

- Rust-Backed Connection Reliability ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Fix connection-scoped and pooled-checkout timeout handling, descriptor lifetime and prepared-plan invalidation races, prepared-handle cleanup after cancellation, isolation-change cursor preservation, and connection stack overflow on small stacks.
Who benefits: Applications exercising those connection and statement operations through Rust-backed paths.
Impact: Applicable timeout budgets are honored, valid cursor state is preserved, and affected lifetime, cleanup, and small-stack failures are avoided. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

- Rust ODBC Streaming Order ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Send data-at-execution values in their bound positions rather than moving them after materialized parameters.
Who benefits: Applications using streamed parameters followed by parameter-array execution on the same statement with the opt-in Rust ODBC provider.
Impact: Reused prepared plans retain the correct value-to-parameter ordering. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

- SQL_VARIANT Length Validation ([#896](https://github.com/microsoft/mssql-python/pull/896))

What changed: Reject oversized scalar SQL_VARIANT payload lengths before narrowing them in Rust-backed decoding.
Who benefits: Applications receiving malformed scalar SQL_VARIANT frames through Rust-backed paths.
Impact: Malformed lengths produce a protocol error instead of wrapping and leaving unread payload bytes; valid values remain unchanged. *(via `mssql-python-rs`)*

> PR [#896](https://github.com/microsoft/mssql-python/pull/896)

## Contributors

[@dxdc](https://github.com/dxdc)
