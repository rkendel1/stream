# Stream

Stream is a local-first information runtime built as a Rust workspace.

It is **not** an RSS reader. RSS, Atom, and JSON Feed are ingestion adapters for a durable runtime that turns external information into normalized, queryable, user-controlled state.

## Architecture

- `feltdb.flow` is the authoritative durable model.
- the Rust runtime owns ingestion, normalization, deduplication, rules, search, and attention logic.
- FeltDB remains the durable state layer through the installed `@feltdb/core` package.
- AppPort remains the portable capability surface.

## Workspace

- `crates/stream-model` — typed identifiers, enums, domain records, fingerprinting
- `crates/stream-core` — FeltDB bridge, runtime service, durable state operations, attention summaries
- `crates/stream-ingest` — source adapter boundary and fetch pipeline
- `crates/stream-rss` — RSS, Atom, and JSON Feed adapters
- `crates/stream-query` — query/search helpers over durable state
- `crates/stream-rules` — minimal rule evaluation
- `crates/stream-appport` — AppPort capability manifest surface
- `crates/stream-cli` — `stream` executable

## Validation

```bash
npm install
npm run feltdb:validate
cargo test --workspace
node --test tests/appport-surface.test.mjs
```
