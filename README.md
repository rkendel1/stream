# Stream

Stream is a local-first information runtime built on FeltDB and AppPort.

It is **not** an RSS reader, though RSS is an explicit first-class ingestion protocol. The product is the durable runtime that turns RSS, Atom, JSON Feed, websites, GitHub, YouTube, email, APIs, and other connected sources into normalized, queryable, user-controlled state.

## What this repository contains

This MVP foundation uses the requested stack:

- `npx create-feltdb` as the starting scaffold pattern for the application layout
- `@feltdb/core` as the durable state runtime entry point
- `feltdb.flow` as the authoritative application model
- `@appport/sdk` to expose the portable Stream capability surface

## Authoritative model

`/home/runner/work/stream/stream/feltdb.flow` is the canonical product model. It defines the durable collections and capability boundaries for:

- sources
- normalized items
- source-to-item provenance
- item relations and deduplication support
- durable read/save/dismiss state
- rules and rule execution history
- subscriptions
- semantic decisions
- attention events
- fetch attempts and failure state

## Library surface

`/home/runner/work/stream/stream/src/stream.js` contains the initial domain helpers for:

- canonical source records
- canonical item records
- stable fingerprinting for deduplication
- search filtering
- attention summaries

`/home/runner/work/stream/stream/src/appport.js` defines the portable AppPort capabilities:

- `stream.source`
- `stream.item`
- `stream.query`
- `stream.search`
- `stream.attention`

These capabilities keep Stream transport-neutral and make the relationship to AppPort explicit.

## CLI surface

`/home/runner/work/stream/stream/src/cli.js` establishes the first-class CLI command surface described in the product specification, including `stream doctor` for observable failure reporting.

## Validation

```bash
npm install
npm test
npm run feltdb:validate
```
