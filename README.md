# Stream

Stream is **not** an RSS reader.

You give Stream things you want to know about — any URL — and it builds an
increasingly useful understanding of what is changing around you and how it
connects to the things you are building. The north star is information
density, not information volume: maximize useful change per unit of human
attention.

```
Add URL → Fetch / Observe → Understand → Topic · Subject · Change
        → Why it matters → Connections → Durable, evidence-backed Signal
```

The URL is the input. Whether it is a web page, a blog, GitHub, a paper,
documentation, a YouTube page, or an RSS/Atom/JSON feed, Stream works out how to
understand and observe it.

## Quick start

```bash
npm install                           # FeltDB + AppPort
cargo run -p stream-desktop --release # the Stream desktop app
```

1. **+ Context** — tell Stream what you care about (`Portable compute`, a project like `AppPort`, a concern like `Customer pain`). Relate contexts to each other.
2. **+ Add URL** — paste a URL. Stream shows *Fetching source → Understanding content → Finding connections → Building signal*.
3. **Today** shows signals, not raw items: topic, subject, what changed, why it may matter to you, and what it connects to.
4. **View evidence** / **Why here?** — every claim quotes the source it came from (excerpt → item → source → URL), and the ranking explains itself factor by factor.
5. Add another URL about the same change: Stream recognizes it and adds it as corroborating evidence to the existing signal instead of showing a second card.
6. Quit and relaunch: everything is still there, because it lives in FeltDB.

The same state is available from the CLI and from AppPort:

```bash
cargo install --path crates/stream-cli   # installs `stream`
stream context add "Portable compute" -d "Running workloads anywhere" --related AppPort
stream add https://example.com/article
stream signals                  # Today, ranked by information density
stream signal <signal-id>       # evidence, connections, and why it is ranked there
stream context list
stream connections [<id>]       # a signal's/item's/context's connections, or the graph
stream sources
stream appport invoke stream.signal.list '{}'
```

### Think with Stream

The **Chat** tab (and `stream ask`, and `stream.chat.ask` over AppPort) answers
questions about what Stream knows — not general chat. Every answer:

- is built from a bundle Stream retrieves for the question (signals, evidence,
  connections, contexts, timeline, saved insights), never the whole database;
- labels each statement **Observed** (a source states it), **Connected**,
  **Inferred**, or **Hypothesis**, and cites the evidence behind it;
- says plainly when the evidence is insufficient or only partial;
- can be drilled into, down to the quoted source text and URL.

"Ask Stream" on any signal, source, or context constrains retrieval to it.
Conversations live only in the window; **Save insight** turns useful reasoning
into a durable, advisory Stream object (insight, hypothesis, question,
investigation, decision candidate) with its question, evidence, and contexts.

```bash
stream ask "Why does this matter to portable compute?"
stream ask "What remains uncertain?" --signal <signal-id>
stream ask "What connects to AppPort?" --save insight
stream insights
```

### Model-backed intelligence (optional)

Stream boots and works with no model: a deterministic local interpreter,
synthesizer, and reasoner. To add a model, point Stream at any
OpenAI-compatible endpoint — local (Ollama, llama.cpp server, LM Studio, vLLM)
or remote:

```bash
STREAM_MODEL_PROVIDER=ollama STREAM_MODEL=llama3.2 cargo run -p stream-desktop
# or: STREAM_MODEL_PROVIDER=openai-compatible STREAM_MODEL_BASE_URL=https://… STREAM_MODEL=… STREAM_MODEL_API_KEY=…
```

The model receives source metadata, the item, your relevant contexts, and
relevant existing Stream knowledge, and must answer in a strict JSON schema.
Everything it proposes passes the evidence gate: a quote that is not verbatim
in the source is discarded along with the claim that depends on it; answers
may cite only retrieved evidence. If the model fails or returns malformed
output, Stream falls back to local intelligence and records a durable
`IntelligenceEvent` (`stream doctor`, `stream.intelligence.status`). No part
of Stream above the provider layer — and nothing on AppPort or in the UI —
knows which provider is active.

`stream-desktop --serve` serves the same UI on `http://127.0.0.1` (token-protected)
for platforms without a system webview.

### Platform notes

The desktop app uses the system webview (WKWebView on macOS, WebView2 on
Windows, WebKitGTK on Linux). On Debian/Ubuntu install
`libwebkit2gtk-4.1-dev libgtk-3-dev` before building; or build without the
native window: `cargo run -p stream-desktop --no-default-features` (serve mode).

## Architecture

```
Desktop UI (stream-desktop)          stream CLI           AppPort clients
          │                               │                      │
          └──────────────► Stream AppPort surface (stream-appport) ◄┘
                                          │
                                     stream-core ── advisory ──► stream-semantic
                            ┌─────────────┼────────────┐          (Interpreter + evidence gate)
                            ▼             ▼            ▼
                        ingest          query        rules / ranking
                   ┌──────┴──────┐
                stream-web    stream-rss (RSS · Atom · JSON Feed adapters)
                                          │
                                          ▼
                                FeltDB (feltdb.flow) — the only authority
```

- `crates/stream-model` — typed IDs and records; URL canonicalization; the intelligence model (`ContextEntry`, `Signal`, `Evidence`, `Connection`)
- `crates/stream-core` — FeltDB bridge, the URL → signal pipeline, context, signals, evidence, connections
- `crates/stream-ingest` — fetching, format detection, the adapter boundary
- `crates/stream-web` — web page understanding and feed discovery
- `crates/stream-rss` — RSS, Atom, and JSON Feed adapters
- `crates/stream-semantic` — the replaceable `Interpreter` trait, the local and model-backed interpreters, the model provider boundary, and the evidence gate
- `crates/stream-reason` — cross-source synthesis and grounded reasoning (local and model-backed) with the answer gate
- `crates/stream-rules` — rules and explainable information-density ranking
- `crates/stream-query` — query helpers
- `crates/stream-appport` — the AppPort manifest and capability dispatch
- `crates/stream-cli` — the `stream` executable
- `crates/stream-desktop` — the desktop app (presentation only)
- `crates/stream-testkit` — test support: isolated FeltDB namespaces and a local fixture web

### Model

- **Source** — a durable identity for a URL: canonical URL (the identity), original URL, user-facing kind (web, GitHub, research, …), the adapter it is observed through (RSS/Atom/JSON Feed/web), title, processing stage, discovered/last-observed timestamps, provenance, and fetch history. Adding the same URL again (tracking parameters, `www.`, fragments, trailing slashes, …) resolves to the same source. Failures are durable state on the source and its fetch attempts; the URL is never lost.
- **ContextEntry** — what the user cares about: name, kind (interest/project/concern), description, aliases, related contexts. Durable and evaluated against every item.
- **Signal** — the information-density object: topic, subject, change, why it matters, status. Derived and advisory, never authority.
- **Evidence** — one verbatim excerpt supporting one claim, pointing at its item, source, provenance, and URL. Individually addressable.
- **Claim** — a proposition about a signal labelled observed / inferred / connected / hypothesis, with its evidence.
- **Synthesis** — when several observations describe one change: what they agree on, what each adds, where they differ (contradictions stay visible), and what is uncertain, each point citing evidence.
- **Insight** — saved reasoning (advisory), with its originating question, evidence, contexts, and signals.
- **IntelligenceEvent** — durable record of provider failures, refused proposals, and fallbacks.
- **Connection** — graph edges: Item → Topic, Item → Subject, Signal/Item → Context or Project (direct or via a related context), Item → Item. Item-to-item relations are also recorded through the existing `ItemRelation` infrastructure.

Five sources reporting the same development become **one signal with five
observations of evidence**: Stream deduplicates information, not evidence.

### Ranking

Today is ordered by a deterministic sum of observable factors: relationship to
your context, your rules, independent corroboration (distinct publishers),
open questions you saved, disputed evidence, connection to earlier
observations, change magnitude, novelty, and recency. Resolved and dismissed
signals leave Today but remain durable. Every factor carries its own
explanation, so Stream can always answer *why did this appear here?* There is
no engagement optimization, infinite scroll, or notification volume.

### Invariants (enforced by tests)

- **FeltDB = authority.** All durable state is FeltDB records. No SQLite, no JSON state files, no desktop-local database, no browser storage. The desktop bridge holds no data.
- **Stream = information processing.** One runtime, one pipeline, one AppPort surface for every client.
- **Evidence = grounding boundary.** Interpretation, synthesis, and answers can only cite verbatim, addressable evidence.
- **Conversation = interface, not memory.** Asking writes nothing; only an explicit save creates an insight.
- **Semantic model = advisory.** An interpreter only proposes. The evidence gate refuses claims whose excerpts are not verbatim in the source item, unknown contexts, and unsupported "why it matters". Refusals are recorded as advisory `SemanticDecision`s. The provider is recorded for audit and hidden from presentation.
- **AppPort = portable interface.** The desktop app, CLI, and AppPort SDK clients all use `StreamAppPort::invoke`.
- **Desktop = presentation.**
- **RSS/Atom/JSON Feed = adapters** underneath the generic source model.
- **Information density > information volume.**

## Data

State lives in FeltDB under the Stream root: `STREAM_ROOT`, or the nearest
directory containing `feltdb.flow`, or the checkout the binaries were built
from. The data path defaults to `.feltdb-data/stream` there (override with
`STREAM_FELTDB_PATH`). The desktop app and the CLI share it and can run at the
same time. FeltDB telemetry is disabled for the local bridge unless you set
`FELTDB_TELEMETRY` yourself.

## Validation

```bash
npm install
npm run feltdb:validate
cargo test --workspace
node --test tests/appport-surface.test.mjs
```

The Rust tests run the whole product loop (URL → source → items → signals →
evidence → connections → restart) against real FeltDB and a local fixture web
server, so they need `node` and `npm install`, but no network access.
