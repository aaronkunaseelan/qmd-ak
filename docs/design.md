# Design

This is the working design for qmd: capture original bytes, parse them once, index deliberate text, and retrieve complete documents. Changes should preserve these boundaries and use the smallest mechanism that meets them. The numbered history referenced by source comments is summarised in [decisions.md](decisions.md).

## 1. User-facing model

`qmd search "…"` finds relevant personal items. `qmd get ID` returns the complete parsed document, not its indexed excerpt. The installed executable is `qmd2`; deployments may alias it to `qmd`.

The public source set comprises Gmail and read-only calendar events (group `mail`), Instapaper (`articles`), local files (`files`), Claude/Codex/Pi session entries (`sessions`), Fathom (`meetings`) and externally published gsd changes (`gsd-events`). The implementation contains additional adapters; the source table is in `qmd2/sources/__init__.py`.

An item is the unit of one search result. Usually item and document id are equal. Session entries have distinct document ids but share a session item, so many matching turns do not fill the result page.

## 2. Shape and stores

```text
Go network fetchers ─────────────┐
Python local readers ───────────┼─► payload files + index.jsonl + receipts
External publishers ────────────┘                  │ spool
                                                  ▼
                                       ingestd: one writer
                                    raw record → raw pack
                                    parse(source, raw, ctx)
                                    docs → doc pack + search upsert
                                    fsync + index commit → checkpoint
                                                  │
                                    seal → upload → local eviction
                                                  ▼
                                         immutable B2 packs
```

The spool ABI is `qmd.spool-entry.v1`. Publishers hand off records; they do not write archive packs or the search index. Local readers tail session files by offset and walk configured file roots.

| Store | Location | Role and recovery |
|---|---|---|
| Raw packs | Local until uploaded; B2 `<prefix>/raw/<source>/` | Original bytes and versions; the archive from which parsing can be repeated. |
| Doc packs | Local cache and B2 `<prefix>/doc/<source>/` | Parsed document versions, tombstones and explicit outcomes; source for index rebuilds. |
| Search store | `~/.local/share/qmd2/index/` | Tantivy current documents and policy text, rebuilt from doc packs. |
| State | `~/.local/share/qmd2/state.sqlite` | Pack catalog, checkpoints and operational state; remote catalogs and source rereads support recovery. |

State and search are implementation details, not consumer APIs. `QMD2_HOME` overrides the data home. B2 bucket and prefix are configured; `packs/v1` is a conventional prefix, not a fixed account or bucket.

## 3. Identity, documents and parsers

### K1: Native identity

Use source ids: `gmail:<account>:<message id>`, `instapaper:<account>:<bookmark id>`, `fathom:<account>:<recording id>`, `calendar:<calendar id>:<event id>`, `file:<root>:<relative path>`, and `gsd-events:<change_set_id>`. Session documents use `<runtime>:<session id>#<entry id>` and group under `<runtime>:<session id>`. Prefer a runtime's native entry id, otherwise its append-only byte offset. Attachments use `<parent id>/att/<part position>`; extracted outputs retain the attachment item. Manual inputs use `manual:<sha256>` and links use a normalized-URL hash.

There is no override file or independent identity registry. Subagent transcripts are separate items linked to their parent.

### K2: Immutable document versions

A document has identity, source/group/kind, version/time, optional message and location headers, context, attachment references, typed text parts, parser version and a raw-pack locator. Parts distinguish `header`, `authored`, `quoted`, `tool_output`, `attachment_text`, `boilerplate` and `binary`.

`get` renders available headers, a separator and the complete parts in order. HTML is cleaned into structured text, not printed as raw markup. Optional quote omission and output limits are explicit caller requests, not indexing truncation. A session item returns its entries in order.

One comparator governs live ingestion and replay. Reparse epochs dominate older parses; source time and capture ordering resolve ordinary versions, with source-specific version semantics where required. Reprocessing identical content is a no-op. Model output uses an explicit pipeline revision. A deletion or reparse removal is recorded in doc packs as a tombstone so rebuilding cannot resurrect it. File disappearance is deliberately not a deletion event: archived file history remains available.

### K3: Pure parsers

One versioned parser per source implements `parse(raw, ctx)`. The small checkpointed context holds only necessary cross-record information such as session title/project and open tool calls. Generic `ctx_update` payloads can extend that context. Retrieval-tool calls and their results must be identified together so their echoed content is not indexed.

A parser failure is represented and counted rather than escaping the ingest loop. Capture failure is not an excuse to manufacture a successful metadata-only article. Session runs belonging to Steward and gsd working directories are excluded by the session parser; existing indexed documents are tombstoned, raw files are untouched, and unknown working directories are kept.

## 4. K4: Pack durability

Open packs are append files at `packs/open/<raw|doc>/<source>/<seq>.rec` under the data home. Each record has a JSON header, its length-delimited bytes and a newline. Batches are fsynced before durable checkpoints advance. Crash recovery truncates incomplete tails; rereads deduplicate records.

Locators use uncompressed coordinates `(stream, source, seq, rec_off, rec_len)` and remain stable after sealing. Sequence allocation is above known local and remote packs; an existing remote key is not overwritten.

Seal at 64 MiB or 24 hours. Sealed objects use independent zstd frames with a seek table and checksum, named `<prefix>/<raw|doc>/<source>/<seq:010d>.zst`. Normal frames target 128 KiB; ordinary records are not split. Records of at least 8 MiB use their own pack; large-record frames are bounded to keep seek-table lengths representable. Stream large archive-only records rather than loading them into the parser's memory path.

Write and fsync the sealed file before removing its open predecessor. Upload through B2's S3 API with server-verified checksums; large uploads use multipart checksums. Sampled cold retrieval verifies the end-to-end read path without reading every upload back.

Only uploaded packs may be evicted. Raw packs and their consumed spool bytes leave local storage after upload; document packs retain a default 8 GiB cache, oldest sealed first. No compaction, dictionary compression or cross-pack deduplication. The open-pack period is an accepted disk-loss window, not immediate off-machine durability.

## 5. K5: Search policy and rebuilds

Tantivy stores current metadata, policy text and pack pointers. Search does not query raw packs. The indexing policy is a single part-kind table in `qmd2/policy.py`: authored and attachment text are eligible, tool output is capped at 2 KiB, and quoted history, boilerplate, binary, model summaries and qmd tool content are excluded. The general body cap is 64 KiB; files have a 256 KiB cap. Metadata remains searchable separately.

File archiving and indexing are distinct. The walker archives regular files up to 2 GiB, streams large reads and can snapshot SQLite through its backup API. Rebuildable trees and core dumps are excluded. The parser indexes prose and code/config as text, machine output/data dumps/credentials as path-only, and gives Git internals no search row. Credential detection uses both filename classes and content markers. Credential raw reads require a protected output file, never stdout. Files are walked every 24 minutes; the daily archive lane has its own receipt.

Search filters source/group/time and the caller's own session before candidate selection. It tries all-term matches first, then appends new any-term items if needed. Candidates are read in bounded 200-hit pages and reranked in Python by lexical score, bounded recency and kind weights, keeping one result per item. There are no vectors, embeddings, fusion tiers or model rerankers.

A policy change rebuilds from doc packs. A parser change reparses raw packs with `reindex --reparse SOURCE`; a capture decode bug may require `--reset-source SOURCE`. Maintenance works in slices of at most 60 seconds per ingest turn, with capture between slices. A full rebuild writes a new index while ordinary readers retain the old index, then swaps at the tail; search can lag during this operation.

## 6. K6: Read contracts

The CLI is the consumer boundary. `search`, `get`, `browse`, `thread` and windowed `grep` serve current documents. `get --raw --out FILE` reads archived file bytes. `get --materialise` explicitly invokes extraction; ordinary attachment heads do not.

- `qmd.status.v1`, printed by `status --json`, declares liveness, installed commit, verify state and per-source completeness/currency. Other keys are internal.
- `qmd.feed.v1`, printed by `feed --source SYSTEM (--after TOKEN | --since-arrival ISO) --json`, enumerates doc versions and tombstones in pack-write order. Tokens are opaque pack positions, no second sequence store. Consumers dedupe `(id, version)`; delivery is at least once and new records cannot appear behind a returned token. Bootstrap before the arrival epoch fails explicitly.
- `qmd.extract.v1`, printed by `extract PATH|-|URL --json`, returns text, kind/model, page coverage, article metadata, fetch attempts and an error. It uses temporary files but writes no archive, queue, state or index.
- `contract ID` prints these shipped schemas from `qmd2/contracts/`.

`QMD_UNTIL` constrains reads inclusively by item time. It does not turn current-document views into historical edit reconstruction. Feed applies the ceiling to each stored version. Missing, unavailable and usage errors are explicit; help and contracts are available through the CLI.

## 7. K7: Receipts, freshness and verify

Every scheduled lane declares a cadence. Each capture/read receipt accounts for `enumerated = written + unchanged + excluded + failed`. A new receipt replaces previous run errors and flags; only cross-run clocks and unresolved progress carry forward.

`fresh_through` advances to the run's start only after an error-free, non-rate-limited, complete-head receipt with no failures or backfill error. It means every item already published before that boundary was captured or explicitly excluded. `null` means unknown. It is neither `last_complete` nor `searchable_through`. A late-published old item still requires feed enumeration.

`processed_through` includes parser exclusions and is compared with the captured source head; it is progress, not completeness. Producer currency expires at `valid_until = fresh_through + max(3600, 2 × cadence_s)`. Unscheduled publisher lanes have unknown scheduled currency, not a fabricated capture clock.

Verify runs every five minutes. It checks heartbeat/status age, receipt accounting, capture/discovery and unresolved lag, new parse failures, attachment work, uploads older than two hours and sampled retrieval including cold B2 reads. Unknown timestamps and missing/corrupt inputs produce failed checks with reasons. Provider rate limiting is distinguished from capture failure but does not create complete discovery.

Verify publishes generic `telemetry/1` and local status. With `--notify`, transitions and the daily red digest append fsynced `{time, state, previous, red_checks, reasons}` records to `alerts.jsonl`. Failed delivery remains pending. qmd does not send messages itself. Disk is a host-monitoring concern; the pack writer retains an 8 GiB free-space safety floor.

Background units yield through `qmd2.slice`; verify adjusts CPU weight from 10 to 50 or 90 at 6× and 12× lane lateness. Interactive reads run at caller priority.

## 8. K8: Extraction and outcomes

Attachment work runs outside capture and returns results through the spool. Detect type from bytes, not metadata. Streaming caps are audio 512 MiB, PDF/DOCX 100 MiB, image 20 MiB and HTML/text 50 MiB. Audio uses bounded ffmpeg conversion and the configured Azure transcription helper; image OCR uses Azure Document Intelligence `prebuilt-read`, and PDF/DOCX uses `prebuilt-layout`. No local OCR/transcription fallback. HTML uses the vendored Defuddle bundle through Node.js; text is decoded directly.

Store extraction model/pipeline revision and explicit page coverage; unknown totals stay unknown. Limited DOCX requests unsupported by the provider fail explicitly instead of silently billing for an unbounded extraction. Near-empty OCR becomes `no_text`. Fathom share URLs have a narrowly scoped transcript extractor; account-only URLs remain generic links.

The queue holds open work only. Completed extraction is a doc; failed/unavailable outcomes have a reason and no searchable body. Remove a queue row only after its stored outcome can be read back. Locks prevent a worker and an inline caller from extracting the same row twice. `add` archives explicit manual inputs; pure `extract` does not.

Links deduplicate by normalized URL and retain parent context. Direct-first fetching can use up to two temporary Mullvad relay processes on a blocked/temporary response. Queued temporary refusals get one delayed retry after at least 180 seconds; synchronous extraction never queues that retry. Source refusals remain item outcomes; qmd-side failures remain visible in health. Credential and relay setup are documented in [config.md](config.md).

## 9. K9–K10: Deploy and secrets

One script, `deploy.sh REF`, exports a committed Git ref, builds offline, installs Python non-editably, stamps the commit and installs systemd user units. It does not switch the shared checkout. Rollback uses the same script with a previous ref. The configured shared heavy lease serializes builds and reindex. Public exports need the separately restored, committed wireproxy sources before deploying.

Configuration and provider credentials are private inputs, never committed examples with real values. Read credentials in-process; errors and logs must never print tokens, environment files or credential values. The files archive can retain sensitive originals even when search hides their content: protect the archive and B2 credentials accordingly.

## 10. Anti-over-engineering rules

- Build only what this design names; otherwise choose the smallest change that meets the retrieval and freshness goals.
- Delete before adding. Every new mechanism must name the failure it prevents.
- No PostgreSQL, separate event journal, sequence spaces, projector/applier, frontiers, item-state/holds vocabulary, release pins, attestations, SHA-named release directories, bridges or live patches.
- No per-path, per-chat or per-item repair exceptions. Source policy belongs in its parser; shared rules belong in one shared definition.
- No second scheduler, installer, comparison pipeline, shadow deployment, new reviewer or gate framework.
- Tests have three forms: a raw → pack → index → search → get chain per source; pure parser tables; real B2/Tantivy/filesystem/subprocess edges. Do not mock internal code. Use synthetic fixtures.
- Line count is a signal, not a budget. Prefer fewer lines of logic; never pack statements. Use `ruff format` and `gofmt`.
- Comments state the current contract in at most five lines. History belongs in Git.
- Before adding a framework, registry, plugin system, retry layer or state machine with more than four states, stop and ask the maintainer.

Performance and storage goals are measurements to validate on an installation, not promises of benchmark results on arbitrary hosts. Do not add a new subsystem merely to meet a guessed budget.
