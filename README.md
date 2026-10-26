# qmd-ak

qmd is a personal archive and retrieval CLI for people and agents working across mail, articles, files, meetings and agent sessions. Its distinguishing rule is that capture, search and retrieval are different things: original bytes are archived, a deliberate text policy feeds search, and `get` returns the complete parsed document rather than the search excerpt.

## What it offers

- Search Gmail, read-only calendar events, Instapaper articles, Fathom meetings, local files, Claude/Codex/Pi sessions and published gsd change events through one CLI.
- Get complete documents by native id; browse recent items, read Gmail threads, or grep full document parts inside a time window.
- Get one search result per item, including one result for a session with many entries. Retrieval-tool calls and results do not echo back as searchable session text.
- Ask how complete each source is with `fresh_through`, rather than inferring health from the most recent document's timestamp.
- Enumerate newly arrived document versions with a resumable feed, including late-published items whose own timestamps are old.
- Keep raw and parsed versions in Backblaze B2 packs, with local document caching and a rebuildable Tantivy search store.
- Extract text from a file, stdin or URL without adding it to the archive. Explicit `add` and materialisation use the archival path instead.
- Inspect named red/green verification checks and their reasons. Missing timestamps and unreadable status are failures, not empty success.

```sh
qmd search "renewal terms" --source mail --json
qmd get 'gmail:pat@example.test:message-id'
qmd browse --source instapaper --limit 10 --json
qmd status --json
qmd extract https://example.org/article --json
```

`qmd` is the CLI name used here; the installer creates `qmd2`. See the alias setup below.

## How it works

```text
network fetchers / local readers / external publishers
                    ↓ payload files + index.jsonl + receipt
                  spool
                    ↓
           ingestd (one writer)
           raw pack → parser → doc pack + Tantivy
                    ↓ seal and upload
                  B2 packs
```

The lane primitives are generic: fetcher → spool → receipt → `fresh_through`, with a versioned parser per source. Local readers use the same accounting; external publishers such as gsd publish through the spool rather than needing another capture scheduler.

| Primitive | Contract or read surface | Rule |
|---|---|---|
| Spool | `qmd.spool-entry.v1`, implemented by `capture/` and `qmd2/ingestd.py` | Payload bytes and index entries hand work to the only archive/index writer. |
| Receipt | Capture/read receipts, evaluated in `qmd2/verify.py` | `enumerated = written + unchanged + excluded + failed`; incomplete capture cannot silently count as complete. |
| `fresh_through` | `qmd status --json` | Every item published before this boundary is captured or explicitly excluded; `null` means unknown. It is not the newest searchable timestamp. |
| `qmd.status.v1` | `qmd contract qmd.status.v1` | Declares liveness, installed commit, verification and source currency; undeclared keys are internal. |
| `qmd.feed.v1` | `qmd feed --source gmail --since-arrival ISO --json`; `qmd contract qmd.feed.v1` | Enumerates versions in arrival order with opaque continuation tokens; consumers dedupe `(id, version)`. |
| `qmd.extract.v1` | `qmd extract PATH\|-\|URL --json`; `qmd contract qmd.extract.v1` | Returns text, kind, model, page coverage, article metadata, fetch attempts and errors without archive/index writes. Unknown fields are null. |
| Raw/doc packs | `qmd2/packs.py`, `qmd2/b2.py` | Immutable uploaded objects at `<prefix>/<raw\|doc>/<source>/<seq:010d>.zst`; the configured prefix can be `packs/v1`. |
| Search policy | `qmd2/policy.py` | Index authored text, not quoted history, boilerplate, binary content or qmd tool echoes. Full documents remain retrievable. |
| Verify | `qmd2 verify --notify`; `qmd2/verify.py` | Checks receipts, capture/discovery lag, ingest heartbeat, upload backlog and sampled retrieval; publishes red/green with reasons. |

A successful error-free, complete-head receipt advances `fresh_through` to the run's start; provider errors, rate limits and incomplete heads do not. Scheduled currency uses the producer's `valid_until`, with a lateness limit of `max(3600, 2 × cadence_s)`. Consumers must not substitute internal progress clocks for completeness. Use the feed to enumerate late arrivals.

Feed tokens are positions in existing doc packs, not a separate event store. Delivery is at least once; a returned position cannot later have new writes inserted behind it. Arrival bootstrap before the recorded feed epoch is rejected rather than silently starting late. `QMD_UNTIL` provides an inclusive item-time replay ceiling for read commands; it does not reconstruct historical edited versions in ordinary search/get.

Packs seal at 64 MiB or 24 hours. Seekable zstd frames permit ranged B2 reads; locators retain uncompressed coordinates across sealing. Uploaded raw packs and consumed spool payloads can leave disk; the newest document packs are cached up to 8 GiB by default. Unuploaded packs are never evicted. Open packs have a disk-loss window until they seal and upload.

## Design decisions

1. **Archive bytes before interpreting them → preserve provenance and enable reparsing → no replacing originals with extracted text.**
2. **One ingest writer → order pack durability, index commits and checkpoints together → no projector/applier pipeline or competing writers.**
3. **Native document ids and explicit item grouping → preserve source identity while collapsing related search hits → no separate identity registry.**
4. **Separate raw packs, doc packs and search store → change parsers or search policy independently → no treating the index as the archive.**
5. **Immutable seekable packs → amortise object overhead and support cold reads → no object per document, pack compaction or cross-pack dedupe.**
6. **Tantivy plus bounded Python reranking → lexical matches remain inspectable and cheap → no embeddings, fusion stack or model reranker.**
7. **One part-kind indexing policy → keep authored content and suppress retrieval echoes → no query-specific exclusions or copied parser policy.**
8. **Receipt accounting and a completeness boundary → expose missing capture even when recent documents exist → no green inferred from newest-item time.**
9. **Arrival-ordered feed over existing packs → consumers see late arrivals and resume → no second journal or sequence space.**
10. **Maintenance in bounded slices → capture gets a turn during rebuilds → no hours-long stop-the-world reparse.**
11. **General source rules and explicit outcomes → failures remain explainable → no per-item repair exceptions or silent drops.**
12. **CLI and shipped JSON contracts → agents and sibling systems share the same interface → no agent-specific retrieval extension or consumer reads of internal stores.**
13. **Offline, non-editable deployment of a committed ref → installed behaviour is independent of a builder's checkout → no live patches or second installer.**

See [design](docs/design.md) for the working architecture and constraints, and [decisions](docs/decisions.md) for numbered decisions referenced in code.

## The family

- [qmd-ak](https://github.com/aaronkunaseelan/qmd-ak) holds the data and publishes retrieval, freshness and feed contracts.
- [gsd-ak](https://github.com/aaronkunaseelan/gsd-ak) decides **what**; its landed changes can arrive in qmd as `gsd-events` without feeding those changes back into its own selection input.
- [Steward](https://github.com/aaronkunaseelan/steward) decides **when** and can consume qmd status and arrival feeds.
- [tg-tools](https://github.com/aaronkunaseelan/tg-tools) delivers over Telegram; its separately configured transcription helper is also used for audio extraction.

These are separate systems, not bundled services. qmd does not make planning or notification decisions for them.

## Run it

This is a Linux/systemd deployment, not a portable one-command package. The checked-in Python wheel set targets Linux aarch64 and Python 3.12. You need `uv`, Git, tar, a compatible Go toolchain and a populated Go module cache for offline builds. HTML extraction needs Node.js; audio and document paths additionally need their configured tools, Azure credentials and providers. B2 and source accounts require your own credentials.

Read [configuration](docs/config.md) first. It lists private account settings, file roots, credential locations, B2 bucket/prefix, tool paths and overrides. The current source table requires account keys even for lanes you do not intend to use; it is not an optional-lane installer. Public archives omit wireproxy: restore and commit the documented upstream vendor tree before deploying a ref. The shared-relay link fallback also expects the documented Mullvad/gluetun setup; missing infrastructure is an explicit error, not a built-in public proxy.

Install from a reviewed release commit in a Git checkout containing its required vendor sources:

```sh
uv sync --frozen --offline
mkdir -p "$HOME/.cache"
./deploy.sh <release-commit>
ln -sfn "$HOME/.local/bin/qmd2" "$HOME/.local/bin/qmd"
qmd status --json
qmd verify
```

`deploy.sh` exports the committed ref, builds Go binaries offline, installs Python non-editably into `~/.local/share/qmd2/venv`, stamps the installed commit and installs user units. Builds and reindex take the host's one heavy lock via flock, default `/run/lock/qmd-batch-heavy.lock` (override with `QMD2_HEAVY_LEASE`). Deploy fails immediately if held; reindex waits. Run steps sequentially, never nested under a caller-held lock. It enables `qmd2-ingestd.service`, `qmd2-attachments.service` and `qmd2-verify.timer`; the timer invokes `qmd2-verify.service` every five minutes. Background work uses `qmd2.slice`. This is an installed copy, not an editable checkout; rollback uses `./deploy.sh <previous-commit>`.

Check both `alive` and the age/state of verify, then each source's `fresh_through`, `current`, `error` and `rate_limited`. An active unit alone is not proof of complete capture. Provider integrations and external producers must be configured before end-to-end health is meaningful. No prebuilt release artifact or provisioned provider account is implied by these instructions.

## Developing

```sh
uv sync --frozen --offline
uv run --offline pytest -q
```

- `qmd2/`: ingestd, packs, index, policy, CLI and source parsers. `capture/`: Go fetchers. `systemd/`: runtime units. `tests/`: synthetic parser tables, end-to-end source chains and real filesystem/subprocess/storage edges.
- `qmd2/contracts/`: shipped JSON schemas. Change consumers only through declared contract fields, never by reading internal SQLite or Tantivy state.
- `vendor/` and `uv.lock`: offline Python dependencies; keep platform-specific wheels. Format Python with `ruff format` and Go with `gofmt`.
- `verify --notify` appends fsynced JSON lines `{time, state, previous, red_checks, reasons}` to `~/.local/share/qmd2/alerts.jsonl` on transitions and a daily red digest. Failed delivery stays pending. Generic monitoring reads the `telemetry/1` output rather than opening the search store.
- Release outline: commit, run the suite, restore required vendor sources, deploy the reviewed ref under the heavy lease, then check installed status/verify and real source receipts. A policy change rebuilds from doc packs; a parser change uses `reindex --reparse SOURCE` from raw packs.

### Link fetch behaviour

URL extraction and link captures share direct-first fetching, then up to two random active Mullvad relays for blocked or temporary refusals. HTTP 403/451 is blocked; 429, 5xx, timeout and connection reset/refusal are temporary. HTTP 404/410 and TLS errors never use relays. A temporary queued capture gets one retry at least 180 seconds after its first try, then ends unavailable; `Retry-After` does not change that schedule. Pure `extract URL` runs synchronously without waiting or queueing another try.

`~/.config/qmd2/fetch.json` can set `mullvad_countries` (default `us`, `se`, `nl`, `de`, `ch`). The shared key/address are read in-process from the configured credential location. The fetcher checks gluetun's localhost HTTP proxy for its current relay and excludes it, rechecking under the tunnel lock. Unknown relay identity ends `shared_key_relay_unknown`; it never changes gluetun's configuration.

Each relay uses a short-lived localhost SOCKS wireproxy subprocess, serialised by `/tmp/qmd2-mullvad-<uid>.lock`. Key/address pass on stdin, not argv or disk; child output is discarded. Linux `setpriv --pdeathsig KILL` ties the tunnel to the fetch process. No interface or routing changes are made. `fetched.attempts` records route, relay, HTTP status, named error and elapsed seconds including tunnel startup; queued retries retain the list.

## Licence

MIT; see [LICENSE](LICENSE).
