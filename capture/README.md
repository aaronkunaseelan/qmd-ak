# qmd2 capture
Build from this directory offline: `GOPROXY=off go build ./cmd/qmd2-fetch`.
Full builds/tests take the heavy lease. `GOPROXY=off go test ./...` and
`GOPROXY=off go vet ./...` use the cached module graph.

`qmd2-fetch gmail|x|instapaper|fathom|whatsapp|calendar --spool DIR --state DIR
[--account ID] [--timeout 30m] [--env-file FILE] [--journal DIR]
[--history FILE | --legacy DB | --objects JSONL] [--limit 1000]
[--context JSON] [--aliases-from DIR] [--device READ_ONLY_SNAPSHOT]`

Production spool roots are `~/.local/share/qmd2/spool/<source>`; imports
use separate roots beneath that directory. Only ingestd writes the store.
Real validation uses scratch spool and state, never those production roots.

Each run prints one JSON receipt and exits 0 only when complete and green.
Fields: source, ok, enumerated, written, unchanged, excluded, failed,
source_head, duration_s; optional error, newest_id, failed_ids, oldest_failed,
exclusion_reasons, connection_head, lower_fidelity, unknown_fields, has_more, requests_sent.
Instapaper records failure_reasons counts and a safe reason class per pending id.
`--pending-only` retries Instapaper checkpoint ids without listing; its receipt
is incomplete (`ok:false`, `has_more:true`). Use a scratch state copy for diagnosis.
`enumerated = written + unchanged + excluded + failed`.
A missing provider page preserves its resume cursor. Failed listing items
retry every run. The first permanent text failure produces an explicit
`text_unavailable` snapshot counted as excluded and retried at most daily;
transient daily retries keep that exclusion. Other transient failures stay red.
After 6 h and 10 tries, transient ids in a run with another successful text fetch
are excluded as `provider_persistent_<class>` and retried daily.

Spool ABI is `qmd.spool-entry.v1`, unchanged: one fsynced `index.jsonl`
entry per observation and payload bytes at `objects/<aa>/<bb>/<sha256>`.
Optional `item_time` is the native item clock used by capture's rule-free head;
ingestd persists it with raw context and advances `processed_through` after an outcome.
Cursor and dedupe fold commit only after durable spool lines. Checkpoints
are source/account JSON files under `--state`, protected by a process lock.

Instapaper payloads pair `bookmark` metadata with `html` in one snapshot.
WhatsApp payloads carry `raw` plus `decoded`; history originals are retained
once as raw archive-only bytes, followed by a small context marker. Children
reference the original digest/ordinal; journal cursors use inode/complete-line offset.
Explicit pairs/media refs survive; ctx_update carries aliases/contacts/groups.
Legacy reads original DB/WAL in mode=ro; objects read local then named B2 remote.
Credentials are read in process from existing files; provider bodies and
subprocess stderr never enter error output. Gmail refreshes only in memory.
Calendar is supported but unscheduled. Gmail starts at the current live
history anchor; an expired anchor triggers a full listing.
`cmd/wa-meow` is receive-only and uses the existing device/journal ABI.
