# Design decisions

These are paraphrased engineering decisions, not deployment receipts. Numbered headings preserve references used in source comments, schemas and contributor guidance. The current architecture is in [design.md](design.md); schemas in `qmd2/contracts/` define the consumer boundary.

## D-33: General policy, not path-specific repairs

Keep archive membership under general publication rules rather than excluding particular troublesome directories. A local anomaly should lead to a rule at the layer that owns its meaning, not a named-path exemption. This rules out per-path, per-chat and per-item repair lists. Explicit source scope is still part of a source's declared policy.

## D-163: One CLI boundary

People and agents use the same CLI. Remove the agent-specific retrieval extension, which merely executed that CLI, rather than maintaining a second tool interface. Caller session-id environment variables still support self-exclusion. This rules out parallel retrieval implementations or treating an agent extension as the product API.

## D-167: Maintainable logic, not line budgets

Preserve behaviour while simplifying code and consolidating tests. Line count is a signal of complexity, not a hard allowance and never a reason to pack statements. Format Python with `ruff format` and Go with `gofmt`.

Tests have three forms: an end-to-end raw → pack → index → search → get chain for each source, a pure table for each parser, and real storage/filesystem/subprocess edges. Fold overlapping tests into those forms instead of mocking internal code or accumulating a separate test for every historical regression. Fixtures are synthetic.

## D-168: Completeness is explicit; calendar is a read-only lane

Expose `fresh_through` as the boundary before which every already-published source item is captured or explicitly excluded. A provider error or incomplete head cannot advance it; unknown is null. Keep this separate from searchable progress and the time of the most recent successful run, so downstream systems do not infer completeness from activity.

Schedule read-only calendar capture in ingestd at a 900-second cadence. This is archive capture, not a replacement for a planner's live calendar read.

Attachment publication must also respect ordering: spool manual bytes before publishing queue work, avoid waiting on a running extraction except when rewriting its work, recognise HTML beyond leading markup, and measure transient retry delay from the last attempt. These rules avoid races and duplicate work without a second pipeline.

## D-170: Keep automation transcripts with their producer

Exclude sessions whose working directory belongs to Steward or gsd runtime archives from qmd's session source, across Claude, Codex and Pi. Tombstone already-indexed session documents through the existing deletion path without touching raw session files; keep sessions with unknown working directories. The producing systems retain their own run archives. This avoids feeding a consumer its own qmd-reading transcripts and does not introduce a second session store in qmd.

## D-169: Status and arrival feeds are contracts

Publish `qmd.status.v1` for liveness and source currency and `qmd.feed.v1` for resumable enumeration. The feed uses existing document-pack positions, not a new journal or sequence space. Late publication must remain visible even when an item's timestamp is old. Consumers dedupe `(id, version)` and read only declared fields. Generic monitoring consumes producer-owned telemetry, not the producer's database.

## D-126: Preserve originals and separate derived stores

Keep original bytes in raw packs, parsed versions in doc packs and policy text in the search store. Parsing and indexing are replaceable interpretations, not substitutes for originals. Use an explicit pipeline revision to order model outputs. This rules out treating extracted text or search snippets as archival truth.

## D-160: Bound the open-pack window

Seal at 64 MiB or 24 hours, whichever comes first. This trades object and upload overhead against the period during which a disk failure can lose an open pack. It does not claim immediate off-machine durability or add force-sealing to every capture.

## D-145: Maintenance yields to capture

Reindex and reparse execute in bounded slices of at most 60 seconds per ingest turn, preserving progress across restarts. Fetching and local readers run between slices. A full rebuild uses a new index and swaps when it reaches the pack tail. This rules out blocking all capture for the duration of a large reparse.

## D-147: Reparse epochs make recovery durable

A source reparse advances its epoch, and the shared version comparator considers epoch before older ordering fields. Replaying doc packs therefore cannot restore an older erroneous parse over the corrected output. No parallel override list is needed.

## D-155 and D-156: Archive scope differs from search scope

Archive files broadly but do not index credential text. Recognise credential classes by filename and content markers; give them path-only rows. Archive regular files up to 2 GiB, stream large reads and snapshot SQLite where possible. Give prose and code/config text rows, machine output and data dumps path-only rows, and Git internals no search row.

Walk files every 24 minutes and report the daily archive pass as a separate cadence-bearing lane. File disappearance does not delete archive history. These rules reject both narrow hand-maintained file allowlists and the assumption that every archived byte belongs in full-text search.

## D-166: gsd publishes changes through the shared spool

Each landed gsd change set has one native `gsd-events` id and readable authored text. gsd publishes it; qmd needs no capture scheduler for this lane. Keep its group separate so gsd selection can avoid consuming its own outputs. This uses the existing spool ABI rather than adding a sibling-specific database integration.

## D-174: One stored attachment outcome

The queue contains open work only. A successful extraction is a document; failed and unavailable outcomes are records with a reason and no searchable body. Remove queue work only after its outcome is stored and readable. Context remains on the parent instead of being copied into multiple tracking records. This rules out permanent terminal queue rows as a second outcome authority.

## D-176: Exclusions belong in parsers

Capture retains source bytes and reports its native head. The source parser alone decides whether an item yields a document or an explicit exclusion. Verify compares the capture head with `processed_through`, which includes exclusions, instead of demanding a searchable document for every captured timestamp. This prevents duplicated exclusion policy and false lag on deliberately excluded content.

## D-172 and D-180: Pure extraction shares the real extractor

`extract PATH|-|URL --json` returns `qmd.extract.v1` without archive, index, queue or persistent state writes. It shares byte routing and extraction with materialisation rather than adding another parser stack. Web HTML uses Defuddle, with title, author, published date and fetch attempts returned where known; unknown metadata stays null. This separates asking for text from choosing to archive it.
