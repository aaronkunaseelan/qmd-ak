# qmd2

Read `docs/design.md` (the law) before changing the program.

## Rules (full text in the program AGENTS.md)
- Build only what `docs/design.md` names. If it isn't there, choose the smallest thing that meets the target and ledger the choice.
- Delete before you add. A new mechanism must name the failure it prevents.
- No PostgreSQL, journal, projector, holds, pins, attestations, release directories, bridges, live patches or comparison runs.
- No frameworks, registries, plugin systems, retry layers or state machines with more than 4 states: ask the maintainer before adding one.
- No per-path or per-chat special cases (D-33).
- Tests come in three forms only: one end-to-end chain per source, one pure table per parser, and real edges (B2, Tantivy, filesystem, subprocess). No mocks of internal code. Fixtures are synthetic.
- Line count is a signal, not a budget (D-167): fewer lines of logic, never packed lines. Python is `ruff format`ted, Go `gofmt`ed.
- Comments state the current contract in ≤5 lines. History belongs in git.
- Never print env files, tokens or credentials, including in errors.

## Layout
- `qmd2/`: Python package (ingestd, packs, index, policy, CLI, sources).
- `capture/`: Go module, `qmd2-fetch` (M2).
- `consumers/mac/`: Mac ssh shim for the CLI (the Pi extension was retired by D-163). `systemd/`: units. `tests/`.
- `vendor/`: every wheel; `uv.lock` resolves only from here (`no-index`).

## Run
- `uv sync --frozen --offline`, then `uv run --offline pytest -q`. Tests and production use the same lock and interpreter (F11).
- Make small commits with your configured git identity. Do not push without review.
