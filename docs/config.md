# Configuration
Install Python dependencies with `uv sync --frozen --offline`.
The lock resolves from `vendor/*.whl`: keep these wheels (Linux aarch64).
Go capture builds use `GOPROXY=off` with a populated module cache.

## Accounts (required, private)
`~/.config/qmd2/accounts.json`; override with `QMD2_ACCOUNTS=/tmp/accounts.json`.
One JSON object; missing required keys fail explicitly. Example values:
- `x_account: "123"`: numeric X id; Go derives the stable SHA256 account.
- `x_username: "pat_synthetic"`: own-post listing username, passed as `--x-username`.
- `instapaper_account: "1234567"`, `whatsapp_account: "12025550101:1@s.whatsapp.net"`.
- `gmail_account`, `fathom_account`, `calendar_account: "pat@example.test"`: source identities.
- `owner_addresses: ["pat@example.test", "pat@org.example.test"]`: personal mail exceptions.
- `owner_domain: "org.example.test"`: shared-mail alias domain.
- `file_roots: {"notes": "~/notes"}`: stable archive root names and local directories.
- `transcriber_tool: "~/tools/transcribe/transcribe.py"`: Azure audio helper.
- `transcription_env: "~/tools/audio/.env"`: secondary audio deployment settings.
Paths expand `~`. Session roots are home-relative: `agents/pi/sessions`,
`.codex/sessions`, `.claude/projects`, `agents/codex/sessions`, `agents/claude-code/projects`.
Sessions in `~/.local/{state/steward,share/steward,share/gsd}` are excluded.

## Credentials (never commit these files)
`/etc/qmd-agent/qmd-agent.env` supplies B2 keys (process env takes precedence):
`QMD_B2_REGION=us-west-004`, `QMD_B2_KEY_ID`, `QMD_B2_APPLICATION_KEY`,
`QMD_B2_BUCKET=example-archive` (b2_bucket, required); `QMD_B2_PREFIX` optional, default `packs/v1` (the pack-layout version).
Go `--env-file` overrides that path; it also reads `~/.config/agent-secrets/credentials.env`.
Go accepts `QMD_<SOURCE>_ACCOUNT` if `--account` is absent; X expects the numeric id.
`QMD_X_BIRD_BIN` defaults to `~/.local/bin/bird-wrapper`; `QMD_BIRD_PRELOAD`
overrides the embedded request counter; `QMD_X_USERNAME` is the standalone CLI fallback.
Instapaper: `QMD_INSTAPAPER_CONSUMER_KEY`, `QMD_INSTAPAPER_CONSUMER_SECRET`,
`QMD_INSTAPAPER_ACCESS_TOKEN`, `QMD_INSTAPAPER_ACCESS_TOKEN_SECRET`.
Fathom: `QMD_FATHOM_API_KEY`. Google reads `~/.config/google-api/tokens-*.json`,
matching `account` or filename; fields: `account, access_token, refresh_token, client_id,
client_secret, expiry_date` (milliseconds). Refresh is in-process, not written back.
WhatsApp reads `~/.local/share/qmd/wa-meow/journal`; receiver owns its device store.
Audio settings: helper-adjacent `.env`, `transcription_env`, then
`~/.config/agent-secrets/azure-openai-transcription.env`; label key is
`TRANSCRIBE_AUDIO_MODEL_MINI` or `AZURE_OPENAI_TRANSCRIPTION_MINI_DEPLOYMENT`.
OCR reads `QMD_DOCUMENT_INTELLIGENCE_ENDPOINT` from the agent env files and
`~/.config/agent-secrets/azure-document-intelligence.key`.
VPN: `~/.config/agent-secrets/vpn-browser.env` has `WIREGUARD_PRIVATE_KEY`,
`WIREGUARD_ADDRESSES`; `~/.config/qmd2/fetch.json` has `mullvad_countries: ["us", "se"]`.

## Runtime and overrides
`QMD2_HOME` / `--home`: archive state (default `~/.local/share/qmd2`).
`QMD_UNTIL`: inclusive ISO replay ceiling. Session self-exclusion reads
`PI_SESSION_ID, CLAUDE_SESSION_ID, CLAUDE_CODE_SESSION_ID, CODEX_THREAD_ID`.
`QMD2_VERIFY_CLI`: sampled-read executable; `XDG_STATE_HOME`: telemetry directory.
`QMD_RCLONE_BIN`, `QMD_RCLONE_CONFIG`: history object download executable/config.
`QMD_GMAIL_BASE_URL`, `QMD_INSTAPAPER_BASE_URL`: test endpoint overrides.
`QMD2_HEAVY_LEASE`: flock file for builds and reindex, default
`/run/lock/qmd-batch-heavy.lock`: the host's one heavy lock, shared with batch-run,
Steward and secwatch. Deploy fails immediately if held; reindex waits. Run steps
sequentially, not nested under a caller-held lock. The empty lock file persists;
process exit releases the lock without owner files or directory cleanup.

## wireproxy (not included in public archives)
Fetch v1.0.9 and verify the checksum in [vendor/wireproxy.md](../vendor/wireproxy.md).
Extract to `vendor/wireproxy`, run `go mod vendor` there and commit it, retaining
upstream licenses. Deploy builds it offline with `-mod=vendor` under the heavy lease.
