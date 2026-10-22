#!/usr/bin/env bash
# Build one committed ref offline and non-editably, without changing the shared checkout.
set -euo pipefail
ref=${1:?usage: deploy.sh git-ref}
repo=$(cd -- "$(dirname -- "$0")" && pwd)
build=$(mktemp -d)
owns_lease=0
lease=${QMD2_HEAVY_LEASE:-"$HOME/workbench/tmp/qmd-target/heavy.lease"}
cleanup() {
  rm -rf -- "$build"
  if [[ $owns_lease == 1 ]]; then
    rm -f "$lease/owner"
    rmdir "$lease"
  fi
}
trap cleanup EXIT
sha=$(git -C "$repo" rev-parse --verify "$ref^{commit}")
git -C "$repo" archive "$sha" | tar -x -C "$build"
# Public exports omit vendored wireproxy; local deployments still build the committed source.
mkdir -p "$build/vendor/wireproxy"
git -C "$repo" archive "$sha:vendor/wireproxy" | tar -x -C "$build/vendor/wireproxy"
printf '\ninstalled_git_sha = "%s"\n' "$sha" >> "$build/qmd2/__init__.py"
runtime="$HOME/.local/share/qmd2"
mkdir -p "$runtime" "$runtime/bin" "$HOME/.local/bin" "$HOME/.config/systemd/user"
if [[ -f "$build/capture/go.mod" ]]; then
  if [[ ${QMD2_DEPLOY_LEASE_HELD:-0} == 1 ]]; then
    test -f "$lease/owner"
  else
    if ! mkdir "$lease" 2>/dev/null; then
      echo 'deploy: heavy lease occupied' >&2
      exit 4
    fi
    owns_lease=1
    printf 'deploy Go build\n' > "$lease/owner"
  fi
  (cd "$build/vendor/wireproxy" && GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 \
    go build -mod=vendor -trimpath -ldflags='-s -w -X main.version=v1.0.9' \
    -o "$runtime/bin/wireproxy" ./cmd/wireproxy)
  (cd "$build/capture" && GOPROXY=off go build -o "$runtime/bin/qmd2-fetch" ./cmd/qmd2-fetch)
  if [[ -d "$build/capture/cmd/wa-meow" ]]; then
    (cd "$build/capture" && GOPROXY=off go build -o "$runtime/bin/wa-meow" ./cmd/wa-meow)
  fi
  if [[ $owns_lease == 1 ]]; then
    rm "$lease/owner"
    rmdir "$lease"
    owns_lease=0
  fi
fi
systemctl --user stop qmd2-attachments.service 2>/dev/null || true
systemctl --user stop qmd2-ingestd.service 2>/dev/null || true
(cd "$build" && UV_PROJECT_ENVIRONMENT="$runtime/venv" uv sync --frozen --offline --no-editable)
ln -sfn "$runtime/venv/bin/qmd2" "$HOME/.local/bin/qmd2"
cp -r "$build"/systemd/* "$HOME/.config/systemd/user/"
mkdir -p "$runtime/attachments"
if [[ ! -f "$runtime/attachments/cutoff-v2" ]]; then
  date -u +%Y-%m-%dT%H:%M:%SZ > "$runtime/attachments/cutoff-v2"
fi
systemctl --user daemon-reload
systemctl --user enable --now qmd2-ingestd.service
if [[ -f "$build/systemd/qmd2-verify.timer" ]]; then
  systemctl --user enable --now qmd2-verify.timer
fi

if [[ -f "$build/systemd/qmd2-attachments.service" ]]; then
  systemctl --user enable --now qmd2-attachments.service
fi
