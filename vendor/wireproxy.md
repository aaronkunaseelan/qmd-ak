# wireproxy v1.0.9

Source: https://github.com/pufferffish/wireproxy/tree/v1.0.9
Archive: https://codeload.github.com/pufferffish/wireproxy/tar.gz/refs/tags/v1.0.9
SHA256: `6359d060740ad878f0c26217d44968a58a069302447e15774cbf747b8d1404d2`

ISC license (`wireproxy/LICENSE`). Transitive license notices are retained
in `wireproxy/vendor/` (including WireGuard's MIT and gVisor's Apache-2.0).
Source and `go mod vendor` output are committed. No module downloads at build:
`GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local CGO_ENABLED=0 go build -mod=vendor
-trimpath -ldflags='-s -w -X main.version=v1.0.9' -o wireproxy ./cmd/wireproxy`.
`deploy.sh` builds this into `~/.local/share/qmd2/bin/wireproxy` under its lease.
