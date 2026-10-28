package fetch

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestXInterruptedHeadDiscoverySubprocess(t *testing.T) {
	for target, listName := range xLists {
		t.Run(listName, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "provider")
			program := `#!/usr/bin/python3
import json, os, pathlib, sys
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
with (root / 'calls').open('a') as log:
    log.write(json.dumps(args) + '\n')
pathlib.Path(os.environ['XBIRD_RECEIPT']).write_text(json.dumps({'requests_sent': 1, 'rate_limited': False}))
target = (root / 'target').read_text()
command = 'user-tweets' if target == 'tweets' else target
if command not in args:
    print(json.dumps({'tweets': [], 'nextCursor': ''}))
elif '--cursor' in args:
    if (root / 'fail').exists():
        print('HTTP 503', file=sys.stderr)
        sys.exit(1)
    print(json.dumps({'tweets': [{'id': 'unseen', 'text': 'second page', 'createdAt': '2026-10-28T12:00:00Z'}], 'nextCursor': ''}))
else:
    print(json.dumps({'tweets': [{'id': 'first', 'text': 'first page', 'createdAt': '2026-10-28T12:00:00Z'}], 'nextCursor': 'unread-page'}))
`
			for name, body := range map[string]string{"provider": program, "target": listName, "fail": "", "preload": ""} {
				if err := os.WriteFile(filepath.Join(root, name), []byte(body), 0700); err != nil {
					t.Fatal(err)
				}
			}
			newRun := func() *run {
				return &run{
					source: "x", account: "synthetic", spool: root,
					statePath: filepath.Join(root, "checkpoint.json"),
					env:       map[string]string{"QMD_X_USERNAME": "synthetic"},
					state:     checkpoint{Version: 1, Hashes: map[string]string{}},
				}
			}
			first := newRun()
			cursor := &xCursor{Backfill: &xBackfill{Done: true}}
			remaining := 26
			err := first.xHeads(context.Background(), binary, filepath.Join(root, "preload"), &remaining, cursor)
			if err == nil || err.Error() != "bird provider HTTP 503" || first.state.Hashes["first"] == "" {
				t.Fatalf("fixture did not persist page one before failing page two: %v", err)
			}
			if err := os.Remove(filepath.Join(root, "fail")); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "calls"), nil, 0600); err != nil {
				t.Fatal(err)
			}
			restarted := newRun()
			checkpointBytes, err := os.ReadFile(restarted.statePath)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(checkpointBytes, &restarted.state); err != nil {
				t.Fatal(err)
			}
			loaded, err := restarted.loadXCursor()
			if err != nil {
				t.Fatal(err)
			}
			remaining = 26
			if err := restarted.xHeads(context.Background(), binary, filepath.Join(root, "preload"), &remaining, loaded); err != nil {
				t.Fatal(err)
			}
			if err := restarted.xHistory(context.Background(), binary, filepath.Join(root, "preload"), &remaining, loaded); err != nil {
				t.Fatal(err)
			}
			calls, err := os.ReadFile(filepath.Join(root, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if restarted.state.Hashes["unseen"] == "" {
				t.Fatalf("list=%d head_complete=%t unseen_missing=true continuation_requested=%t", target,
					restarted.receipt.HeadComplete, strings.Contains(string(calls), "--cursor"))
			}
		})
	}
}
