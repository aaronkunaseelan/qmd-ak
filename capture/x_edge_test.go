package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestXAccountConfigurationSubprocess(t *testing.T) {
	root := t.TempDir()
	bird := filepath.Join(root, "bird")
	writeBirdTestCLI(t, bird)
	os.WriteFile(filepath.Join(root, "mode"), []byte("headonly"), 0600)
	env := []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1", "QMD_X_BIRD_BIN=" + bird}
	args := []string{"x", "--spool", filepath.Join(root, "spool"), "--state", filepath.Join(root, "state"), "--env-file", filepath.Join(root, "absent.env")}
	missing, code := captureChild(t, env, args...)
	if code != 1 || missing.Error != "source account required" {
		t.Fatal("missing account must fail explicitly")
	}
	receipt, code := captureChild(t, env, append(args, "--account", "123", "--x-username", "synthetic")...)
	if code != 0 || receipt.Account != digest([]byte("123")) || !receipt.HeadComplete {
		t.Fatalf("numeric account must preserve hashed spool identity: code=%d receipt=%+v", code, receipt)
	}
}

func TestXListingSubprocess(t *testing.T) {
	root := t.TempDir()
	bird := filepath.Join(root, "bird")
	mode := filepath.Join(root, "mode")
	log := filepath.Join(root, "calls")
	writeBirdTestCLI(t, bird)
	r := seedXCapture(root, bird)
	call := func(m string) error {
		t.Helper()
		if err := os.WriteFile(mode, []byte(m), 0600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(log, nil, 0600); err != nil {
			t.Fatal(err)
		}
		r.receipt = Receipt{Source: "x"}
		return runX(context.Background(), r)
	}
	if err := call("normal"); err != nil || !r.receipt.HeadComplete || r.receipt.RequestsSent != 26 || r.receipt.Written != 2 || r.receipt.Unchanged != 3 {
		t.Fatalf("head plus two backfill pages: err=%v receipt=%+v", err, r.receipt)
	}
	var c xCursor
	if json.Unmarshal(r.state.Cursor, &c) != nil || c.Backfill.List != 1 || c.Backfill.Cursor != "3" || c.Backfill.Done {
		t.Fatal("legacy cursor did not migrate and progress")
	}
	checkBirdHeadCalls(t, log)
	raw, err := os.ReadFile(r.statePath)
	if err != nil || json.Unmarshal(raw, &r.state) != nil {
		t.Fatal("durable checkpoint unavailable")
	}
	if err := call("normal"); err != nil || !r.receipt.HeadComplete || r.receipt.Backfill.List != 2 || r.receipt.Backfill.Done {
		t.Fatal("backfill must progress without holding head")
	}
	if err := call("normal"); err != nil || !r.receipt.HeadComplete || !r.receipt.Backfill.Done {
		t.Fatal("backfill must finish without holding head")
	}
	if err := call("normal"); err != nil || r.receipt.RequestsSent != 14 || !r.receipt.Backfill.Done {
		t.Fatal("done backfill must stay done")
	}
	if err := call("changed"); err != nil || !r.receipt.HeadComplete || r.receipt.Written < 5 {
		t.Fatalf("changed head must continue to known page: err=%v receipt=%+v", err, r.receipt)
	}
	if err := call("cap"); err == nil || !r.receipt.RateLimited || r.receipt.HeadComplete || r.receipt.RequestsSent != 26 {
		t.Fatal("cap during head must be incomplete and limited")
	}
	if err := call("limited"); err == nil || !r.receipt.RateLimited || r.receipt.HeadComplete {
		t.Fatal("provider 429 must stop head")
	}
	if err := call("normal"); err != nil {
		t.Fatal(err)
	}
	r.state.Cursor = json.RawMessage(`{"backfill":{"list":1,"cursor":"1","done":false}}`)
	if err := call("tailcap"); err != nil || !r.receipt.HeadComplete || !r.receipt.RateLimited || r.receipt.Backfill.Cursor != "2" {
		t.Fatal("backfill cap must retain completed head and cursor")
	}
	r.state.Cursor = json.RawMessage(`{"list":0,"cursor":"","full_pass":true}`)
	if err := call("normal"); err != nil || !r.receipt.Backfill.Done {
		t.Fatal("completed legacy traversal must migrate as done")
	}
}

func TestXViewerStateVersions(t *testing.T) {
	for _, nested := range []bool{false, true} {
		t.Run(fmt.Sprint(nested), func(t *testing.T) {
			root := t.TempDir()
			r := seedXCapture(root, "")
			for i, bookmarked := range []bool{false, true, true} {
				viewer := map[string]any{"favorited": true, "bookmarked": bookmarked, "favorite_count": i}
				tweet := map[string]any{"id": "42", "text": "synthetic", "viewCount": i}
				if nested {
					tweet["_raw"] = map[string]any{"legacy": viewer}
				} else {
					for k, v := range viewer {
						tweet[k] = v
					}
				}
				raw, _ := json.Marshal(tweet)
				if err := r.xTweet(raw); err != nil {
					t.Fatal(err)
				}
				want := min(i+1, 2)
				if r.receipt.Written != want {
					t.Fatalf("snapshot %d: got %d versions, want %d", i, r.receipt.Written, want)
				}
			}
			entries, err := os.ReadFile(filepath.Join(r.spool, "index.jsonl"))
			if err != nil || len(bytes.Split(bytes.TrimSpace(entries), []byte("\n"))) != 2 {
				t.Fatal("spool must retain two versions")
			}
		})
	}
}

func writeBirdTestCLI(t *testing.T, bird string) {
	t.Helper()
	script := `#!/usr/bin/python3
import json, os, sys
from pathlib import Path
args = sys.argv[1:]
root = Path(__file__).parent
mode = (root / 'mode').read_text()
with (root / 'calls').open('a') as f:
    f.write(json.dumps(args) + '\n')
kind = args[1]
if kind == 'user-tweets' and args[2] != 'synthetic': sys.exit(1)
cost = {'whoami': 5, 'bookmarks': 1, 'likes': 6, 'user-tweets': 2, 'thread': 1, 'search': 1}[kind]
if mode == 'cap' and kind == 'bookmarks': cost = 6
if mode == 'tailcap' and '--cursor' in args: cost = 7
remaining = int(os.environ['XBIRD_REMAINING'])
json.dump({'requests_sent': min(cost, remaining), 'rate_limited': mode == 'limited'}, open(os.environ['XBIRD_RECEIPT'], 'w'))
if mode == 'limited' or cost > remaining: sys.exit(1)
if kind == 'whoami':
    print('user_id: 123')
    sys.exit(0)
if kind == 'thread':
    print(json.dumps({'tweets': [{'id': args[2], 'text': 'synthetic'}]}))
    sys.exit(0)
cursor = args[args.index('--cursor') + 1] if '--cursor' in args else '0'
n = int(cursor)
ident = kind + cursor
text = 'changed' if mode == 'changed' and kind == 'bookmarks' and n == 0 else 'synthetic'
nxt = str(n + 1) if kind in ('bookmarks', 'likes') and n < 4 else ''
if mode == 'headonly': nxt = ''
if mode == 'cap' and kind == 'bookmarks':
    ident = 'new' + cursor
    nxt = str(n + 1)
print(json.dumps({'tweets': [{'id': ident, 'text': text}], 'nextCursor': nxt}))
`
	if err := os.WriteFile(bird, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
}

func seedXCapture(root, bird string) *run {
	r := &run{source: "x", account: digest([]byte("123")), spool: filepath.Join(root, "spool"), statePath: filepath.Join(root, "state.json"), env: map[string]string{"QMD_X_BIRD_BIN": bird, "QMD_X_USERNAME": "synthetic"}, state: checkpoint{Version: 1, Hashes: map[string]string{}, Cursor: json.RawMessage(`{"list":1,"cursor":"1","full_pass":false}`)}}
	for _, kind := range []string{"bookmarks", "likes", "user-tweets"} {
		raw, _ := json.Marshal(map[string]any{"id": kind + "0", "text": "synthetic"})
		r.state.Hashes[kind+"0"] = digest(raw)
	}
	return r
}

func checkBirdHeadCalls(t *testing.T, log string) {
	t.Helper()
	raw, err := os.ReadFile(log)
	if err != nil {
		t.Fatal(err)
	}
	var calls [][]string
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var args []string
		if json.Unmarshal(line, &args) != nil {
			t.Fatal("invalid call log")
		}
		calls = append(calls, args)
	}
	if len(calls) != 6 || calls[1][1] != "bookmarks" || calls[2][1] != "likes" || calls[3][1] != "user-tweets" || calls[4][len(calls[4])-1] != "1" {
		t.Fatal("heads must precede migrated history")
	}
}
