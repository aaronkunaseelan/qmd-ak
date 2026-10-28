package fetch

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestXContextSubprocess(t *testing.T) {
	root := t.TempDir()
	binary := filepath.Join(root, "bird")
	script := `#!/usr/bin/python3
import json, os, sys
from pathlib import Path
root = Path(__file__).parent
args = sys.argv[1:]
with (root / 'calls').open('a') as f: f.write(json.dumps(args) + '\n')
json.dump({'requests_sent': 1}, open(os.environ['XBIRD_RECEIPT'], 'w'))
if args[1] == 'whoami':
    print('user_id: 123'); sys.exit(0)
assert '--max-pages' in args and args[args.index('--max-pages')+1] == ('1' if args[1] == 'thread' else '3')
mode = (root / 'context-mode').read_text() if (root / 'context-mode').exists() else 'reply'
def tweet(id, author, parent=None):
    text = (root / 'edited').read_text() if id == '42' and (root / 'edited').exists() else 'synthetic '+id
    return dict(id=id, authorId=author, author=dict(username=author), text=text,
                conversationId='10', inReplyToStatusId=parent)
if args[1] == 'search':
    assert args[2] == 'conversation_id:10 from:author'
    posts = [tweet('42', 'author', None if mode == 'root' else '10'),
             tweet('12', 'author', '42' if mode == 'root' else '11'), tweet('13', 'stranger', '10')]
    print(json.dumps(dict(tweets=posts, nextCursor='more')))
elif args[2] == '11':
    print(json.dumps(dict(tweets=[tweet('11', 'stranger', '10'), tweet('10', 'stranger')])) )
else:
    print(json.dumps(dict(tweets=[tweet('42', 'author', '10'), tweet('10', 'stranger'), tweet('13', 'stranger', '10')], nextCursor='more')))
`
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	r := seedXCapture(root, binary)
	c, err := r.loadXCursor()
	if err != nil {
		t.Fatal(err)
	}
	r.xstate = c
	raw := json.RawMessage(`{"id":"42","text":"synthetic 42","authorId":"author","author":{"username":"author"},"conversationId":"10","inReplyToStatusId":"10","bookmarked":true}`)
	c.Due["42"] = &xDue{At: time.Now().Add(24 * time.Hour), Tweet: raw, Initial: true}
	remaining := 26
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil {
		t.Fatal(err)
	}
	captured := c.Contexts["42"]
	if r.receipt.RequestsSent != 3 || len(captured.Posts) != 3 || !strings.Contains(strings.Join(captured.Notices, " "), "3-page cap hit") || c.Due["42"].Initial {
		t.Fatalf("scoped context: %+v %+v", captured, r.receipt)
	}
	authors := map[string]bool{}
	for _, post := range captured.Posts {
		authors[xString(post.Tweet, "id")] = post.Authored
	}
	if !authors["12"] || authors["11"] || authors["10"] {
		t.Fatal("author text versus non-authored parents")
	}
	if _, exists := authors["13"]; exists {
		t.Fatal("unrelated replies must not be stored")
	}
	before := r.receipt.Written
	if err := r.xTweet(raw); err != nil || r.receipt.Written != before {
		t.Fatal("poll cannot erase retained context")
	}
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || r.receipt.RequestsSent != 3 {
		t.Fatal("no early refetch")
	}
	c.Due["42"].At = time.Now().Add(-time.Hour)
	zero := 0
	if err := r.xContexts(context.Background(), binary, "unused", &zero, c); err != nil || len(r.receipt.ContextSkipped) != 1 || c.Due["42"] == nil || len(c.Contexts["42"].Posts) != 3 {
		t.Fatal("spent allowance must preserve context and record a deferred refetch")
	}
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || c.Due["42"] != nil || c.Contexts["42"] != nil || r.receipt.RequestsSent != 6 {
		t.Fatal("one 24h refetch must retire the due entry and cached context")
	}
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || r.receipt.RequestsSent != 6 {
		t.Fatal("never refetch automatically again")
	}
	before = r.receipt.Written
	if err := r.xTweet(raw); err != nil || r.receipt.Written != before || len(c.Due) != 0 {
		t.Fatal("ordinary listing after retirement must not overwrite the enriched doc")
	}
	edited := json.RawMessage(strings.Replace(string(raw), "synthetic 42", "edited 42", 1))
	os.WriteFile(filepath.Join(root, "edited"), []byte("edited 42"), 0600)
	if err := r.xTweet(edited); err != nil || r.receipt.Written != before || c.Due["42"] == nil || c.Due["42"].Initial {
		t.Fatal("edit must schedule one immediate context fetch, without a bare overwrite")
	}
	if err := r.xContexts(context.Background(), binary, "unused", &zero, c); err != nil || r.receipt.Written != before || len(c.Contexts) != 0 || c.Due["42"] == nil {
		t.Fatal("budget-deferred edit must leave the prior archived context intact")
	}
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || r.receipt.RequestsSent != 9 || len(c.Contexts) != 0 || len(c.Due) != 0 || r.receipt.Written != before+1 {
		t.Fatal("edit fetch must explicitly emit once and retire its cache immediately")
	}
	if err := r.xTweet(edited); err != nil || r.receipt.Written != before+1 || len(c.Due) != 0 {
		t.Fatal("enriched emission must retain the own-tweet listing digest")
	}
	// The real CLI shares the checkpoint but must preserve the scheduled run's receipt.
	r.receipt = Receipt{Source: "x", HeadComplete: true, RequestsSent: 17}
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
	previous, err := os.ReadFile(r.statePath)
	if err != nil {
		t.Fatal(err)
	}
	stateDir := filepath.Join(root, "state")
	os.MkdirAll(stateDir, 0700)
	stateFile := filepath.Join(stateDir, "x-"+digest([]byte(r.account))[:16]+".json")
	os.WriteFile(stateFile, previous, 0600)
	receipt, code := captureChild(t, []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1", "QMD_X_BIRD_BIN=" + binary}, "x", "--account", "123", "--x-id", "42", "--spool", r.spool, "--state", stateDir, "--env-file", filepath.Join(root, "absent"))
	if code != 0 || receipt.RequestsSent != 4 {
		t.Fatalf("demand CLI: code=%d receipt=%+v", code, receipt)
	}
	var checkpoint checkpoint
	saved, _ := os.ReadFile(stateFile)
	if json.Unmarshal(saved, &checkpoint) != nil || checkpoint.Receipt.RequestsSent != 17 || !checkpoint.Receipt.HeadComplete {
		t.Fatal("demand must not replace the scheduled capture receipt")
	}
	var demanded xCursor
	if json.Unmarshal(checkpoint.Cursor, &demanded) != nil || len(demanded.Contexts) != 0 || len(demanded.Due) != 0 {
		t.Fatal("on-demand context must not grow the cursor")
	}
	entries, _ := os.ReadFile(filepath.Join(r.spool, "index.jsonl"))
	lines := bytes.Split(bytes.TrimSpace(entries), []byte("\n"))
	var entry SpoolEntry
	json.Unmarshal(lines[len(lines)-1], &entry)
	payload, _ := os.ReadFile(filepath.Join(r.spool, entry.Path))
	if bytes.Contains(payload, []byte("synthetic 13")) || !bytes.Contains(payload, []byte("x_context")) || !bytes.Contains(payload, []byte("edited 42")) {
		t.Fatal("only scoped provider context and the current edit are archived")
	}
	// Initial root context uses held bytes; its follow-up also refreshes the focal.
	os.WriteFile(filepath.Join(root, "context-mode"), []byte("root"), 0600)
	rootRaw := json.RawMessage(strings.Replace(string(edited), `"inReplyToStatusId":"10"`, `"inReplyToStatusId":null`, 1))
	c.Due["42"] = &xDue{At: time.Now().Add(time.Hour), Tweet: rootRaw, Initial: true}
	requests := r.receipt.RequestsSent
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || r.receipt.RequestsSent != requests+1 || len(c.Contexts) != len(c.Due) {
		t.Fatal("root context must use only author search and retain cache only while pending")
	}
	c.Due["42"].At = time.Now().Add(-time.Second)
	if err := r.xContexts(context.Background(), binary, "unused", &remaining, c); err != nil || r.receipt.RequestsSent != requests+3 || len(c.Contexts) != 0 || len(c.Due) != 0 {
		t.Fatal("root follow-up must refresh focal detail and retire all cursor payloads")
	}
	c.Contexts["orphan"] = captured
	if err := r.xCommit(c); err != nil {
		t.Fatal(err)
	}
	loaded, err := r.loadXCursor()
	if err != nil || len(loaded.Contexts) != 0 {
		t.Fatal("loading an old cursor must prune completed orphan context")
	}
}
