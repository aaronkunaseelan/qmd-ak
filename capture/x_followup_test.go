package fetch

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestXFollowupRefreshesFocalSubprocess(t *testing.T) {
	for _, outcome := range []string{"updated", "refused", "omitted"} {
		t.Run(outcome, func(t *testing.T) {
			root := t.TempDir()
			bird := filepath.Join(root, "bird")
			program := `#!/usr/bin/python3
import json, os, pathlib, sys
root = pathlib.Path(__file__).parent
args = sys.argv[1:]
with (root / 'calls').open('a') as log:
    log.write(json.dumps(args) + '\n')
pathlib.Path(os.environ['XBIRD_RECEIPT']).write_text(json.dumps({'requests_sent': 1}))
mode = (root / 'mode').read_text()
if 'thread' in args:
    if mode == 'refused':
        print('HTTP 404', file=sys.stderr)
        sys.exit(1)
    if mode == 'updated':
        print(json.dumps({'tweets': [{'id': '100', 'text': 'updated focal', 'createdAt': '2026-10-28T00:00:00Z', 'author': {'username': 'author'}, 'conversationId': '100'}]}))
        sys.exit(0)
print(json.dumps({'tweets': []}))
`
			if err := os.WriteFile(bird, []byte(program), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "mode"), []byte(outcome), 0600); err != nil {
				t.Fatal(err)
			}
			r := seedXCapture(root, bird)
			c, err := r.loadXCursor()
			if err != nil {
				t.Fatal(err)
			}
			r.xstate = c
			c.Due["100"] = &xDue{At: time.Now().Add(-time.Hour), Tweet: json.RawMessage(`{"id":"100","text":"old focal","createdAt":"2026-10-27T00:00:00Z","author":{"username":"author"},"conversationId":"100"}`)}
			c.Contexts["100"] = &xContext{Posts: []xContextPost{}, Notices: []string{}}
			remaining := 26
			if err := r.xContexts(context.Background(), bird, "unused", &remaining, c); err != nil {
				t.Fatal(err)
			}
			calls, err := os.ReadFile(filepath.Join(root, "calls"))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(calls), `"thread"`) {
				t.Fatal("follow-up retired without focal lookup")
			}
			entries, err := os.ReadFile(filepath.Join(r.spool, "index.jsonl"))
			if err != nil {
				t.Fatal(err)
			}
			var entry SpoolEntry
			if err := json.Unmarshal([]byte(strings.TrimSpace(string(entries))), &entry); err != nil {
				t.Fatal(err)
			}
			archived, err := os.ReadFile(filepath.Join(r.spool, entry.Path))
			if err != nil {
				t.Fatal(err)
			}
			var envelope struct {
				Tweet struct {
					Text string `json:"text"`
				} `json:"tweet"`
				Context xContext `json:"x_context"`
			}
			if err := json.Unmarshal(archived, &envelope); err != nil {
				t.Fatal(err)
			}
			if outcome == "updated" {
				if envelope.Tweet.Text != "updated focal" {
					t.Fatal("fresh focal was not retained")
				}
			} else if !strings.Contains(strings.Join(envelope.Context.Notices, " "), "post 100 unavailable") {
				t.Fatalf("focal refusal not named: %+v", envelope.Context.Notices)
			}
			if c.Due["100"] != nil {
				t.Fatal("completed follow-up still due")
			}
		})
	}
}
