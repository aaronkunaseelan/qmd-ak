package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestBirdFailureTable(t *testing.T) {
	for _, row := range []struct{ private, want string }{{"HTTP 404 private details", "bird provider HTTP 404"}, {"statusCode: 429 private details", "bird provider HTTP 429"}, {"unknown option private details", "bird unsupported option"}, {"unauthorized private details", "bird credential refused"}, {"Failed to determine current user: private details", "bird identity unavailable"}, {"Could not determine current user", "bird identity unavailable"}, {"Could not parse settings page for user info", "bird identity unavailable"}, {"bird identity unavailable private details", "bird identity unavailable"}, {"private details", "bird subprocess failed"}} {
		if got := birdFailure(row.private).Error(); got != row.want {
			t.Errorf("classification %q want %q", got, row.want)
		}
	}
}
func TestBirdIdentitySubprocess(t *testing.T) {
	for _, row := range []struct {
		name, body string
		status     int
		want       string
	}{
		{"viewer", `{"data":{"viewer":{"user_results":{"result":{"rest_id":"123","core":{"screen_name":"synthetic"}}}}}}`, 200, "123"},
		{"missing", `{"data":{"viewer":null}}`, 200, "bird identity unavailable"},
		{"invalid-json", `private details`, 200, "bird identity unavailable"},
		{"numeric-id", `{"data":{"viewer":{"user_results":{"result":{"rest_id":123,"core":{"screen_name":"synthetic"}}}}}}`, 200, "bird identity unavailable"},
		{"missing-name", `{"data":{"viewer":{"user_results":{"result":{"rest_id":"123"}}}}}`, 200, "bird identity unavailable"},
		{"graphql-error", `{"errors":[{"message":"private details"}]}`, 200, "bird identity unavailable"},
		{"retired-query", `private details`, 404, "bird identity unavailable"},
		{"rate-limit", `private details`, 429, "xbird: rate_limited"},
	} {
		t.Run(row.name, func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				requests++
				if !strings.HasSuffix(req.URL.Path, "/viewerQuery") || req.Header.Get("X-Csrf-Token") != "synthetic" || !strings.Contains(req.URL.Query().Get("variables"), `:true`) {
					t.Error("viewer request must retain authentication and logged-in variable")
				}
				w.WriteHeader(row.status)
				fmt.Fprint(w, row.body)
			}))
			defer server.Close()
			root := t.TempDir()
			bootstrap := filepath.Join(root, "network.cjs")
			preload := filepath.Join(root, "preload.cjs")
			receipt := filepath.Join(root, "receipt.json")
			// Route the subprocess's real HTTP edge to a synthetic local provider.
			os.WriteFile(bootstrap, []byte(`const original = fetch; globalThis.fetch = (url, init) => original(process.env.TEST_ORIGIN + new URL(url).pathname + new URL(url).search, init);`), 0600)
			os.WriteFile(preload, birdPreload, 0600)
			program := `fetch('https://x.com/i/api/account/settings.json', {headers: {'x-csrf-token':'synthetic'}}).then(r=>r.json()).then(user=>{if(user.screen_name!=='synthetic')throw Error('bad name');console.log(user.user_id)}).catch(async e=>{try{await fetch('https://api.twitter.com/1.1/account/settings.json')}catch{}console.log(e.message)});`
			cmd := exec.Command("node", "-r", bootstrap, "-r", preload, "-e", program)
			cmd.Env = append(os.Environ(), "NODE_OPTIONS=", "TEST_ORIGIN="+server.URL, "XBIRD_RECEIPT="+receipt, "XBIRD_REMAINING=3")
			output, err := cmd.CombinedOutput()
			if err != nil || strings.TrimSpace(string(output)) != row.want {
				t.Fatalf("identity output=%q err=%v", output, err)
			}
			raw, err := os.ReadFile(receipt)
			var counts struct {
				Requests int  `json:"requests_sent"`
				Limited  bool `json:"rate_limited"`
			}
			if err != nil || json.Unmarshal(raw, &counts) != nil || counts.Requests != 1 || requests != 1 || counts.Limited != (row.status == 429) {
				t.Fatalf("identity must count one request and never fall back: %s, requests=%d", raw, requests)
			}
		})
	}
}

func TestBirdAccountAssertionSubprocess(t *testing.T) {
	for _, account := range []string{"123", "456"} {
		t.Run(account, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "bird")
			writeBirdTestCLI(t, binary)
			os.WriteFile(filepath.Join(root, "mode"), []byte("headonly"), 0600)
			r := seedXCapture(root, binary)
			r.account = digest([]byte(account))
			err := runX(context.Background(), r)
			if account == "123" && err != nil {
				t.Fatal(err)
			}
			if account != "123" && (err == nil || err.Error() != "bird account assertion failed" || r.receipt.Enumerated != 0) {
				t.Fatal("wrong account must fail before listing or writing")
			}
		})
	}
}

func TestTweetVersionTable(t *testing.T) {
	for _, row := range []struct {
		before, after string
		same          bool
	}{{`{"id":"1","text":"hello","likeCount":1,"_raw":{"legacy":{"favorite_count":1}}}`, `{"id":"1","text":"hello","likeCount":99,"_raw":{"legacy":{"favorite_count":99}}}`, true}, {`{"id":"1","text":"hello","_raw":{"normal_followers_count":1,"media_count":2}}`, `{"id":"1","text":"hello","_raw":{"normal_followers_count":9,"media_count":8}}`, true},
		{`{"id":"1","text":"hello","_raw":{"card":{"legacy":{"binding_values":[{"key":"image","value":{"url":"one"}}]}},"core":{"user_results":{"result":{"legacy":{"following":false}}}}}}`, `{"id":"1","text":"hello","_raw":{"card":{"legacy":{"binding_values":[{"key":"image","value":{"url":"two"}}]}},"core":{"user_results":{"result":{"relationship_perspectives":{"following":true}}}}}}`, true},
		{`{"id":"1","quotedTweet":{"id":"2","text":"quote","_raw":{"views":{"count":"1"}}}}`, `{"id":"1","quotedTweet":{"id":"2","text":"quote","_raw":{"views":{"count":"2"},"card":{"binding_values":[]}}}}`, true},
		{`{"id":"1","quotedTweet":{"id":"2","text":"quote"}}`, `{"id":"1","quotedTweet":{"id":"2","text":"edited quote"}}`, false},
		{`{"id":"1","media":[{"url":"one"}]}`, `{"id":"1","media":[{"url":"two"}]}`, false},
		{`{"id":"1","article":{"text":"one"}}`, `{"id":"1","article":{"text":"two"}}`, false},
		{`{"id":"1","createdAt":"one"}`, `{"id":"1","createdAt":"two"}`, false},
		{`{"id":"1","text":"hello","account":"same"}`, `{"id":"1","text":"changed","account":"same"}`, false}, {`{"id":"1","text":"hello","account":"one"}`, `{"id":"1","text":"hello","account":"two"}`, false}} {
		var a, b any
		json.Unmarshal([]byte(row.before), &a)
		json.Unmarshal([]byte(row.after), &b)
		x, _ := json.Marshal(stableTweet(a))
		y, _ := json.Marshal(stableTweet(b))
		if (string(x) == string(y)) != row.same {
			t.Fatal("post content/counter version contract failed")
		}
	}
}
