package fetch

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestCalendarExpiredSyncHTTPEdge(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	config := filepath.Join(root, ".config/google-api")
	os.MkdirAll(config, 0700)
	os.WriteFile(filepath.Join(config, "tokens-synthetic.json"), []byte(`{"account":"synthetic","access_token":"synthetic","expiry_date":4102444800000}`), 0600)
	statePath := filepath.Join(root, "state.json")
	full := 0
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/gmail/v1/users/me/profile":
			w.Write([]byte(`{"emailAddress":"synthetic"}`))
		case "/calendar/v3/calendars/current/events":
			if req.URL.Query().Get("syncToken") == "expired" {
				w.WriteHeader(http.StatusGone)
				return
			}
			if req.URL.Query().Get("pageToken") != "" {
				t.Error("expired page reused in full listing")
			}
			full++
			if full == 1 {
				w.WriteHeader(http.StatusForbidden)
				return
			}
			w.Write([]byte(`{"items":[{"id":"new","updated":"2026-10-20T00:00:00Z"}],"nextSyncToken":"fresh"}`))
		default:
			t.Errorf("unexpected request path %s", req.URL.Path)
			w.WriteHeader(404)
		}
	}))
	defer server.Close()
	client := server.Client()
	transport := client.Transport.(*http.Transport)
	transport.TLSClientConfig.ServerName = "example.com"
	transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	cursor := calendarCursor{V: "calendar-sync-v1", At: "current", Cals: []string{"other", "current"}, Page: "stale-page", Tokens: map[string]string{"other": "retained", "current": "expired"}}
	r := &run{source: "calendar", account: "synthetic", client: client, spool: filepath.Join(root, "spool"), statePath: statePath}
	r.state.Hashes = map[string]string{}
	r.state.Cursor, _ = json.Marshal(cursor)
	if err := runCalendar(context.Background(), r); err == nil {
		t.Fatal("full-listing HTTP 403 must remain an error")
	}
	body, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatalf("reset was not durable: %v", err)
	}
	var saved checkpoint
	if err := json.Unmarshal(body, &saved); err != nil {
		t.Fatal(err)
	}
	cursor = calendarCursor{}
	json.Unmarshal(saved.Cursor, &cursor)
	if cursor.Tokens["current"] != "" || cursor.Page != "" || cursor.Tokens["other"] != "retained" {
		t.Fatalf("incorrect reset: %+v", cursor)
	}
	r.state = saved
	if err := runCalendar(context.Background(), r); err != nil || !r.receipt.HeadComplete || r.receipt.Written != 1 {
		t.Fatalf("restart failed: %v receipt=%+v", err, r.receipt)
	}
	cursor = calendarCursor{}
	json.Unmarshal(r.state.Cursor, &cursor)
	if cursor.Tokens["current"] != "fresh" || cursor.Tokens["other"] != "retained" {
		t.Fatalf("incorrect recovered tokens: %+v", cursor)
	}
}
