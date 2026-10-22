package fetch

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestInstapaperHTTPSubprocess(t *testing.T) {
	for _, status := range []int{400, 200} {
		t.Run(http.StatusText(status), func(t *testing.T) { testInstapaperHTTPSubprocess(t, status) })
	}
}

func testInstapaperHTTPSubprocess(t *testing.T, initialStatus int) {
	root := t.TempDir()
	spool := filepath.Join(root, "spool")
	state := filepath.Join(root, "state")
	var badStatus atomic.Int32
	var listings atomic.Int32
	var textRequests atomic.Int32
	badStatus.Store(int32(initialStatus))
	server := instapaperTextServer(&badStatus, &listings, &textRequests)
	defer server.Close()
	first, code := ipTestChild(t, root, server.URL)
	if code != 0 || !first.OK || !first.HeadComplete || first.Written != 1 || first.Failed != 0 || first.Excluded != 1 || first.ExclusionReasons["text_unavailable"] != 1 || !first.SourceHead.Equal(time.Unix(1791856800, 0)) {
		t.Fatalf("first permanent receipt %+v", first)
	}
	path := filepath.Join(state, "instapaper-"+digest([]byte("42"))[:16]+".json")
	var saved checkpoint
	var cursor ipCursor
	readIPState(t, path, &saved, &cursor)
	if p := cursor.Pending["1"]; p.Tries != 1 || p.ExcludedReason != "text_unavailable" || time.Until(p.RetryAt) < 23*time.Hour {
		t.Fatal("first permanent answer must establish daily exclusion")
	}
	before := textRequests.Load()
	second, code := ipTestChild(t, root, server.URL)
	if code != 0 || !second.OK || second.Excluded != 1 || second.Failed != 0 || textRequests.Load() != before || !second.SourceHead.Equal(first.SourceHead) {
		t.Fatalf("deferred receipt %+v", second)
	}
	makeIPRetryDue(t, path)
	badStatus.Store(503)
	third, code := ipTestChild(t, root, server.URL)
	if code != 0 || !third.OK || third.Failed != 0 || third.Excluded != 1 || third.ExclusionReasons["text_unavailable"] != 1 || textRequests.Load() != before+2 || !third.OldestFailed.IsZero() || len(third.FailedIDs) != 0 || !third.SourceHead.Equal(first.SourceHead) {
		t.Fatalf("excluded transient receipt %+v", third)
	}
	readIPState(t, path, &saved, &cursor)
	if time.Until(cursor.Pending["1"].RetryAt) < 23*time.Hour {
		t.Fatal("transient daily attempt must defer another day")
	}
	beforeListings := listings.Load()
	before = textRequests.Load()
	partial, code := ipTestChild(t, root, server.URL, "--pending-only")
	if code == 0 || partial.OK || partial.HeadComplete || !partial.HasMore || partial.Excluded != 1 || partial.Failed != 0 || listings.Load() != beforeListings || textRequests.Load() != before {
		t.Fatal("pending-only must defer excluded item without listing")
	}
	makeIPRetryDue(t, path)
	badStatus.Store(201)
	fifth, code := ipTestChild(t, root, server.URL)
	if code != 0 || !fifth.OK || fifth.Written != 1 || fifth.Failed != 0 || !fifth.SourceHead.Equal(time.Unix(1791856800, 0)) {
		t.Fatalf("recovered receipt %+v", fifth)
	}
	checkIPTextSnapshots(t, spool)
}

func instapaperTextServer(badStatus, listings, textRequests *atomic.Int32) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.ParseForm()
		switch req.URL.Path {
		case "/api/1/account/verify_credentials":
			w.Write([]byte(`[{"type":"user","user_id":42,"username":"synthetic"}]`))
		case "/api/1/bookmarks/list":
			listings.Add(1)
			if req.Form.Get("folder_id") == "unread" && req.Form.Get("have") == "" {
				w.Write([]byte(`[{"type":"bookmark","bookmark_id":1,"hash":"a","time":1791856800,"title":"Unavailable"},{"type":"bookmark","bookmark_id":2,"hash":"b","time":1791849601,"title":"Available"}]`))
			} else {
				w.Write([]byte(`[]`))
			}
		case "/api/1/bookmarks/get_text":
			textRequests.Add(1)
			if req.Form.Get("bookmark_id") == "1" && badStatus.Load() != 201 {
				w.WriteHeader(int(badStatus.Load()))
				return
			}
			w.Write([]byte("<p>synthetic authored article</p>"))
		default:
			w.WriteHeader(404)
		}
	}))
	return server
}

func checkIPTextSnapshots(t *testing.T, spool string) {
	t.Helper()
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	entries := splitLines(index)
	if len(entries) != 3 {
		t.Fatal("available, unavailable marker and recovered snapshot expected")
	}
	var marker, recovered SpoolEntry
	json.Unmarshal(entries[0], &marker)
	json.Unmarshal(entries[2], &recovered)
	markerBytes, _ := os.ReadFile(filepath.Join(spool, marker.Path))
	recoveredBytes, _ := os.ReadFile(filepath.Join(spool, recovered.Path))
	if marker.Key != "1" || marker.State.Labels["reason"] != "text_unavailable" || !strings.Contains(string(markerBytes), `"text_unavailable":true`) || strings.Contains(string(recoveredBytes), `"text_unavailable":true`) {
		t.Fatal("unavailable marker or later replacement invalid")
	}
}

func ipTestChild(t *testing.T, root, endpoint string, extra ...string) (Receipt, int) {
	t.Helper()
	args := []string{"instapaper", "--account", "42", "--spool", filepath.Join(root, "spool"), "--state", filepath.Join(root, "state"), "--env-file", filepath.Join(root, "absent.env")}
	return captureChild(t, []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1", "QMD_INSTAPAPER_BASE_URL=" + endpoint, "QMD_INSTAPAPER_CONSUMER_KEY=synthetic", "QMD_INSTAPAPER_CONSUMER_SECRET=synthetic", "QMD_INSTAPAPER_ACCESS_TOKEN=synthetic", "QMD_INSTAPAPER_ACCESS_TOKEN_SECRET=synthetic"}, append(args, extra...)...)
}

func readIPState(t *testing.T, path string, saved *checkpoint, cursor *ipCursor) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(b, saved) != nil || json.Unmarshal(saved.Cursor, cursor) != nil {
		t.Fatal("checkpoint invalid")
	}
}

func writeIPState(t *testing.T, path string, saved checkpoint) {
	t.Helper()
	b, _ := json.Marshal(saved)
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
}

func makeIPRetryDue(t *testing.T, path string) {
	t.Helper()
	var saved checkpoint
	var cursor ipCursor
	readIPState(t, path, &saved, &cursor)
	p := cursor.Pending["1"]
	p.RetryAt = time.Now().Add(-time.Second)
	cursor.Pending["1"] = p
	saved.Cursor, _ = json.Marshal(cursor)
	writeIPState(t, path, saved)
}

func ipStateChild(t *testing.T, root, endpoint, path string, saved *checkpoint, cursor *ipCursor) Receipt {
	t.Helper()
	r, _ := ipTestChild(t, root, endpoint)
	*cursor = ipCursor{}
	readIPState(t, path, saved, cursor)
	return r
}

func TestInstapaperPersistentHTTPSubprocess(t *testing.T) {
	for _, row := range []struct {
		name     string
		age      time.Duration
		tries    int
		healthy  bool
		excluded bool
	}{{"too_young", 5 * time.Hour, 9, true, false}, {"too_few_tries", 7 * time.Hour, 8, true, false}, {"outage", 7 * time.Hour, 9, false, false}, {"persistent", 6 * time.Hour, 9, true, true}} {
		t.Run(row.name, func(t *testing.T) {
			testInstapaperPersistent(t, row.age, row.tries, row.healthy, row.excluded)
		})
	}
}

func testInstapaperPersistent(t *testing.T, age time.Duration, tries int, healthy, excluded bool) {
	t.Helper()
	root := t.TempDir()
	state := filepath.Join(root, "state")
	os.Mkdir(state, 0700)
	var badStatus atomic.Int32
	var textRequests atomic.Int32
	badStatus.Store(504)
	server := instapaperPersistentServer(&badStatus, &textRequests)
	defer server.Close()
	firstFailure := time.Now().UTC().Add(-age)
	cursor := ipCursor{Have: map[string]map[string]string{}, Pending: map[string]ipPending{
		"1": {Bookmark: json.RawMessage(`{"bookmark_id":1,"time":1791856800}`), Folder: "unread", Hash: "a", Tries: tries, FailedAt: firstFailure},
	}}
	if healthy {
		cursor.Pending["2"] = ipPending{Bookmark: json.RawMessage(`{"bookmark_id":2,"time":1791849601}`), Folder: "unread", Hash: "b"}
	}
	cp := checkpoint{Version: 1, Hashes: map[string]string{}}
	cp.Cursor, _ = json.Marshal(cursor)
	path := filepath.Join(state, "instapaper-"+digest([]byte("42"))[:16]+".json")
	writeIPState(t, path, cp)
	r := ipStateChild(t, root, server.URL, path, &cp, &cursor)
	p := cursor.Pending["1"]
	wantHead := time.Time{}
	if healthy {
		wantHead = time.Unix(1791849601, 0)
	}
	if r.Excluded > 0 {
		wantHead = time.Unix(1791856800, 0)
	}
	if !r.SourceHead.Equal(wantHead) {
		t.Fatal("capture head does not match newest captured outcome")
	}
	if !p.FirstFailedAt.Equal(firstFailure) {
		t.Fatal("legacy failure age not preserved")
	}
	if !excluded {
		if r.OK || r.Failed != 1 || r.Excluded != 0 || p.ExcludedReason != "" {
			t.Fatal("threshold or outage guard failed")
		}
		return
	}
	if !r.OK || r.Failed != 0 || r.Excluded != 1 || r.ExclusionReasons["provider_persistent_http_504"] != 1 || len(r.FailedIDs) != 0 || !r.OldestFailed.IsZero() || time.Until(p.RetryAt) < 23*time.Hour {
		t.Fatal("persistent exclusion invalid")
	}
	checkIPPersistentRecovery(t, root, server.URL, path, &badStatus, &textRequests, cp, cursor)
}

func instapaperPersistentServer(badStatus, textRequests *atomic.Int32) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		req.ParseForm()
		switch req.URL.Path {
		case "/api/1/account/verify_credentials":
			w.Write([]byte(`[{"type":"user","user_id":42}]`))
		case "/api/1/bookmarks/list":
			w.Write([]byte(`[]`))
		case "/api/1/bookmarks/get_text":
			textRequests.Add(1)
			if req.Form.Get("bookmark_id") == "1" && badStatus.Load() != 200 {
				w.WriteHeader(int(badStatus.Load()))
				return
			}
			w.Write([]byte("<p>synthetic recovered article</p>"))
		default:
			w.WriteHeader(404)
		}
	}))
	return server
}

func checkIPPersistentRecovery(t *testing.T, root, endpoint, path string, badStatus, textRequests *atomic.Int32, cp checkpoint, cursor ipCursor) {
	t.Helper()
	spool := filepath.Join(root, "spool")
	before := textRequests.Load()
	r := ipStateChild(t, root, endpoint, path, &cp, &cursor)
	if !r.OK || r.Excluded != 1 || textRequests.Load() != before {
		t.Fatal("daily retry was not deferred")
	}
	p := cursor.Pending["1"]
	p.RetryAt = time.Now().Add(-time.Second)
	cursor.Pending["1"] = p
	cp.Cursor, _ = json.Marshal(cursor)
	writeIPState(t, path, cp)
	r = ipStateChild(t, root, endpoint, path, &cp, &cursor)
	if !r.OK || r.Failed != 0 || r.Excluded != 1 || r.ExclusionReasons["provider_persistent_http_504"] != 1 {
		t.Fatal("due transient daily retry must preserve its established exclusion")
	}
	before = textRequests.Load()
	ipStateChild(t, root, endpoint, path, &cp, &cursor)
	if textRequests.Load() != before {
		t.Fatal("failed daily attempt retried too early")
	}
	p = cursor.Pending["1"]
	p.RetryAt = time.Now().Add(-time.Second)
	cursor.Pending["1"] = p
	cp.Cursor, _ = json.Marshal(cursor)
	writeIPState(t, path, cp)
	badStatus.Store(200)
	r = ipStateChild(t, root, endpoint, path, &cp, &cursor)
	if !r.OK || r.Written != 1 || len(cursor.Pending) != 0 || !r.SourceHead.Equal(time.Unix(1791856800, 0)) {
		t.Fatal("daily recovery did not replace exclusion")
	}
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	entries := splitLines(index)
	if len(entries) != 3 {
		t.Fatal("healthy article, exclusion and recovered article expected")
	}
	var marker, recovered SpoolEntry
	json.Unmarshal(entries[1], &marker)
	json.Unmarshal(entries[2], &recovered)
	if marker.State.Labels["reason"] != "provider_persistent_http_504" || recovered.State.Labels["reason"] != "" {
		t.Fatal("marker/recovery reason invalid")
	}
}
