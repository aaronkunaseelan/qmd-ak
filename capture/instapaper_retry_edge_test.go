package fetch

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestBR1Instapaper429RemainsTransient(t *testing.T) {
	root := t.TempDir()
	var status, listings, requests atomic.Int32
	status.Store(429)
	server := instapaperTextServer(&status, &listings, &requests)
	defer server.Close()
	receipt, _ := ipTestChild(t, root, server.URL)
	path := filepath.Join(root, "state", "instapaper-"+digest([]byte("42"))[:16]+".json")
	var saved checkpoint
	var cursor ipCursor
	readIPState(t, path, &saved, &cursor)
	pending := cursor.Pending["1"]
	if receipt.Excluded != 0 || pending.ExcludedReason != "" {
		t.Fatalf("temporary HTTP 429 became permanent: excluded=%d failed=%d reason=%q retry_in=%v rate_limited=%v",
			receipt.Excluded, receipt.Failed, pending.ExcludedReason, time.Until(pending.RetryAt), receipt.RateLimited)
	}
}

func TestBR1InstapaperPersistentDailyRetryStaysExcluded(t *testing.T) {
	root := t.TempDir()
	var status, listings, requests atomic.Int32
	status.Store(503)
	server := instapaperTextServer(&status, &listings, &requests)
	defer server.Close()
	ipTestChild(t, root, server.URL)
	path := filepath.Join(root, "state", "instapaper-"+digest([]byte("42"))[:16]+".json")
	var saved checkpoint
	var cursor ipCursor
	readIPState(t, path, &saved, &cursor)
	pending := cursor.Pending["1"]
	pending.Tries = 11
	pending.ExcludedReason = "provider_persistent_http_503"
	pending.FirstFailedAt = time.Now().Add(-48 * time.Hour)
	pending.RetryAt = time.Now().Add(-time.Minute)
	cursor.Pending["1"] = pending
	saved.Cursor, _ = json.Marshal(cursor)
	writeIPState(t, path, saved)
	receipt, _ := ipTestChild(t, root, server.URL)
	if receipt.Failed != 0 || receipt.Excluded < 1 {
		t.Fatalf("daily excluded retry reopened failure: failed=%d excluded=%d reason=%v", receipt.Failed, receipt.Excluded, receipt.FailureReasons)
	}
}

func TestBR1InstapaperTimeoutPastLaneDeadlineExcludes(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	os.Mkdir(state, 0700)
	var status, listings, requests atomic.Int32
	status.Store(504)
	server := instapaperTextServer(&status, &listings, &requests)
	defer server.Close()
	firstFailure := time.Now().UTC().Add(-2 * time.Hour)
	cursor := ipCursor{Have: map[string]map[string]string{}, Pending: map[string]ipPending{
		"1": {Bookmark: json.RawMessage(`{"bookmark_id":1,"time":1791856800}`), Folder: "unread", Hash: "a", Tries: 3, FailedAt: firstFailure, FirstFailedAt: firstFailure},
	}}
	cp := checkpoint{Version: 1, Hashes: map[string]string{}}
	cp.Cursor, _ = json.Marshal(cursor)
	path := filepath.Join(state, "instapaper-"+digest([]byte("42"))[:16]+".json")
	writeIPState(t, path, cp)
	receipt, code := ipTestChild(t, root, server.URL)
	if code != 0 || !receipt.OK || !receipt.HeadComplete || receipt.Failed != 0 || receipt.Excluded != 1 || receipt.ExclusionReasons["text_timeout"] != 1 || len(receipt.FailedIDs) != 0 || !receipt.OldestFailed.IsZero() {
		t.Fatalf("a failure past the lane deadline still holds the lane: %+v", receipt)
	}
	cursor = ipCursor{}
	readIPState(t, path, &cp, &cursor)
	if p := cursor.Pending["1"]; p.ExcludedReason != "text_timeout" || time.Until(p.RetryAt) < 23*time.Hour {
		t.Fatalf("exclusion is not retried daily: reason=%q retry_in=%v", p.ExcludedReason, time.Until(p.RetryAt))
	}
	before := requests.Load()
	again, _ := ipTestChild(t, root, server.URL)
	if !again.OK || again.Failed != 0 || again.Excluded != 1 || requests.Load() != before {
		t.Fatalf("deferred exclusion reopened a failure: failed=%d excluded=%d requests=%d", again.Failed, again.Excluded, requests.Load()-before)
	}
}
