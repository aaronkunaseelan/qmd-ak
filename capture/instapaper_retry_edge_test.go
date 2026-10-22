package fetch

import (
	"encoding/json"
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
