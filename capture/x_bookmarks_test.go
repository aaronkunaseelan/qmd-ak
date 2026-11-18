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

// A save that leaves the bookmarks list is only visible in a full walk; once the walk settles, the demand fetch
// re-reads the post live and its new version carries no bookmark.
func TestXBookmarkWalkRemovalChain(t *testing.T) {
	root := t.TempDir()
	bird := filepath.Join(root, "bird")
	writeBirdTestCLI(t, bird)
	os.WriteFile(filepath.Join(root, "mode"), []byte("saved"), 0600)
	writeBookmarkList(t, root, [][]string{{"saved1", "saved2"}})
	r := seedXCapture(root, bird)
	seedXKnown(t, r, "saved1", "saved2")
	// History is complete, so the run spends its allowance on the head, the walk and the queued refresh.
	r.state.Cursor = json.RawMessage(`{"backfill":{"list":0,"cursor":"","done":true}}`)

	if err := runX(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c := xCursorOf(t, r)
	if strings.Join(c.Bookmarks, ",") != "saved1,saved2" || c.BookmarksWalkedAt.IsZero() || c.BookmarksWalk != nil {
		t.Fatalf("the first walk only seeds the settled set: %+v", c)
	}
	if len(c.Due) != 0 || r.receipt.Written != 0 || r.receipt.RequestsSent != 15 {
		t.Fatalf("a walk over known saves writes and queues nothing: %+v sent=%d", c.Due, r.receipt.RequestsSent)
	}

	// Inside the day a save that left the list is not seen, because the list is not walked.
	writeBookmarkList(t, root, [][]string{{"saved1"}})
	if err := runX(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c = xCursorOf(t, r)
	if len(c.Due) != 0 || strings.Join(c.Bookmarks, ",") != "saved1,saved2" {
		t.Fatalf("no walk inside 24 h decides no removal: due=%+v settled=%v", c.Due, c.Bookmarks)
	}

	// A day later the walk runs to the end and queues the save that left the list.
	ageXWalk(t, r, 25*time.Hour)
	if err := runX(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c = xCursorOf(t, r)
	if strings.Join(c.Bookmarks, ",") != "saved1" || c.BookmarksWalk != nil {
		t.Fatalf("a completed walk settles what it saw: %v", c.Bookmarks)
	}
	if len(c.Due) != 1 || c.Due["saved2"] == nil || c.Due["saved2"].Initial {
		t.Fatalf("the removal must be queued for a per-post refresh: %+v", c.Due)
	}
	if bytes.Contains(c.Due["saved2"].Tweet, []byte("bookmarked")) {
		t.Fatal("the queued refresh carries the live post, never the archived flags")
	}

	// The queued refresh re-reads the post and writes a version without the bookmark.
	if err := runX(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c = xCursorOf(t, r)
	if c.Due["saved2"] != nil || c.Contexts["saved2"] != nil || r.receipt.Written != 1 {
		t.Fatalf("the demand fetch must complete the removal: due=%+v written=%d", c.Due, r.receipt.Written)
	}
	payloads := spoolPayloads(t, r, "saved2")
	if len(payloads) != 1 || bytes.Contains(payloads[0], []byte("bookmarked")) {
		t.Fatalf("one live version without the bookmark flag: %d payloads", len(payloads))
	}
}

// A walk cut short by the budget or an error keeps its cursor and decides no removal.
func TestXBookmarkWalkPartialResumes(t *testing.T) {
	root := t.TempDir()
	bird := filepath.Join(root, "bird")
	writeBirdTestCLI(t, bird)
	os.WriteFile(filepath.Join(root, "mode"), []byte("saved"), 0600)
	writeBookmarkList(t, root, [][]string{{"saved1"}, {"saved3"}})
	r := seedXCapture(root, bird)
	seedXKnown(t, r, "saved1", "saved3")

	c := &xCursor{Bookmarks: []string{"saved1", "saved2"}, BookmarksWalkedAt: time.Now().Add(-25 * time.Hour), Due: map[string]*xDue{}}
	r.xstate = c
	remaining := 1
	if err := r.xBookmarks(context.Background(), bird, "unused", &remaining, c); err != nil {
		t.Fatal(err)
	}
	if c.BookmarksWalk == nil || c.BookmarksWalk.Done || remaining != 0 || len(c.BookmarksWalk.Seen) != 1 {
		t.Fatalf("the walk spends one request per page and keeps its cursor: %+v", c.BookmarksWalk)
	}
	if len(c.Due) != 0 || strings.Join(c.Bookmarks, ",") != "saved1,saved2" {
		t.Fatal("a partial walk decides no removal")
	}

	// The next run resumes from the saved cursor, reaches the end and only then decides.
	remaining = 3
	if err := r.xBookmarks(context.Background(), bird, "unused", &remaining, c); err != nil {
		t.Fatal(err)
	}
	if c.BookmarksWalk != nil || len(c.Due) != 1 || c.Due["saved2"] == nil || c.Due["saved2"].At.After(time.Now()) {
		t.Fatalf("a completed walk queues what left the list: %+v", c.Due)
	}
	if strings.Join(c.Bookmarks, ",") != "saved1,saved3" {
		t.Fatalf("the completed walk settles exactly what it saw: %v", c.Bookmarks)
	}

	// A page the provider refuses leaves the walk short with its cursor and ids, and does not fail the run.
	os.WriteFile(filepath.Join(root, "mode"), []byte("limited"), 0600)
	c = &xCursor{Bookmarks: []string{"saved1", "saved2"}, BookmarksWalkedAt: time.Now().Add(-25 * time.Hour), BookmarksWalk: &xBookmarkWalk{Cursor: "1", Seen: []string{"saved1"}}, Due: map[string]*xDue{}}
	r.xstate = c
	remaining = 2
	if err := r.xBookmarks(context.Background(), bird, "unused", &remaining, c); err != nil {
		t.Fatalf("a refused page is not a failed run: %v", err)
	}
	if c.BookmarksWalk == nil || c.BookmarksWalk.Done || c.BookmarksWalk.Cursor != "1" || strings.Join(c.BookmarksWalk.Seen, ",") != "saved1" {
		t.Fatalf("a refused page keeps the walk's cursor and ids: %+v", c.BookmarksWalk)
	}
	if len(c.Due) != 0 || strings.Join(c.Bookmarks, ",") != "saved1,saved2" {
		t.Fatal("a refused page decides no removal")
	}
}

// A provider error on the refresh read drops no removal: the id stays settled and the next walk decides it again.
func TestXBookmarkWalkRemovalRefused(t *testing.T) {
	root := t.TempDir()
	bird := filepath.Join(root, "bird")
	writeBirdTestCLI(t, bird)
	os.WriteFile(filepath.Join(root, "mode"), []byte("threadlimited"), 0600)
	writeBookmarkList(t, root, [][]string{{"saved1"}})
	r := seedXCapture(root, bird)
	seedXKnown(t, r, "saved1", "saved2")
	r.state.Cursor = json.RawMessage(`{"backfill":{"list":0,"cursor":"","done":true},"bookmarks":["saved1","saved2"],"bookmarks_walked_at":"` + time.Now().Add(-25*time.Hour).UTC().Format(time.RFC3339) + `"}`)

	if err := runX(context.Background(), r); err != nil {
		t.Fatalf("a refused refresh read is not a failed run: %v", err)
	}
	c := xCursorOf(t, r)
	if strings.Join(c.Bookmarks, ",") != "saved1,saved2" || len(c.Due) != 0 {
		t.Fatalf("a refused refresh read keeps the removal settled: settled=%v due=%+v", c.Bookmarks, c.Due)
	}

	// With the provider answering again, the next walk queues the same removal.
	os.WriteFile(filepath.Join(root, "mode"), []byte("saved"), 0600)
	ageXWalk(t, r, 25*time.Hour)
	if err := runX(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	c = xCursorOf(t, r)
	if strings.Join(c.Bookmarks, ",") != "saved1" || len(c.Due) != 1 || c.Due["saved2"] == nil {
		t.Fatalf("the next walk must queue what left the list: settled=%v due=%+v", c.Bookmarks, c.Due)
	}
}

func TestXBookmarkDigestVersions(t *testing.T) {
	saved := map[string]any{"id": "42", "text": "synthetic", "bookmarked": true}
	live := map[string]any{"id": "42", "text": "synthetic"}
	nested := map[string]any{"id": "42", "text": "synthetic", "_raw": map[string]any{"legacy": map[string]any{"bookmarked": true}}}
	counted := map[string]any{"id": "42", "text": "synthetic", "bookmarked": true, "bookmark_count": 9}
	if xTweetDigest(saved) == xTweetDigest(live) {
		t.Fatal("a save that loses its bookmark must version the post")
	}
	if xTweetDigest(nested) != xTweetDigest(saved) {
		t.Fatal("the provider envelope's viewer flag is the same save")
	}
	if xTweetDigest(counted) != xTweetDigest(saved) {
		t.Fatal("counters never version a post")
	}
}

func writeBookmarkList(t *testing.T, root string, pages [][]string) {
	t.Helper()
	raw, err := json.Marshal(pages)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "list.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func seedXKnown(t *testing.T, r *run, ids ...string) {
	t.Helper()
	for _, id := range ids {
		raw, _ := json.Marshal(map[string]any{"id": id, "text": "synthetic"})
		r.state.Hashes[id] = digest(raw)
	}
}

func xCursorOf(t *testing.T, r *run) xCursor {
	t.Helper()
	var c xCursor
	if json.Unmarshal(r.state.Cursor, &c) != nil {
		t.Fatal("cursor unreadable")
	}
	return c
}

func ageXWalk(t *testing.T, r *run, age time.Duration) {
	t.Helper()
	c := xCursorOf(t, r)
	c.BookmarksWalkedAt = time.Now().Add(-age)
	raw, err := json.Marshal(&c)
	if err != nil {
		t.Fatal(err)
	}
	r.state.Cursor = raw
	if err := r.save(); err != nil {
		t.Fatal(err)
	}
}

func spoolPayloads(t *testing.T, r *run, key string) [][]byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(r.spool, "index.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	var out [][]byte
	for _, line := range bytes.Split(bytes.TrimSpace(raw), []byte("\n")) {
		var entry SpoolEntry
		if json.Unmarshal(line, &entry) != nil {
			t.Fatal("invalid spool entry")
		}
		if entry.Key != key {
			continue
		}
		payload, err := os.ReadFile(filepath.Join(r.spool, entry.Path))
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, payload)
	}
	return out
}
