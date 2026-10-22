package fetch

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

//go:embed request-cap.cjs
var birdPreload []byte

var xLists = [...]string{"bookmarks", "likes", "tweets"}

// Bird output and stderr remain private; the child reports only request counts.
func runX(ctx context.Context, r *run) error {
	binary := r.env["QMD_X_BIRD_BIN"]
	if binary == "" {
		home, _ := os.UserHomeDir()
		binary = filepath.Join(home, ".local/bin/bird-wrapper")
	}
	file, e := os.CreateTemp("", "qmd2-preload-*.cjs")
	if e != nil {
		return errors.New("bird preload creation failed")
	}
	preload := file.Name()
	defer os.Remove(preload)
	if _, e = file.Write(birdPreload); e != nil {
		file.Close()
		return errors.New("bird preload write failed")
	}
	if e = file.Close(); e != nil {
		return errors.New("bird preload close failed")
	}
	if p := r.env["QMD_BIRD_PRELOAD"]; p != "" {
		preload = p
	}
	if _, e := os.Stat(preload); e != nil {
		return errors.New("bird request preload unavailable")
	}
	remaining := 26
	identity, e := r.birdCommand(ctx, binary, preload, &remaining, "--plain", "whoami")
	if e != nil {
		return e
	}
	match := regexp.MustCompile(`(?m)^\s*user_id:\s*([0-9]+)\s*$`).FindSubmatch(identity)
	if len(match) != 2 || digest(match[1]) != r.account {
		return errors.New("bird account assertion failed")
	}
	c, e := r.loadXCursor()
	if e != nil {
		return e
	}
	if e := r.xCommit(c); e != nil {
		return e
	}
	if e := r.xHeads(ctx, binary, preload, &remaining, c); e != nil {
		return e
	}
	return r.xHistory(ctx, binary, preload, &remaining, c)
}

func (r *run) birdCommand(ctx context.Context, binary, preload string, remaining *int, args ...string) ([]byte, error) {
	if *remaining <= 0 {
		r.receipt.RateLimited = true
		return nil, errors.New("bird request allowance exhausted")
	}
	f, e := os.CreateTemp("", "qmd2-bird-*")
	if e != nil {
		return nil, errors.New("bird receipt unavailable")
	}
	path := f.Name()
	f.Close()
	os.Remove(path)
	defer os.Remove(path)
	cmd := exec.CommandContext(ctx, binary, args...)
	cmd.Env = append(os.Environ(), "NODE_OPTIONS=--require="+preload, "XBIRD_RECEIPT="+path, "XBIRD_REMAINING="+strconv.Itoa(*remaining))
	var stderr boundedBuffer
	cmd.Stderr = &stderr
	var b boundedBuffer
	cmd.Stdout = &b
	e = cmd.Run()
	raw, err := os.ReadFile(path)
	var receipt struct {
		Requests    *int `json:"requests_sent"`
		RateLimited bool `json:"rate_limited"`
	}
	if err != nil || json.Unmarshal(raw, &receipt) != nil || receipt.Requests == nil || *receipt.Requests < 0 || *receipt.Requests > *remaining {
		return nil, errors.New("bird request receipt invalid")
	}
	*remaining -= *receipt.Requests
	r.receipt.RequestsSent += *receipt.Requests
	if receipt.RateLimited {
		r.receipt.RateLimited = true
		return nil, errors.New("bird rate limited")
	}
	if e != nil {
		if *remaining == 0 {
			r.receipt.RateLimited = true
			return nil, errors.New("bird request allowance exhausted")
		}
		failure := birdFailure(stderr.String())
		if failure.Error() == "bird provider HTTP 429" || failure.Error() == "bird rate limited" {
			r.receipt.RateLimited = true
		}
		return nil, failure
	}
	return b.Bytes(), nil
}

func (r *run) loadXCursor() (*xCursor, error) {
	var c xCursor
	if len(r.state.Cursor) > 0 {
		var old struct {
			List     int    `json:"list"`
			Cursor   string `json:"cursor"`
			FullPass bool   `json:"full_pass"`
		}
		if json.Unmarshal(r.state.Cursor, &c) != nil || json.Unmarshal(r.state.Cursor, &old) != nil {
			return nil, errors.New("bird cursor invalid")
		}
		if c.Backfill == nil {
			c.Backfill = &xBackfill{List: old.List, Cursor: old.Cursor, Done: old.FullPass}
		}
	}
	if c.Backfill == nil {
		c.Backfill = &xBackfill{}
	}
	if c.Backfill.List < 0 || c.Backfill.List >= len(xLists) {
		return nil, errors.New("bird list cursor invalid")
	}
	return &c, nil
}

func (r *run) xCommit(c *xCursor) error {
	r.receipt.Backfill = c.Backfill
	r.state.Cursor, _ = json.Marshal(c)
	return r.save()
}

func (r *run) xPage(ctx context.Context, binary, preload string, remaining *int, list int, cursor string) (string, bool, error) {
	args := []string{"--plain"}
	if xLists[list] == "tweets" {
		username := r.env["QMD_X_USERNAME"]
		if username == "" {
			return "", false, errors.New("QMD_X_USERNAME required (or --x-username)")
		}
		args = append(args, "user-tweets", username)
	} else {
		args = append(args, xLists[list], "--all")
	}
	args = append(args, "--json-full", "--count", "20", "--max-pages", "1")
	if cursor != "" {
		args = append(args, "--cursor", cursor)
	}
	b, e := r.birdCommand(ctx, binary, preload, remaining, args...)
	if e != nil {
		return "", false, e
	}
	var page struct {
		Tweets []json.RawMessage `json:"tweets"`
		Next   string            `json:"nextCursor"`
	}
	if json.Unmarshal(b, &page) != nil || page.Tweets == nil {
		return "", false, errors.New("bird page invalid")
	}
	writtenBefore := r.receipt.Written
	for _, raw := range page.Tweets {
		if e := r.xTweet(raw); e != nil {
			return "", false, e
		}
	}
	if page.Next != "" && page.Next == cursor {
		return "", false, errors.New("bird repeated cursor")
	}
	return page.Next, r.receipt.Written != writtenBefore, nil
}

func (r *run) xTweet(raw json.RawMessage) error {
	var tweet map[string]any
	var id string
	if json.Unmarshal(raw, &tweet) != nil {
		r.fail()
		return errors.New("bird tweet invalid")
	}
	id, _ = tweet["id"].(string)
	if id == "" {
		r.fail()
		return errors.New("bird tweet identity missing")
	}
	// Counters do not change a post version; raw payload keeps the full provider object.
	canonical := stableTweet(tweet).(map[string]any)
	provider, _ := tweet["_raw"].(map[string]any)
	viewer, _ := provider["legacy"].(map[string]any)
	for _, flag := range []string{"bookmarked", "favorited"} {
		value, present := tweet[flag]
		if !present {
			value = viewer[flag]
		}
		if value == true {
			canonical[flag] = true
		}
	}
	stable, _ := json.Marshal(canonical)
	payload := append(append([]byte(`{"tweet":`), raw...), '}')
	date, _ := tweet["createdAt"].(string)
	head, _ := time.Parse(time.RFC3339Nano, date)
	if head.IsZero() {
		head, _ = time.Parse(time.RubyDate, date)
	}
	if e := r.emitHash(id, payload, ObservationState{Version: "x-post-v1", Labels: map[string]string{"origin": "bird"}}, head, digest(stable)); e != nil {
		return e
	}
	return nil
}

func (r *run) xHeads(ctx context.Context, binary, preload string, remaining *int, c *xCursor) error {
	// Heads restart every run so history cannot postpone discovery.
	for list := range xLists {
		cursor := ""
		for {
			next, changed, err := r.xPage(ctx, binary, preload, remaining, list, cursor)
			if err != nil {
				return err
			}
			if err = r.xCommit(c); err != nil {
				return err
			}
			if next == "" || !changed {
				break
			}
			cursor = next
		}
	}
	return nil
}

func (r *run) xHistory(ctx context.Context, binary, preload string, remaining *int, c *xCursor) error {
	r.receipt.HeadComplete = true
	for !c.Backfill.Done && *remaining > 0 {
		failedBefore := r.receipt.Failed
		next, _, err := r.xPage(ctx, binary, preload, remaining, c.Backfill.List, c.Backfill.Cursor)
		if err != nil {
			r.receipt.Enumerated -= r.receipt.Failed - failedBefore
			r.receipt.Failed = failedBefore
			r.receipt.BackfillError = err.Error()
			break
		}
		c.Backfill.Cursor = next
		if next == "" {
			c.Backfill.List++
			if c.Backfill.List == len(xLists) {
				c.Backfill.List = 0
				c.Backfill.Done = true
			}
		}
		if err = r.xCommit(c); err != nil {
			return err
		}
	}
	return r.xCommit(c)
}

type xBackfill struct {
	List   int    `json:"list"`
	Cursor string `json:"cursor"`
	Done   bool   `json:"done"`
}
type xCursor struct {
	Backfill *xBackfill `json:"backfill"`
}

// Post versions use normalized content; provider envelopes stay in archived payloads.
func stableTweet(v any) any {
	switch x := v.(type) {
	case map[string]any:
		out := map[string]any{}
		for k, v := range x {
			lower := strings.ToLower(k)
			if strings.HasSuffix(lower, "_count") || strings.HasSuffix(k, "Count") {
				continue
			}
			switch lower {
			case "_raw", "count", "views", "favorited", "retweeted", "bookmarked":
				continue
			}

			out[k] = stableTweet(v)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, v := range x {
			out[i] = stableTweet(v)
		}
		return out
	default:
		return v
	}
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > 64<<20 {
		return 0, fmt.Errorf("subprocess output exceeds bound")
	}
	return b.Buffer.Write(p)
}

// Only fixed classifications and numeric HTTP status may leave private stderr.
func birdFailure(stderr string) error {
	lower := strings.ToLower(stderr)
	if match := regexp.MustCompile(`(?i)(?:http(?: error| status)?|status(?:code)?|status code)[^0-9]{0,12}([45][0-9]{2})`).FindStringSubmatch(stderr); len(match) == 2 {
		return fmt.Errorf("bird provider HTTP %s", match[1])
	}
	for _, row := range []struct{ needle, reason string }{{"unknown option", "bird unsupported option"}, {"rate limit", "bird rate limited"}, {"unauthorized", "bird credential refused"}, {"forbidden", "bird provider forbidden"}, {"query id", "bird query id unavailable"}, {"not found", "bird provider endpoint unavailable"}, {"fetch failed", "bird transport failed"}} {
		if strings.Contains(lower, row.needle) {
			return errors.New(row.reason)
		}
	}
	return errors.New("bird subprocess failed")
}
