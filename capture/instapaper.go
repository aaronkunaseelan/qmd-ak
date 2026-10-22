package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ipPending struct {
	Bookmark       json.RawMessage `json:"bookmark"`
	Folder         string          `json:"folder"`
	Hash           string          `json:"hash"`
	Tries          int             `json:"tries"`
	FailedAt       time.Time       `json:"failed_at"`
	Reason         string          `json:"reason,omitempty"`
	FirstFailedAt  time.Time       `json:"first_failed_at"`
	RetryAt        time.Time       `json:"retry_at,omitempty"`
	ExcludedReason string          `json:"excluded_reason,omitempty"`
}
type ipCursor struct {
	Folder  int                          `json:"folder"`
	Have    map[string]map[string]string `json:"have"`
	Pending map[string]ipPending         `json:"pending"`
}

func permanentTextFailure(status int, html []byte) bool {
	return status == 200 && len(strings.TrimSpace(string(html))) == 0 || status >= 400 && status < 500 && status != 429
}

// Failed text ids retry each run; text-unavailable ids retry daily without failing receipts.
func runInstapaper(ctx context.Context, r *run) error {
	r.client.Timeout = 30 * time.Second
	base := "https://www.instapaper.com"
	if configured := r.env["QMD_INSTAPAPER_BASE_URL"]; configured != "" {
		u, e := url.Parse(configured)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("Instapaper API endpoint invalid")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && (u.Scheme != "http" || ip == nil || !ip.IsLoopback()) {
			return errors.New("Instapaper API endpoint requires HTTPS")
		}
		base = strings.TrimRight(configured, "/")
	}
	o := ipOptions{r.env["QMD_INSTAPAPER_CONSUMER_KEY"], r.env["QMD_INSTAPAPER_CONSUMER_SECRET"], r.env["QMD_INSTAPAPER_ACCESS_TOKEN"], r.env["QMD_INSTAPAPER_ACCESS_TOKEN_SECRET"]}
	if o.ConsumerKey == "" || o.ConsumerSecret == "" || o.AccessToken == "" || o.AccessTokenSecret == "" {
		return errors.New("Instapaper credentials missing")
	}
	if e := r.ipIdentity(ctx, base, o); e != nil {
		return e
	}
	c := ipCursor{Have: map[string]map[string]string{}, Pending: map[string]ipPending{}}
	if len(r.state.Cursor) > 0 && (json.Unmarshal(r.state.Cursor, &c) != nil || c.Have == nil || c.Folder < 0 || c.Folder > 2) {
		return errors.New("Instapaper cursor invalid")
	}
	if c.Pending == nil {
		c.Pending = map[string]ipPending{}
	}
	attempted := map[string]bool{}
	succeeded := 0
	pendingIDs := make([]string, 0, len(c.Pending))
	for id := range c.Pending {
		pendingIDs = append(pendingIDs, id)
	}
	sort.Strings(pendingIDs)
	for _, id := range pendingIDs {
		if e := r.ipCaptureText(ctx, base, o, &c, attempted, &succeeded, id, c.Pending[id]); e != nil {
			ipSave(r, &c)
			return e
		}
		if e := ipSave(r, &c); e != nil {
			return e
		}
	}
	if r.pendingOnly {
		r.receipt.HasMore = true
		return r.ipFinish(&c, succeeded)
	}
	return r.ipTraverse(ctx, base, o, &c, attempted, &succeeded)
}

func (r *run) ipPost(ctx context.Context, base string, o ipOptions, path string, form url.Values) ([]byte, int, error) {
	endpoint := base + path
	nonce, e := oauthNonce()
	if e != nil {
		return nil, 0, errors.New("Instapaper signing failed")
	}
	auth, e := oauthHeader(o, "POST", endpoint, form, nonce, strconv.FormatInt(time.Now().Unix(), 10))
	if e != nil {
		return nil, 0, errors.New("Instapaper signing failed")
	}
	return r.request(ctx, "POST", endpoint, map[string]string{"Authorization": auth, "Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
}

func (r *run) ipIdentity(ctx context.Context, base string, o ipOptions) error {
	identity, _, e := r.ipPost(ctx, base, o, "/api/1/account/verify_credentials", nil)
	if e != nil {
		return e
	}
	var users []struct {
		Type     string      `json:"type"`
		ID       json.Number `json:"user_id"`
		Username string      `json:"username"`
	}
	if json.Unmarshal(identity, &users) != nil || len(users) != 1 || users[0].Type != "user" || users[0].ID.String() == "" {
		return errors.New("Instapaper account assertion invalid")
	}
	if r.account == "primary" {
		r.account = users[0].ID.String()
	}
	if r.account != users[0].ID.String() && r.account != users[0].Username {
		return errors.New("Instapaper account mismatch")
	}
	return nil
}

func ipSave(r *run, c *ipCursor) error {
	r.state.Cursor, _ = json.Marshal(c)
	return r.save()
}

func (r *run) ipWrite(c *ipCursor, succeeded *int, id string, p ipPending, html []byte, head time.Time, reason string) error {
	payload, _ := json.Marshal(struct {
		Bookmark        json.RawMessage `json:"bookmark"`
		HTML            string          `json:"html"`
		TextUnavailable bool            `json:"text_unavailable,omitempty"`
		Reason          string          `json:"reason,omitempty"`
	}{p.Bookmark, string(html), reason != "", reason})
	labels := map[string]string{"folder": p.Folder}
	if reason != "" {
		labels["reason"] = reason
	}
	beforeWritten, beforeUnchanged := r.receipt.Written, r.receipt.Unchanged
	if e := r.emit(id, payload, ObservationState{Version: "instapaper-snapshot-v1", Labels: labels}, head); e != nil {
		return e
	}
	if reason != "" {
		r.receipt.Written, r.receipt.Unchanged = beforeWritten, beforeUnchanged
		r.receipt.Excluded++
		r.receipt.ExclusionReasons[reason]++
	} else {
		delete(c.Pending, id)
		(*succeeded)++
	}
	if c.Have[p.Folder] == nil {
		c.Have[p.Folder] = map[string]string{}
	}
	c.Have[p.Folder][id] = p.Hash
	return nil
}

func (r *run) ipFinish(c *ipCursor, succeeded int) error {
	remaining := r.receipt.FailedIDs[:0]
	r.receipt.OldestFailed = time.Time{}
	for _, id := range r.receipt.FailedIDs {
		p := c.Pending[id]
		transient := strings.HasPrefix(p.Reason, "http_5") || p.Reason == "timeout/network"
		if succeeded > 0 && transient && p.Tries >= 10 && time.Since(p.FirstFailedAt) >= 6*time.Hour {
			p.ExcludedReason = "provider_persistent_" + strings.ReplaceAll(p.Reason, "/", "_")
			p.RetryAt = time.Now().UTC().Add(24 * time.Hour)
			var metadata struct {
				Time json.Number `json:"time"`
			}
			json.Unmarshal(p.Bookmark, &metadata)
			secs, _ := strconv.ParseFloat(metadata.Time.String(), 64)
			if e := r.ipWrite(c, &succeeded, id, p, nil, time.Unix(int64(secs), 0), p.ExcludedReason); e != nil {
				return e
			}
			r.receipt.Enumerated--
			r.receipt.Failed--
			c.Pending[id] = p
		} else {
			remaining = append(remaining, id)
			if r.receipt.OldestFailed.IsZero() || p.FirstFailedAt.Before(r.receipt.OldestFailed) {
				r.receipt.OldestFailed = p.FirstFailedAt
			}
		}
	}
	r.receipt.FailedIDs = remaining
	return ipSave(r, c)
}

func (r *run) ipCaptureText(ctx context.Context, base string, o ipOptions, c *ipCursor, attempted map[string]bool, succeeded *int, id string, p ipPending) error {
	attempted[id] = true
	var metadata struct {
		Time json.Number `json:"time"`
	}
	json.Unmarshal(p.Bookmark, &metadata)
	secs, _ := strconv.ParseFloat(metadata.Time.String(), 64)
	head := time.Unix(int64(secs), 0)
	if p.ExcludedReason != "" && time.Now().Before(p.RetryAt) {
		r.observe(head)
		r.receipt.Enumerated++
		r.receipt.Excluded++
		r.receipt.ExclusionReasons[p.ExcludedReason]++
		return nil
	}
	html, status, err := r.ipPost(ctx, base, o, "/api/1/bookmarks/get_text", url.Values{"bookmark_id": {id}})
	unavailable := err != nil || len(strings.TrimSpace(string(html))) == 0
	if unavailable {
		if r.ipUnavailable(c, id, &p, html, status, err) {
			return nil
		}
		html = nil
	}
	reason := ""
	if unavailable {
		reason = "text_unavailable"
		p.ExcludedReason = reason
		p.RetryAt = time.Now().UTC().Add(24 * time.Hour)
		c.Pending[id] = p
	}
	return r.ipWrite(c, succeeded, id, p, html, head, reason)
}

func (r *run) ipUnavailable(c *ipCursor, id string, p *ipPending, html []byte, status int, err error) bool {
	p.Reason = "timeout/network"
	if status >= 300 {
		p.Reason = "http_" + strconv.Itoa(status)
	} else if err == nil {
		p.Reason = "no_text"
	}
	if r.receipt.FailureReasons == nil {
		r.receipt.FailureReasons = map[string]int{}
	}
	r.receipt.FailureReasons[p.Reason]++
	p.Tries++
	if p.FailedAt.IsZero() {
		p.FailedAt = time.Now().UTC()
	}
	if p.FirstFailedAt.IsZero() {
		p.FirstFailedAt = p.FailedAt
	}
	if p.ExcludedReason != "" {
		p.RetryAt = time.Now().UTC().Add(24 * time.Hour)
	}
	c.Pending[id] = *p
	if p.ExcludedReason != "" {
		r.receipt.Enumerated++
		r.receipt.Excluded++
		r.receipt.ExclusionReasons[p.ExcludedReason]++
		return true
	}
	if !permanentTextFailure(status, html) || (status == 200 && err != nil) {
		r.fail()
		r.receipt.FailedIDs = append(r.receipt.FailedIDs, id)
		if r.receipt.OldestFailed.IsZero() || p.FailedAt.Before(r.receipt.OldestFailed) {
			r.receipt.OldestFailed = p.FailedAt
		}
		return true
	}
	return false
}

func ipKnown(c *ipCursor, folder string) (map[string]bool, []string) {
	known := map[string]bool{}
	for id := range c.Have[folder] {
		known[id] = true
	}
	for id, p := range c.Pending {
		if p.Folder == folder {
			known[id] = true
		}
	}
	ids := make([]string, 0, len(known))
	for id := range known {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return known, ids
}

func (r *run) ipTraverse(ctx context.Context, base string, o ipOptions, c *ipCursor, attempted map[string]bool, succeeded *int) error {
	folders := []string{"unread", "starred", "archive"}
	for {
		folder := folders[c.Folder]
		if c.Have[folder] == nil {
			c.Have[folder] = map[string]string{}
		}
		known, ids := ipKnown(c, folder)
		b, _, e := r.ipPost(ctx, base, o, "/api/1/bookmarks/list", url.Values{"folder_id": {folder}, "limit": {"100"}, "have": {strings.Join(ids, ",")}})
		if e != nil {
			return e
		}
		var records []json.RawMessage
		if json.Unmarshal(b, &records) != nil {
			return errors.New("Instapaper list invalid")
		}
		bookmarks, progress, e := r.ipRecords(ctx, base, o, c, attempted, succeeded, folder, known, records)
		if e != nil {
			return e
		}
		if bookmarks == 0 {
			c.Folder++
		} else if !progress {
			return errors.New("Instapaper traversal made no progress")
		}
		complete := c.Folder == len(folders)
		if complete {
			c.Folder = 0
		}
		if e = ipSave(r, c); e != nil {
			return e
		}
		if complete {
			r.receipt.HeadComplete = !r.receipt.RateLimited
			return r.ipFinish(c, *succeeded)
		}
	}
}

func (r *run) ipRecords(ctx context.Context, base string, o ipOptions, c *ipCursor, attempted map[string]bool, succeeded *int, folder string, known map[string]bool, records []json.RawMessage) (int, bool, error) {
	var e error
	bookmarks, progress := 0, false
	for _, raw := range records {
		var item struct {
			Type      string      `json:"type"`
			ID        json.Number `json:"bookmark_id"`
			Hash      string      `json:"hash"`
			DeleteIDs string      `json:"delete_ids"`
		}
		if json.Unmarshal(raw, &item) != nil {
			return 0, false, errors.New("Instapaper record invalid")
		}
		switch item.Type {
		case "user":
		case "meta":
			if e := r.ipDelete(c, item.DeleteIDs); e != nil {
				return 0, false, e
			}
		case "bookmark":
			bookmarks++
			id := item.ID.String()
			if id == "" || item.Hash == "" {
				r.fail()
				continue
			}
			if prior, ok := c.Have[folder][id]; ok && prior == item.Hash || attempted[id] {
				if !known[id] {
					progress = true
				}
				c.Have[folder][id] = item.Hash
				r.receipt.Enumerated++
				r.receipt.Unchanged++
				continue
			}
			progress = true
			p := ipPending{Bookmark: raw, Folder: folder, Hash: item.Hash}
			if prior, ok := c.Pending[id]; ok {
				p = prior
				p.Bookmark, p.Folder, p.Hash = raw, folder, item.Hash
			}
			if e = r.ipCaptureText(ctx, base, o, c, attempted, succeeded, id, p); e != nil {
				ipSave(r, c)
				return 0, false, e
			}
			c.Have[folder][id] = item.Hash
			if e = ipSave(r, c); e != nil {
				return 0, false, e
			}
		default:
			return 0, false, errors.New("Instapaper unknown or error record")
		}
	}
	return bookmarks, progress, nil
}

func (r *run) ipDelete(c *ipCursor, deleteIDs string) error {
	var e error
	for _, id := range strings.Split(deleteIDs, ",") {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		payload, _ := json.Marshal(map[string]string{"bookmark_id": id, "state": "deleted"})
		if e = r.emit(id, payload, ObservationState{Version: "instapaper-snapshot-v1", Labels: map[string]string{"state": "deleted"}}, time.Time{}); e != nil {
			return e
		}
		delete(c.Pending, id)
	}
	return nil
}
