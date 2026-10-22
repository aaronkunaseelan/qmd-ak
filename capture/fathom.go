package fetch

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"
)

// Space every wire request, including retries, to at most 20 requests/minute.
type fathomTransport struct {
	base     http.RoundTripper
	next     time.Time
	interval time.Duration
	receipt  *Receipt
}

func (t *fathomTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if delay := time.Until(t.next); delay > 0 {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-req.Context().Done():
			return nil, req.Context().Err()
		case <-timer.C:
		}
	}
	t.next = time.Now().Add(t.interval)
	t.receipt.RequestsSent++
	return t.base.RoundTrip(req)
}

type fathomCursor struct {
	V            int       `json:"v"`
	Page         string    `json:"page,omitempty"`
	Started      time.Time `json:"started"`
	Since        time.Time `json:"since"`
	LastFull     time.Time `json:"last_full"`
	LastContent  time.Time `json:"last_content"`
	LastComplete []string  `json:"last_complete"`
	Seen         []string  `json:"seen,omitempty"`
	Full         bool      `json:"full"`
	Active       bool      `json:"active"`
}

func runFathom(ctx context.Context, r *run) error {
	key := r.env["QMD_FATHOM_API_KEY"]
	if key == "" {
		return fmt.Errorf("fathom: missing API key")
	}
	client := *r.client
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = &fathomTransport{base: base, interval: 3 * time.Second, receipt: &r.receipt}
	original := r.client
	r.client = &client
	defer func() { r.client = original }()
	if err := r.fathomIdentity(ctx, key); err != nil {
		return err
	}
	c := fathomCursor{V: 2}
	if len(r.state.Cursor) > 0 && (json.Unmarshal(r.state.Cursor, &c) != nil || c.V != 2) {
		return fmt.Errorf("fathom: unsupported cursor")
	}
	if !c.Active {
		now := time.Now().UTC()
		c.Active = true
		c.Page = ""
		c.Seen = nil
		c.Started = now
		c.Since = c.LastContent.Add(-2 * time.Minute)
		if settle := now.Add(-30 * time.Minute); settle.Before(c.Since) {
			c.Since = settle
		}
		c.Full = c.LastFull.IsZero() || now.Sub(c.LastFull) >= 36*time.Minute
	}
	for {
		if err := r.fathomPage(ctx, key, &c); err != nil {
			return err
		}
		r.state.Cursor, _ = json.Marshal(c)
		if err := r.save(); err != nil {
			return err
		}
		if !c.Active {
			r.receipt.HeadComplete = true
			return nil
		}
	}
}

func (r *run) fathomGet(ctx context.Context, key, path string, q url.Values) ([]byte, error) {
	body, status, err := r.request(ctx, "GET", "https://api.fathom.ai/external/v1"+path+"?"+q.Encode(), map[string]string{"X-Api-Key": key}, nil)
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, fmt.Errorf("fathom: HTTP %d", status)
	}
	return body, nil
}

func (r *run) fathomIdentity(ctx context.Context, key string) error {
	body, err := r.fathomGet(ctx, key, "/team_members", nil)
	if err != nil {
		return err
	}
	var members struct {
		Items []struct {
			Email string `json:"email"`
		} `json:"items"`
	}
	if json.Unmarshal(body, &members) != nil {
		return fmt.Errorf("fathom: invalid identity response")
	}
	asserted := false
	for _, m := range members.Items {
		if m.Email == r.account {
			asserted = true
		}
	}
	if !asserted {
		return fmt.Errorf("fathom: account not asserted by team membership")
	}
	return nil
}

func (r *run) fathomPage(ctx context.Context, key string, c *fathomCursor) error {
	q := url.Values{"include_transcript": {"true"}, "include_summary": {"true"}, "include_action_items": {"true"}, "include_crm_matches": {"true"}, "include_highlights": {"true"}}
	if c.Page != "" {
		q.Set("cursor", c.Page)
	}
	if !c.Full {
		q.Set("created_after", c.Since.Format(time.RFC3339))
	}
	body, err := r.fathomGet(ctx, key, "/meetings", q)
	if err != nil {
		return err
	}
	var page struct {
		Items []json.RawMessage `json:"items"`
		Next  string            `json:"next_cursor"`
	}
	if json.Unmarshal(body, &page) != nil || page.Items == nil {
		return fmt.Errorf("fathom: invalid meetings page")
	}
	known, err := r.fathomItems(page.Items, c)
	if err != nil {
		return err
	}
	if page.Next != "" && page.Next == c.Page {
		return fmt.Errorf("fathom: repeated page cursor")
	}
	c.Page = page.Next
	if c.Page == "" {
		if c.Full {
			for _, id := range c.LastComplete {
				if !known[id] {
					raw, _ := json.Marshal(map[string]string{"recording_id": id, "state": "deleted"})
					if err = r.emit(id, raw, ObservationState{Version: "fathom-meeting-v1", Labels: map[string]string{"state": "deleted"}}, time.Time{}); err != nil {
						return err
					}
				}
			}
			sort.Strings(c.Seen)
			c.LastComplete = c.Seen
			c.LastFull = c.Started
		}
		c.LastContent = c.Started
		c.Active = false
		c.Seen = nil
	}
	return nil
}

func (r *run) fathomItems(items []json.RawMessage, c *fathomCursor) (map[string]bool, error) {
	known := map[string]bool{}
	for _, id := range c.Seen {
		known[id] = true
	}
	for _, raw := range items {
		var m struct {
			ID      json.Number `json:"recording_id"`
			Created string      `json:"created_at"`
			Start   string      `json:"recording_start_time"`
		}
		if json.Unmarshal(raw, &m) != nil || m.ID.String() == "" || known[m.ID.String()] {
			r.receipt.Enumerated++
			r.receipt.Failed++
			return nil, fmt.Errorf("fathom: missing or duplicate recording id")
		}
		head, _ := time.Parse(time.RFC3339Nano, m.Created)
		if head.IsZero() {
			head, _ = time.Parse(time.RFC3339Nano, m.Start)
		}
		if err := r.emit(m.ID.String(), raw, ObservationState{Version: "fathom-meeting-v1", Labels: map[string]string{"state": "meeting"}}, head); err != nil {
			return nil, err
		}
		known[m.ID.String()] = true
		c.Seen = append(c.Seen, m.ID.String())
	}
	return known, nil
}
