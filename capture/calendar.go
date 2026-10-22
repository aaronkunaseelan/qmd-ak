package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"
)

var errCalendarGone = errors.New("calendar: HTTP 410")

type calendarCursor struct {
	V      string            `json:"v"`
	At     string            `json:"at,omitempty"`
	Cals   []string          `json:"cals,omitempty"`
	Tokens map[string]string `json:"tokens,omitempty"`
	Page   string            `json:"page,omitempty"`
}

// Calendar uses read-only list and incremental sync requests.
func runCalendar(ctx context.Context, r *run) error {
	token, err := r.googleToken(ctx)
	if err != nil {
		return err
	}
	body, err := r.calendarGet(ctx, token, "/gmail/v1/users/me/profile")
	if err != nil {
		return err
	}
	var profile struct {
		Email string `json:"emailAddress"`
	}
	if json.Unmarshal(body, &profile) != nil || profile.Email != r.account {
		return fmt.Errorf("calendar: account assertion failed")
	}
	c := calendarCursor{V: "calendar-sync-v1", Tokens: map[string]string{}}
	if len(r.state.Cursor) > 0 && (json.Unmarshal(r.state.Cursor, &c) != nil || c.V != "calendar-sync-v1") {
		return fmt.Errorf("calendar: invalid cursor")
	}
	if c.Tokens == nil {
		c.Tokens = map[string]string{}
	}
	if c.At == "" {
		c.At = "@calendar-list"
		c.Cals = nil
		c.Page = ""
	}
	for {
		if c.At == "@calendar-list" {
			if err = r.calendarList(ctx, token, &c); err != nil {
				return err
			}
		} else {
			if err = r.calendarEvents(ctx, token, &c); err != nil {
				return err
			}
		}
		r.state.Cursor, _ = json.Marshal(c)
		if err = r.save(); err != nil {
			return err
		}
		if c.At == "" {
			r.receipt.HeadComplete = true
			return nil
		}
	}
}

func (r *run) calendarGet(ctx context.Context, token, path string) ([]byte, error) {
	r.receipt.RequestsSent++
	body, status, err := r.request(ctx, "GET", "https://www.googleapis.com"+path, map[string]string{"Authorization": "Bearer " + token, "Accept": "application/json"}, nil)
	if status == 410 {
		return nil, errCalendarGone
	}
	if err != nil {
		return nil, err
	}
	if status != 200 {
		return nil, fmt.Errorf("calendar: HTTP %d", status)
	}
	return body, nil
}

func (r *run) calendarList(ctx context.Context, token string, c *calendarCursor) error {
	q := url.Values{"maxResults": {"250"}}
	if c.Page != "" {
		q.Set("pageToken", c.Page)
	}
	body, err := r.calendarGet(ctx, token, "/calendar/v3/users/me/calendarList?"+q.Encode())
	if err != nil {
		return err
	}
	var p struct {
		Items []struct {
			ID string `json:"id"`
		} `json:"items"`
		Next string `json:"nextPageToken"`
	}
	if json.Unmarshal(body, &p) != nil {
		return fmt.Errorf("calendar: invalid calendar list")
	}
	for _, item := range p.Items {
		if item.ID == "" {
			return fmt.Errorf("calendar: calendar has no id")
		}
		exists := false
		for _, id := range c.Cals {
			if id == item.ID {
				exists = true
			}
		}
		if !exists {
			c.Cals = append(c.Cals, item.ID)
		}
	}
	if p.Next != "" && p.Next == c.Page {
		return fmt.Errorf("calendar: repeated list cursor")
	}
	c.Page = p.Next
	if c.Page == "" {
		c.At = ""
		if len(c.Cals) > 0 {
			c.At = c.Cals[0]
		}
	}
	return nil
}

func (r *run) calendarEvents(ctx context.Context, token string, c *calendarCursor) error {
	q := url.Values{"maxResults": {"250"}, "showDeleted": {"true"}}
	if c.Tokens[c.At] != "" {
		q.Set("syncToken", c.Tokens[c.At])
	}
	if c.Page != "" {
		q.Set("pageToken", c.Page)
	}
	body, err := r.calendarGet(ctx, token, "/calendar/v3/calendars/"+url.PathEscape(c.At)+"/events?"+q.Encode())
	if errors.Is(err, errCalendarGone) && c.Tokens[c.At] != "" {
		delete(c.Tokens, c.At)
		c.Page = ""
		return nil // The outer loop saves the reset before starting the full listing.
	}
	if err != nil {
		return err
	}
	var p struct {
		Items []json.RawMessage `json:"items"`
		Next  string            `json:"nextPageToken"`
		Sync  string            `json:"nextSyncToken"`
	}
	if json.Unmarshal(body, &p) != nil {
		return fmt.Errorf("calendar: invalid events page")
	}
	for _, raw := range p.Items {
		var event struct {
			ID      string `json:"id"`
			Updated string `json:"updated"`
			Status  string `json:"status"`
		}
		if json.Unmarshal(raw, &event) != nil || event.ID == "" {
			r.receipt.Enumerated++
			r.receipt.Failed++
			return fmt.Errorf("calendar: event has no id")
		}
		head, _ := time.Parse(time.RFC3339Nano, event.Updated)
		state := "event"
		if event.Status == "cancelled" {
			state = "deleted"
		}
		if err = r.emit(c.At+":"+event.ID, raw, ObservationState{Version: "calendar-event-v1", Labels: map[string]string{"calendar_id": c.At, "state": state}}, head); err != nil {
			return err
		}
	}
	if p.Next != "" && p.Next == c.Page {
		return fmt.Errorf("calendar: repeated events cursor")
	}
	c.Page = p.Next
	if c.Page == "" {
		if p.Sync == "" {
			return fmt.Errorf("calendar: missing sync token")
		}
		c.Tokens[c.At] = p.Sync
		next := ""
		for i, id := range c.Cals {
			if id == c.At && i+1 < len(c.Cals) {
				next = c.Cals[i+1]
			}
		}
		c.At = next
	}
	return nil
}
