package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Google tokens are read and refreshed in process; existing token files stay read-only.
func (r *run) googleToken(ctx context.Context) (string, error) {
	home, _ := os.UserHomeDir()
	files, e := filepath.Glob(filepath.Join(home, ".config/google-api/tokens-*.json"))
	if e != nil {
		return "", errors.New("Google credentials unavailable")
	}
	for _, path := range files {
		b, e := os.ReadFile(path)
		if e != nil {
			continue
		}
		var t struct {
			Account      string `json:"account"`
			AccessToken  string `json:"access_token"`
			RefreshToken string `json:"refresh_token"`
			ClientID     string `json:"client_id"`
			ClientSecret string `json:"client_secret"`
			ExpiryDate   int64  `json:"expiry_date"`
		}
		if json.Unmarshal(b, &t) != nil {
			continue
		}
		profile := filepath.Base(path)
		if r.account != t.Account && profile != "tokens-"+r.account+".json" {
			continue
		}
		r.account = t.Account
		if t.AccessToken != "" && time.UnixMilli(t.ExpiryDate).After(time.Now().Add(5*time.Minute)) {
			return t.AccessToken, nil
		}
		if t.RefreshToken == "" || t.ClientID == "" || t.ClientSecret == "" {
			return "", errors.New("Google token refresh unavailable")
		}
		form := url.Values{"client_id": {t.ClientID}, "client_secret": {t.ClientSecret}, "refresh_token": {t.RefreshToken}, "grant_type": {"refresh_token"}}
		raw, _, e := r.request(ctx, "POST", "https://oauth2.googleapis.com/token", map[string]string{"Content-Type": "application/x-www-form-urlencoded"}, []byte(form.Encode()))
		if e != nil {
			return "", errors.New("Google token refresh refused")
		}
		var grant struct {
			AccessToken string `json:"access_token"`
		}
		if json.Unmarshal(raw, &grant) != nil || grant.AccessToken == "" {
			return "", errors.New("Google token response invalid")
		}
		return grant.AccessToken, nil
	}
	return "", errors.New("Google account credentials unavailable")
}

type gmailCursor struct {
	History string `json:"history,omitempty"`
	Full    bool   `json:"full,omitempty"`
	Page    string `json:"page,omitempty"`
	Anchor  string `json:"anchor,omitempty"`
}

func runGmail(ctx context.Context, r *run) error {
	token, e := r.googleToken(ctx)
	if e != nil {
		return e
	}
	headers := map[string]string{"Authorization": "Bearer " + token}
	api := "https://gmail.googleapis.com"
	if configured := r.env["QMD_GMAIL_BASE_URL"]; configured != "" {
		u, e := url.Parse(configured)
		if e != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return errors.New("Gmail API endpoint invalid")
		}
		ip := net.ParseIP(u.Hostname())
		if u.Scheme != "https" && (u.Scheme != "http" || ip == nil || !ip.IsLoopback()) {
			return errors.New("Gmail API endpoint requires HTTPS")
		}
		api = strings.TrimRight(configured, "/")
	}
	base := api + "/gmail/v1/users/me"
	b, _, e := r.gmailGet(ctx, base, headers, "/profile")
	if e != nil {
		return e
	}
	var profile struct {
		Email   string `json:"emailAddress"`
		History string `json:"historyId"`
	}
	if json.Unmarshal(b, &profile) != nil || profile.Email != r.account || profile.History == "" {
		return errors.New("Gmail account assertion failed")
	}
	c := gmailCursor{}
	if len(r.state.Cursor) > 0 && json.Unmarshal(r.state.Cursor, &c) != nil {
		return errors.New("Gmail cursor invalid")
	}
	if e := r.gmailAnchor(ctx, base, headers, &c, profile.History); e != nil {
		return e
	}
	return r.gmailCapture(ctx, base, headers, &c, profile.History)
}

func (r *run) gmailGet(ctx context.Context, base string, headers map[string]string, path string) ([]byte, int, error) {
	return r.request(ctx, "GET", base+path, headers, nil)
}

func gmailSave(r *run, c *gmailCursor) error {
	r.state.Cursor, _ = json.Marshal(c)
	return r.save()
}

func (r *run) gmailMessage(ctx context.Context, base string, headers map[string]string, id string) error {
	raw, status, err := r.gmailGet(ctx, base, headers, "/messages/"+url.PathEscape(id)+"?format=raw")
	if status == 404 {
		r.receipt.Enumerated++
		r.receipt.Excluded++
		r.receipt.ExclusionReasons["provider_deleted"]++
		return nil
	}
	if err != nil {
		r.fail()
		return err
	}
	var m struct {
		ID           string `json:"id"`
		Raw          string `json:"raw"`
		InternalDate string `json:"internalDate"`
	}
	if json.Unmarshal(raw, &m) != nil || m.ID != id || m.Raw == "" {
		r.fail()
		return errors.New("Gmail envelope invalid")
	}
	mime, err := base64.RawURLEncoding.DecodeString(m.Raw)
	if err != nil {
		mime, err = base64.URLEncoding.DecodeString(m.Raw)
	}
	if err != nil || len(mime) == 0 {
		r.fail()
		return errors.New("Gmail MIME decode failed")
	}
	ms, _ := strconv.ParseInt(m.InternalDate, 10, 64)
	return r.emit(id, raw, ObservationState{Version: "gmail-history-message-v1", Labels: map[string]string{"state": "message"}}, time.UnixMilli(ms))
}

func (r *run) gmailAnchor(ctx context.Context, base string, headers map[string]string, c *gmailCursor, profileHistory string) error {
	// A new cursor captures the newest message before anchoring live history.
	if c.History == "" && !c.Full {
		raw, _, err := r.gmailGet(ctx, base, headers, "/messages?maxResults=1")
		if err != nil {
			return err
		}
		var page struct {
			Messages []struct {
				ID string `json:"id"`
			} `json:"messages"`
		}
		if json.Unmarshal(raw, &page) != nil {
			return errors.New("Gmail listing invalid")
		}
		for _, m := range page.Messages {
			if err = r.gmailMessage(ctx, base, headers, m.ID); err != nil {
				return err
			}
		}
		c.History = profileHistory
		if err = gmailSave(r, c); err != nil {
			return err
		}
	}
	return nil
}

func (r *run) gmailCapture(ctx context.Context, base string, headers map[string]string, c *gmailCursor, profileHistory string) error {
	for {
		if c.Full {
			if err := r.gmailFull(ctx, base, headers, c, profileHistory); err != nil {
				return err
			}
			if c.Full {
				continue
			}
		}
		done, err := r.gmailDelta(ctx, base, headers, c, profileHistory)
		if err != nil || done {
			return err
		}
	}
}

func (r *run) gmailFull(ctx context.Context, base string, headers map[string]string, c *gmailCursor, profileHistory string) error {
	if c.Anchor == "" {
		c.Anchor = profileHistory
	}
	raw, _, err := r.gmailGet(ctx, base, headers, "/messages?maxResults=500&includeSpamTrash=true&pageToken="+url.QueryEscape(c.Page))
	if err != nil {
		return err
	}
	var page struct {
		Messages []struct {
			ID string `json:"id"`
		} `json:"messages"`
		Next string `json:"nextPageToken"`
	}
	if json.Unmarshal(raw, &page) != nil {
		return errors.New("Gmail listing invalid")
	}
	for _, m := range page.Messages {
		if err = r.gmailMessage(ctx, base, headers, m.ID); err != nil {
			return err
		}
	}
	c.Page = page.Next
	if c.Page == "" {
		c.Full = false
		c.History = c.Anchor
		c.Anchor = ""
	}
	if err = gmailSave(r, c); err != nil {
		return err
	}
	return nil
}

type gmailHistoryPage struct {
	History []struct {
		ID    string `json:"id"`
		Added []struct {
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
		} `json:"messagesAdded"`
		Deleted []struct {
			Message struct {
				ID string `json:"id"`
			} `json:"message"`
		} `json:"messagesDeleted"`
	} `json:"history"`
	Next      string `json:"nextPageToken"`
	HistoryID string `json:"historyId"`
}

func (r *run) gmailDelta(ctx context.Context, base string, headers map[string]string, c *gmailCursor, profileHistory string) (bool, error) {
	raw, status, err := r.gmailGet(ctx, base, headers, "/history?startHistoryId="+url.QueryEscape(c.History)+"&historyTypes=messageAdded&historyTypes=messageDeleted&maxResults=100&pageToken="+url.QueryEscape(c.Page))
	if status == 404 {
		c.Full = true
		c.Page = ""
		c.Anchor = profileHistory
		if err = gmailSave(r, c); err != nil {
			return false, err
		}
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var page gmailHistoryPage
	if json.Unmarshal(raw, &page) != nil {
		return false, errors.New("Gmail history invalid")
	}
	if err := r.gmailApplyHistory(ctx, base, headers, &page); err != nil {
		return false, err
	}
	// Commit only an entirely consumed history page; replay of a failed page is idempotent.
	if page.Next != "" {
		c.Page = page.Next
	} else {
		c.Page = ""
		if page.HistoryID != "" {
			c.History = page.HistoryID
		}
	}
	if err = gmailSave(r, c); err != nil {
		return false, err
	}
	if page.Next == "" {
		r.receipt.HeadComplete = true
		return true, nil
	}
	return false, nil
}

func (r *run) gmailApplyHistory(ctx context.Context, base string, headers map[string]string, page *gmailHistoryPage) error {
	var err error
	for _, h := range page.History {
		deleted := map[string]bool{}
		for _, d := range h.Deleted {
			deleted[d.Message.ID] = true
			notice, _ := json.Marshal(map[string]string{"id": d.Message.ID, "state": "deleted", "historyId": h.ID})
			if err = r.emit(d.Message.ID, notice, ObservationState{Version: "gmail-history-tombstone-v1", Labels: map[string]string{"state": "deleted"}}, time.Time{}); err != nil {
				return err
			}
		}
		seen := map[string]bool{}
		for _, added := range h.Added {
			m := added.Message
			if deleted[m.ID] || seen[m.ID] {
				continue
			}
			seen[m.ID] = true
			if err = r.gmailMessage(ctx, base, headers, m.ID); err != nil {
				return err
			}
		}
	}
	return nil
}
