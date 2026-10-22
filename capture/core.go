package fetch

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type ObservationState struct {
	Version string            `json:"version,omitempty"`
	Labels  map[string]string `json:"labels,omitempty"`
}
type SpoolEntry struct {
	Schema        string           `json:"schema"`
	ObservationID string           `json:"observation_id,omitempty"`
	AttemptID     string           `json:"attempt_id,omitempty"`
	State         ObservationState `json:"state,omitempty"`
	Source        string           `json:"source"`
	Account       string           `json:"account"`
	Key           string           `json:"key"`
	SHA256        string           `json:"sha256"`
	Bytes         int64            `json:"bytes"`
	Path          string           `json:"path"`
	CapturedAt    time.Time        `json:"captured_at"`
	ItemTime      *time.Time       `json:"item_time,omitempty"`
}
type Receipt struct {
	HeadComplete     bool           `json:"head_complete"`
	Backfill         *xBackfill     `json:"backfill,omitempty"`
	BackfillError    string         `json:"backfill_error"`
	RateLimited      bool           `json:"rate_limited"`
	RequestsSent     int            `json:"requests_sent,omitempty"`
	HasMore          bool           `json:"has_more,omitempty"`
	Account          string         `json:"account,omitempty"`
	FailedIDs        []string       `json:"failed_ids,omitempty"`
	OldestFailed     time.Time      `json:"oldest_failed,omitempty"`
	ExclusionReasons map[string]int `json:"exclusion_reasons,omitempty"`
	FailureReasons   map[string]int `json:"failure_reasons,omitempty"`
	Source           string         `json:"source"`
	OK               bool           `json:"ok"`
	Enumerated       int            `json:"enumerated"`
	Written          int            `json:"written"`
	Unchanged        int            `json:"unchanged"`
	Excluded         int            `json:"excluded"`
	Failed           int            `json:"failed"`
	SourceHead       time.Time      `json:"source_head"`
	DurationS        float64        `json:"duration_s"`
	Error            string         `json:"error,omitempty"`
	NewestID         string         `json:"newest_id,omitempty"`
	ConnectionHead   time.Time      `json:"connection_head,omitempty"`
	LowerFidelity    int            `json:"lower_fidelity,omitempty"`
	UnknownFields    int            `json:"unknown_fields,omitempty"`
}
type checkpoint struct {
	Version    int             `json:"v"`
	Cursor     json.RawMessage `json:"cursor,omitempty"`
	Head       time.Time       `json:"source_head"`
	NewestTime time.Time       `json:"newest_time"`
	NewestID   string          `json:"newest_id,omitempty"`
	// Hashes are the source traversal's dedupe fold, committed with its cursor.
	Hashes  map[string]string `json:"hashes"`
	Receipt Receipt           `json:"receipt"`
}
type run struct {
	source, account, spool, statePath, journal, history, device, legacy, objects, contextPath, aliasesFrom string
	limit                                                                                                  int
	pendingOnly                                                                                            bool
	env                                                                                                    map[string]string
	state                                                                                                  checkpoint
	receipt                                                                                                Receipt
	client                                                                                                 *http.Client
}

func digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func atomicWrite(path string, b []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func writeSpool(root, source, account, key string, payload []byte, state ObservationState, itemTime ...time.Time) error {
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil {
		return e
	}
	hash := digest(payload)
	rel := filepath.Join("objects", hash[:2], hash[2:4], hash)
	path := filepath.Join(root, rel)
	b, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		e = atomicWrite(path, payload)
	} else if e == nil && digest(b) != hash {
		e = errors.New("spool object corrupt")
	}
	if e != nil {
		return e
	}
	entry := SpoolEntry{Schema: "qmd.spool-entry.v1", ObservationID: hex.EncodeToString(id[:]), State: state, Source: source, Account: account, Key: key, SHA256: hash, Bytes: int64(len(payload)), Path: rel, CapturedAt: time.Now().UTC()}
	if len(itemTime) > 0 && !itemTime[0].IsZero() {
		stamp := itemTime[0].UTC()
		entry.ItemTime = &stamp
	}
	line, e := json.Marshal(entry)
	if e != nil {
		return e
	}
	f, e := os.OpenFile(filepath.Join(root, "index.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	_, e = f.Write(append(line, '\n'))
	if e == nil {
		e = f.Sync()
	}
	return e
}
func (r *run) observe(head time.Time) {
	if head.After(r.state.Head) {
		r.state.Head = head.UTC()
	}
}
func (r *run) fail() { r.receipt.Enumerated++; r.receipt.Failed++ }
func (r *run) emit(key string, payload []byte, state ObservationState, head time.Time) error {
	return r.emitHash(key, payload, state, head, digest(payload))
}
func (r *run) emitHash(key string, payload []byte, state ObservationState, head time.Time, hash string) error {
	r.receipt.Enumerated++
	r.observe(head)
	if !head.IsZero() && !head.Before(r.state.NewestTime) {
		r.state.NewestTime = head
		r.state.NewestID = key
		r.receipt.NewestID = key
	}
	if key == "" {
		r.receipt.Failed++
		return errors.New("item identity missing")
	}
	if r.state.Hashes[key] == hash {
		r.receipt.Unchanged++
		return nil
	}
	var disk syscall.Statfs_t
	if syscall.Statfs(filepath.Dir(r.statePath), &disk) != nil || disk.Bavail*uint64(disk.Bsize) < uint64(8<<30)+uint64(len(payload))*2 {
		r.receipt.Failed++
		return errors.New("disk below write floor")
	}
	if e := writeSpool(r.spool, r.source, r.account, key, payload, state, head); e != nil {
		r.receipt.Failed++
		return errors.New("spool write failed")
	}
	r.state.Hashes[key] = hash
	r.receipt.Written++
	return nil
}
func (r *run) save() error {
	r.state.Receipt = r.receipt
	b, e := json.Marshal(r.state)
	if e == nil {
		e = atomicWrite(r.statePath, b)
	}
	if e != nil {
		return errors.New("checkpoint write failed")
	}
	return nil
}
func loadEnv(paths []string) (map[string]string, error) {
	out := map[string]string{}
	for _, p := range paths {
		f, e := os.Open(p)
		if os.IsNotExist(e) {
			continue
		}
		if e != nil {
			return nil, errors.New("credential file unreadable")
		}
		s := bufio.NewScanner(f)
		for s.Scan() {
			line := strings.TrimSpace(s.Text())
			line = strings.TrimPrefix(line, "export ")
			k, v, ok := strings.Cut(line, "=")
			if !ok || strings.HasPrefix(k, "#") {
				continue
			}
			out[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), "\"'")
		}
		e = s.Err()
		f.Close()
		if e != nil {
			return nil, errors.New("credential file invalid")
		}
	}
	for _, line := range os.Environ() {
		k, v, _ := strings.Cut(line, "=")
		out[k] = v
	}
	return out, nil
}
func (r *run) request(ctx context.Context, method, endpoint string, headers map[string]string, body []byte) ([]byte, int, error) {
	for attempt := 0; attempt < 2; attempt++ {
		req, e := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
		if e != nil {
			return nil, 0, errors.New("request invalid")
		}
		for k, v := range headers {
			req.Header.Set(k, v)
		}
		resp, e := r.client.Do(req)
		if e != nil {
			if attempt == 0 && ctx.Err() == nil {
				continue
			}
			return nil, 0, errors.New("provider transport failed")
		}
		status := resp.StatusCode
		b, e := io.ReadAll(io.LimitReader(resp.Body, 64<<20+1))
		resp.Body.Close()
		if e != nil || len(b) > 64<<20 {
			return nil, status, errors.New("provider response invalid or too large")
		}
		if (status == 429 || status >= 500) && attempt == 0 {
			delay := time.Second
			if secs, err := strconv.ParseFloat(resp.Header.Get("Retry-After"), 64); err == nil && secs > 0 {
				delay = time.Duration(secs * float64(time.Second))
			} else if t, err := http.ParseTime(resp.Header.Get("Retry-After")); err == nil && time.Until(t) > 0 {
				delay = time.Until(t)
			}
			timer := time.NewTimer(delay)
			select {
			case <-ctx.Done():
				timer.Stop()
				if status == 429 {
					r.receipt.RateLimited = true
				}
				return nil, status, errors.New("request deadline reached")
			case <-timer.C:
			}
			continue
		}
		if status == 429 {
			r.receipt.RateLimited = true
		}
		if status < 200 || status >= 300 {
			return b, status, fmt.Errorf("provider HTTP %d", status)
		}
		return b, status, nil
	}
	return nil, 0, errors.New("request failed")
}
func Main(args []string) int {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: qmd2-fetch <source> --spool DIR --state DIR")
		return 2
	}
	fs := flag.NewFlagSet("qmd2-fetch", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	spool := fs.String("spool", "", "spool directory")
	state := fs.String("state", "", "checkpoint directory")
	account := fs.String("account", "", "asserted source account")
	envFile := fs.String("env-file", "/etc/qmd-agent/qmd-agent.env", "existing credentials file")
	username := fs.String("x-username", "", "X username for own posts")
	journal := fs.String("journal", "", "read-only journal root")
	history := fs.String("history", "", "local history object")
	legacy := fs.String("legacy", "", "original read-only messages.db")
	limit := fs.Int("limit", 1000, "maximum import rows per run")
	pendingOnly := fs.Bool("pending-only", false, "Instapaper: retry checkpoint ids without listing")
	objects := fs.String("objects", "", "custody inventory JSONL")
	contextPath := fs.String("context", "", "frozen source context JSON")
	aliasesFrom := fs.String("aliases-from", "", "all original history objects directory")
	device := fs.String("device", "", "read-only device snapshot")
	timeout := fs.Duration("timeout", 30*time.Minute, "run deadline")
	if args[0] == "--help" || args[0] == "-h" {
		fmt.Println("qmd2-fetch gmail|x|instapaper|fathom|whatsapp|calendar --spool DIR --state DIR [--account ID] [--x-username USER] [--journal DIR] [--history FILE | --legacy DB | --objects JSONL] [--context JSON] [--aliases-from DIR] [--limit N] [--device SNAPSHOT]")
		return 0
	}
	if e := fs.Parse(args[1:]); e != nil || *spool == "" || *state == "" || fs.NArg() != 0 || *limit < 1 || *limit > 10000 || boolCount(*history != "", *legacy != "", *objects != "") > 1 || (*pendingOnly && args[0] != "instapaper") {
		fmt.Fprintln(os.Stderr, "invalid fetch arguments")
		return 2
	}
	started := time.Now()
	r := &run{source: args[0], account: *account, spool: *spool, journal: *journal, history: *history, device: *device, legacy: *legacy, objects: *objects, contextPath: *contextPath, aliasesFrom: *aliasesFrom, limit: *limit, receipt: Receipt{Source: args[0], ExclusionReasons: map[string]int{}}, client: &http.Client{Timeout: 120 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
	r.pendingOnly = *pendingOnly
	e := r.loadAccount(*envFile)
	if e == nil && *username != "" {
		r.env["QMD_X_USERNAME"] = *username
	}
	var lock *os.File
	locked := false
	if e == nil {
		lock, locked, e = r.openState(*state)
		if lock != nil {
			defer lock.Close()
		}
	}
	r.receipt.NewestID = r.state.NewestID
	if r.receipt.NewestID == "" {
		r.receipt.NewestID = r.state.Receipt.NewestID
	}
	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if e == nil {
		e = r.capture(ctx)
	}
	return r.finish(started, e, locked)
}

func (r *run) loadAccount(envFile string) error {
	home, _ := os.UserHomeDir()
	env, e := loadEnv([]string{envFile, filepath.Join(home, ".config/agent-secrets/credentials.env")})
	r.env = env
	if e == nil {
		if r.account == "" {
			r.account = env["QMD_"+strings.ToUpper(r.source)+"_ACCOUNT"]
		}
		if r.source == "x" && r.account != "" {
			r.account = digest([]byte(r.account))
		}
		if r.account == "" {
			e = errors.New("source account required")
		}
	}
	return e
}

func (r *run) openState(stateDir string) (*os.File, bool, error) {
	e := os.MkdirAll(stateDir, 0700)
	var lock *os.File
	locked := false
	if e == nil {
		r.statePath = filepath.Join(stateDir, r.source+"-"+digest([]byte(r.account))[:16]+".json")
		lock, e = os.OpenFile(r.statePath+".lock", os.O_CREATE|os.O_RDWR, 0600)
		if e == nil {
			e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
			locked = e == nil
		}
	}
	if e == nil {
		r.state = checkpoint{Version: 1, Hashes: map[string]string{}}
		b, err := os.ReadFile(r.statePath)
		if err == nil {
			if json.Unmarshal(b, &r.state) != nil || r.state.Version != 1 || r.state.Hashes == nil {
				e = errors.New("checkpoint invalid")
			}
		} else if !os.IsNotExist(err) {
			e = errors.New("checkpoint unreadable")
		}
	}
	return lock, locked, e
}

func (r *run) capture(ctx context.Context) error {
	switch r.source {
	case "gmail":
		return runGmail(ctx, r)
	case "x":
		return runX(ctx, r)
	case "instapaper":
		return runInstapaper(ctx, r)
	case "fathom":
		return runFathom(ctx, r)
	case "calendar":
		return runCalendar(ctx, r)
	case "whatsapp":
		return runWhatsApp(ctx, r)
	default:
		return errors.New("unknown source")
	}
}

func (r *run) finish(started time.Time, e error, locked bool) int {
	r.receipt.Account = r.account
	r.receipt.SourceHead = r.state.Head
	r.receipt.OK = e == nil && r.receipt.Failed == 0 && !r.pendingOnly && (!r.receipt.RateLimited || (r.source == "x" && r.receipt.HeadComplete))
	r.receipt.DurationS = time.Since(started).Seconds()
	if e != nil {
		r.receipt.Error = e.Error()
	}
	if r.statePath != "" && r.state.Hashes != nil && locked {
		if err := r.save(); err != nil {
			r.receipt.OK = false
			r.receipt.Error = "checkpoint write failed"
		}
	}
	json.NewEncoder(os.Stdout).Encode(r.receipt)
	if !r.receipt.OK {
		return 1
	}
	return 0
}

func boolCount(values ...bool) int {
	n := 0
	for _, v := range values {
		if v {
			n++
		}
	}
	return n
}
