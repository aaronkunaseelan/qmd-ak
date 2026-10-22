// Package journal persists opaque capture records without interpreting them.
package journal

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Record struct {
	V                int             `json:"v"`
	Seq              uint64          `json:"seq"`
	Epoch            string          `json:"epoch"`
	ReceivedAt       time.Time       `json:"received_at"`
	Kind             string          `json:"kind"`
	Info             json.RawMessage `json:"info"`
	ProtoB64         string          `json:"proto_b64"`
	WhatsmeowVersion string          `json:"whatsmeow_version"`
}
type Writer struct {
	mu       sync.Mutex
	dir      string
	lock     *os.File
	file     *os.File
	index    int
	seq      uint64
	epoch    string
	max      int64
	syncFile func(*os.File) error
}

func files(dir string) ([]string, error) {
	names, e := filepath.Glob(filepath.Join(dir, "journal", "*.jsonl"))
	sort.Strings(names)
	return names, e
}
func Open(dir, epoch, accountJID, deviceID, version string, maxBytes int64) (*Writer, error) {
	if epoch == "" || maxBytes <= 0 {
		return nil, errors.New("epoch and positive maxBytes required")
	}
	if e := os.MkdirAll(filepath.Join(dir, "journal"), 0700); e != nil {
		return nil, e
	}
	l, e := os.OpenFile(filepath.Join(dir, "journal.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return nil, e
	}
	if e = syscall.Flock(int(l.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		l.Close()
		return nil, fmt.Errorf("journal already owned: %w", e)
	}
	w := &Writer{dir: dir, lock: l, epoch: epoch, max: maxBytes, syncFile: func(f *os.File) error { return f.Sync() }}
	names, e := files(dir)
	if e != nil {
		w.Close()
		return nil, e
	}
	if e = w.resume(names); e != nil {
		w.Close()
		return nil, e
	}
	if e = w.openSegment(); e != nil {
		w.Close()
		return nil, e
	}
	if len(names) == 0 {
		info, _ := json.Marshal(map[string]string{"account_jid": accountJID, "device_id": deviceID})
		if e = w.Append(Record{Kind: "header", Info: info, WhatsmeowVersion: version}); e != nil {
			w.Close()
			return nil, e
		}
	}
	return w, nil
}
func (w *Writer) resume(names []string) error {
	if len(names) > 0 {
		last := names[len(names)-1]
		n, e := strconv.Atoi(strings.TrimSuffix(filepath.Base(last), ".jsonl"))
		if e != nil {
			return e
		}
		w.index = n
		for _, name := range names {
			b, e := os.ReadFile(name)
			if e != nil {
				return e
			}
			end := bytes.LastIndexByte(b, '\n') + 1
			if end < len(b) {
				if name != last {
					return errors.New("torn non-final segment")
				}
				aside := name + ".torn." + strconv.FormatInt(time.Now().UnixNano(), 10)
				if e = os.WriteFile(aside, b[end:], 0600); e != nil {
					return e
				}
				w.index++
			}
			for _, line := range bytes.Split(b[:end], []byte{'\n'}) {
				if len(line) == 0 {
					continue
				}
				var r Record
				if e = json.Unmarshal(line, &r); e != nil {
					return e
				}
				if r.Epoch == w.epoch && r.Seq > w.seq {
					w.seq = r.Seq
				}
			}
		}
	} else {
		w.index = 1
	}
	return nil
}

func (w *Writer) openSegment() error {
	f, e := os.OpenFile(filepath.Join(w.dir, "journal", fmt.Sprintf("%08d.jsonl", w.index)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if e == nil {
		w.file = f
	}
	return e
}
func (w *Writer) Append(r Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.file == nil {
		return errors.New("closed")
	}
	r.V = 1
	r.Seq = w.seq + 1
	r.Epoch = w.epoch
	if r.ReceivedAt.IsZero() {
		r.ReceivedAt = time.Now().UTC()
	}
	b, e := json.Marshal(r)
	if e != nil {
		return e
	}
	b = append(b, '\n')
	st, e := w.file.Stat()
	if e != nil {
		return e
	}
	if st.Size() > 0 && st.Size()+int64(len(b)) > w.max {
		if e = w.file.Close(); e != nil {
			return e
		}
		w.index++
		if e = w.openSegment(); e != nil {
			return e
		}
	}
	n, e := w.file.Write(b)
	if e != nil {
		return e
	}
	if n != len(b) {
		return io.ErrShortWrite
	}
	if e = w.syncFile(w.file); e != nil {
		return e
	}
	w.seq = r.Seq
	return nil
}
func (w *Writer) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	var e error
	if w.file != nil {
		e = w.file.Close()
		w.file = nil
	}
	if w.lock != nil {
		syscall.Flock(int(w.lock.Fd()), syscall.LOCK_UN)
		w.lock.Close()
		w.lock = nil
	}
	return e
}
