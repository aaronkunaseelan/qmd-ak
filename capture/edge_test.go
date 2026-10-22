package fetch

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCaptureProcess(t *testing.T) {
	if os.Getenv("QMD2_CAPTURE_TEST_CHILD") != "1" {
		return
	}
	marker := -1
	for i, a := range os.Args {
		if a == "--" {
			marker = i
			break
		}
	}
	if marker < 0 {
		os.Exit(2)
	}
	os.Exit(Main(os.Args[marker+1:]))
}
func splitLines(b []byte) [][]byte {
	var out [][]byte
	start := 0
	for i, v := range b {
		if v == '\n' {
			if i > start {
				out = append(out, b[start:i])
			}
			start = i + 1
		}
	}
	return out
}

func captureChild(t *testing.T, env []string, args ...string) (Receipt, int) {
	t.Helper()
	cmd := exec.Command(os.Args[0], append([]string{"-test.run=^TestCaptureProcess$", "--"}, args...)...)
	cmd.Env = env
	out, err := cmd.Output()
	code := 0
	if err != nil {
		if exit, ok := err.(*exec.ExitError); ok {
			code = exit.ExitCode()
		} else {
			t.Fatal(err)
		}
	}
	var receipt Receipt
	if json.Unmarshal(out, &receipt) != nil {
		t.Fatal("invalid child receipt")
	}
	if receipt.Enumerated != receipt.Written+receipt.Unchanged+receipt.Excluded+receipt.Failed {
		t.Fatal("receipt counts do not add up")
	}
	return receipt, code
}

func TestJournalFilesystemSubprocess(t *testing.T) {
	root := t.TempDir()
	journal := filepath.Join(root, "journal")
	os.Mkdir(journal, 0700)
	spool := filepath.Join(root, "spool")
	state := filepath.Join(root, "state")
	timestamp := "2026-10-13T19:00:00Z"
	text := "synthetic message"
	p, _ := proto.Marshal(&e2e.Message{Conversation: &text})
	line := func(id, epoch string, received string) []byte {
		b, _ := json.Marshal(map[string]any{"v": 1, "seq": 2, "epoch": epoch, "kind": "message", "received_at": received, "info": map[string]any{"ID": id, "Chat": "12025550123@s.whatsapp.net", "Sender": "12025550123@s.whatsapp.net", "Timestamp": timestamp}, "proto_b64": base64.StdEncoding.EncodeToString(p)})
		return append(b, '\n')
	}
	header := []byte(`{"v":1,"seq":1,"epoch":"20261013T190000Z","kind":"header","info":{"account_jid":"synthetic-account"}}` + "\n")
	path := filepath.Join(journal, "00000001.jsonl")
	first := append(append([]byte{}, header...), line("one", "20261013T190000Z", "2026-10-13T20:00:00Z")...)
	activity := []byte(`{"kind":"presence","received_at":"2026-10-13T23:56:49Z"}` + "\n")
	first = append(first, activity...)
	os.WriteFile(path, append(first, []byte(`{"kind":"message"`)...), 0600)
	child := func() (Receipt, int) {
		return captureChild(t, append(os.Environ(), "QMD2_CAPTURE_TEST_CHILD=1"), "whatsapp", "--account", "synthetic-account", "--journal", journal, "--spool", spool, "--state", state, "--env-file", filepath.Join(root, "absent.env"))
	}
	a, code := child()
	if code != 0 || !a.OK || !a.HeadComplete || a.Written != 1 || a.SourceHead.Format(time.RFC3339) != timestamp || a.ConnectionHead.Format(time.RFC3339) != "2026-10-13T23:56:49Z" {
		t.Fatalf("first capture %+v code %d", a, code)
	}
	checkJournalSpool(t, spool)
	// Complete the torn line as a malformed record: cursor must not consume it.
	os.WriteFile(path, append(append([]byte{}, first...), []byte(`{"kind":"message"}`+"\n")...), 0600)
	a, code = child()
	if code == 0 || a.OK || a.HeadComplete || a.Failed != 1 {
		t.Fatalf("malformed tail %+v", a)
	}
	// Repair the same failed offset and append a later segment with a backward epoch.
	os.WriteFile(path, append(append([]byte{}, first...), line("two", "20261013T180000Z", "2026-10-13T18:00:00Z")...), 0600)
	os.WriteFile(filepath.Join(journal, "00000002.jsonl"), line("three", "20261012T180000Z", "2026-10-12T18:00:00Z"), 0600)
	a, code = child()
	if code != 0 || a.Written != 2 {
		t.Fatalf("backward clock replay %+v", a)
	}
	cpPath := filepath.Join(state, "whatsapp-"+digest([]byte("synthetic-account"))[:16]+".json")
	cpBytes, _ := os.ReadFile(cpPath)
	var cp checkpoint
	json.Unmarshal(cpBytes, &cp)
	cp.Head = time.Date(2026, 10, 13, 23, 56, 49, 0, time.UTC)
	cpBytes, _ = json.Marshal(cp)
	os.WriteFile(cpPath, cpBytes, 0600)
	a, code = child()
	if code != 0 || a.Written != 0 || !a.SourceHead.Equal(time.Date(2026, 10, 13, 19, 0, 0, 0, time.UTC)) {
		t.Fatalf("unchanged capture %+v", a)
	}
}

func checkJournalSpool(t *testing.T, spool string) {
	t.Helper()
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	var entry SpoolEntry
	if json.Unmarshal(bytes.TrimSpace(index), &entry) != nil || entry.Schema != "qmd.spool-entry.v1" || entry.AttemptID != "" || entry.ObservationID == "" {
		t.Fatal("spool ABI invalid")
	}
	if entry.ItemTime == nil || entry.ItemTime.Format(time.RFC3339) != "2026-10-13T19:00:00Z" {
		t.Fatal("spool item time must match the capture head, not arrival time")
	}
	payload, e := os.ReadFile(filepath.Join(spool, entry.Path))
	if e != nil || int64(len(payload)) != entry.Bytes || digest(payload) != entry.SHA256 {
		t.Fatal("spool payload integrity invalid")
	}
	if !strings.HasPrefix(entry.Path, "objects/"+entry.SHA256[:2]+"/"+entry.SHA256[2:4]+"/") {
		t.Fatal("spool object layout invalid")
	}
}
