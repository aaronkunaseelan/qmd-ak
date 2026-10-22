package fetch

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestHistoryRawFilesystemSubprocess(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "history.jsonl")
	source := []byte(` { "kind": "history_sync", "received_at":"2026-10-14T23:56:49Z", "info": { "Data": { "syncType": 0,
 "phoneNumberToLidMappings": [{"lidJID":"1","pnJID":"12025550101"}],
 "pushnames": [{"ID":"1@lid","pushname":"Synthetic sender"}],
 "conversations": [{"ID":"1@lid","name":"Synthetic chat","messages":[{"message":{"key":{"remoteJID":"1@lid","ID":"one","fromMe":false},"message":{"conversation":"synthetic body"},"messageTimestamp":"1791849600"}}]}] } } }
`)
	// A JSONL record may contain whitespace but never an embedded physical newline.
	source = bytes.ReplaceAll(source, []byte("\n "), []byte(" "))
	os.WriteFile(path, source, 0600)
	spool := filepath.Join(root, "spool")
	state := filepath.Join(root, "state")
	cmd := exec.Command(os.Args[0], "-test.run=^TestCaptureProcess$", "--", "whatsapp", "--history", path, "--account", "synthetic-account", "--spool", spool, "--state", state, "--env-file", filepath.Join(root, "absent.env"))
	cmd.Env = []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1"}
	out, e := cmd.Output()
	if e != nil {
		t.Fatal("history subprocess failed")
	}
	var receipt Receipt
	json.Unmarshal(out, &receipt)
	if !receipt.OK || receipt.Written != 3 || receipt.Enumerated != 3 || receipt.Failed != 0 || !receipt.SourceHead.Equal(time.Unix(1791849600, 0).UTC()) {
		t.Fatalf("history receipt %+v", receipt)
	}
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	lines := splitLines(index)
	if len(lines) != 3 {
		t.Fatal("must emit original, context, child in order")
	}
	var original, marker, child SpoolEntry
	json.Unmarshal(lines[0], &original)
	json.Unmarshal(lines[1], &marker)
	json.Unmarshal(lines[2], &child)
	retained, _ := os.ReadFile(filepath.Join(spool, original.Path))
	hash := digest(source)
	if !bytes.Equal(retained, source) || original.SHA256 != hash || original.Bytes != int64(len(source)) {
		t.Fatal("original bytes changed")
	}
	if original.State.Labels["archive_only"] != "true" || original.State.Labels["object_hash"] != hash || original.State.Labels["size"] != strconv.Itoa(len(source)) || original.State.Labels["source"] != "whatsapp" {
		t.Fatal("archive-only metadata invalid")
	}
	markerBytes, _ := os.ReadFile(filepath.Join(spool, marker.Path))
	var contextMarker struct {
		Raw     map[string]string            `json:"raw"`
		Context map[string]map[string]string `json:"ctx_update"`
	}
	if json.Unmarshal(markerBytes, &contextMarker) != nil || contextMarker.Raw["history_sha256"] != hash || contextMarker.Context["groups"]["12025550101@s.whatsapp.net"] != "Synthetic chat" || contextMarker.Context["contacts"]["12025550101@s.whatsapp.net"] != "Synthetic sender" {
		t.Fatal("context marker invalid")
	}
	childBytes, _ := os.ReadFile(filepath.Join(spool, child.Path))
	var childPayload struct {
		Raw     map[string]any `json:"raw"`
		Decoded waDecoded      `json:"decoded"`
	}
	json.Unmarshal(childBytes, &childPayload)
	if childPayload.Raw["history_sha256"] != hash || childPayload.Raw["ordinal"] != float64(0) || childPayload.Decoded.Chat != "12025550101@s.whatsapp.net" || childPayload.Decoded.Text != "synthetic body" {
		t.Fatal("child ref or native decode invalid")
	}
}
