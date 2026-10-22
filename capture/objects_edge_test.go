package fetch

import (
	"encoding/base64"
	"encoding/json"
	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestCustodyObjectsFilesystemSubprocess(t *testing.T) {
	root := t.TempDir()
	inventory, aliases, frozen, originals := seedCustodyObjects(t, root)
	spool := filepath.Join(root, "spool")
	state := filepath.Join(root, "state")
	child := func() (Receipt, int) {
		return captureChild(t, []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1"}, "whatsapp", "--objects", inventory, "--context", frozen, "--aliases-from", aliases, "--account", "synthetic-account", "--limit", "3", "--spool", spool, "--state", state, "--env-file", filepath.Join(root, "absent.env"))
	}
	first, code := child()
	if code == 0 || first.OK || first.Failed != 1 || first.Written != 7 || first.HasMore || strings.HasPrefix(first.NewestID, "history:") {
		t.Fatalf("first objects receipt %+v", first)
	}
	cpPath := filepath.Join(state, "whatsapp-"+digest([]byte("synthetic-account"))[:16]+".json")
	b, _ := os.ReadFile(cpPath)
	var cp checkpoint
	json.Unmarshal(b, &cp)
	var c waCursor
	json.Unmarshal(cp.Cursor, &c)
	if len(c.ObjectFailures) != 1 || c.Pairs["2@lid"] != "12025550102@s.whatsapp.net" {
		t.Fatal("failure or full alias prepass not checkpointed")
	}
	os.WriteFile(filepath.Join(root, "two.json"), originals[1], 0600)
	second, code := child()
	if code != 0 || !second.OK || second.Failed != 0 || second.Written != 3 || second.Unchanged != 1 || second.HasMore {
		t.Fatalf("retry objects receipt %+v", second)
	}
	third, code := child()
	if code != 0 || !third.OK || third.Written != 0 {
		t.Fatalf("unchanged objects receipt %+v", third)
	}
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	seen := map[string]bool{}
	for _, line := range splitLines(index) {
		var entry SpoolEntry
		json.Unmarshal(line, &entry)
		payload, _ := os.ReadFile(filepath.Join(spool, entry.Path))
		var record struct {
			Decoded waDecoded `json:"decoded"`
		}
		json.Unmarshal(payload, &record)
		if record.Decoded.Kind == "message" {
			if record.Decoded.Chat != "12025550101@s.whatsapp.net" || record.Decoded.Sender != "12025550102@s.whatsapp.net" {
				t.Fatal("children emitted before all aliases")
			}
			seen[record.Decoded.ID] = true
		}
	}
	if len(seen) != 3 {
		t.Fatal("later good item blocked by failed object")
	}
}

func seedCustodyObjects(t *testing.T, root string) (string, string, string, [][]byte) {
	t.Helper()
	aliases := filepath.Join(root, "aliases")
	os.Mkdir(aliases, 0700)
	for i, pair := range [][2]string{{"1", "12025550101"}, {"2", "12025550102"}} {
		record := map[string]any{"kind": "history_sync", "info": map[string]any{"Data": map[string]any{"syncType": 0, "phoneNumberToLidMappings": []any{map[string]string{"lidJID": pair[0], "pnJID": pair[1]}}}}}
		b, _ := json.Marshal(record)
		os.WriteFile(filepath.Join(aliases, []string{"first.jsonl", "second.jsonl"}[i]), append(b, '\n'), 0600)
	}
	frozen := filepath.Join(root, "context.json")
	os.WriteFile(frozen, []byte(`{"ctx_update":{"aliases":{},"contacts":{"12025550102@s.whatsapp.net":"Frozen contact"},"groups":{}}}`), 0600)
	at := time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC)
	inventory := filepath.Join(root, "inventory.jsonl")
	var rows []inventoryRow
	var originals [][]byte
	for i, id := range []string{"one", "two", "three"} {
		text := "synthetic " + id
		protoBytes, _ := proto.Marshal(&e2e.Message{Conversation: &text})
		b, _ := json.Marshal(waRecord{Kind: "message", Info: map[string]any{"ID": id, "Chat": "1@lid", "Sender": "2@lid", "Timestamp": at.Format(time.RFC3339), "IsFromMe": false}, Proto: base64.StdEncoding.EncodeToString(protoBytes)})
		path := filepath.Join(root, id+".json")
		if i != 1 {
			os.WriteFile(path, b, 0600)
		}
		originals = append(originals, b)
		rows = append(rows, inventoryRow{ObjectHash: digest(b), SourceSystem: "whatsapp", ExternalItemKey: digest([]byte(id)), SourceAt: at, CapturedAt: at, Locations: []inventoryLocation{{Kind: "filesystem", Ref: "local:" + path}}})
	}
	f, _ := os.Create(inventory)
	for _, row := range rows {
		json.NewEncoder(f).Encode(row)
	}
	f.Close()
	return inventory, aliases, frozen, originals
}
