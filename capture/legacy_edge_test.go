package fetch

import (
	"database/sql"
	"encoding/json"
	common "go.mau.fi/whatsmeow/proto/waCommon"
	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	web "go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLegacySQLiteWALSubprocess(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "messages.db")
	db, text := writeLegacyDB(t, path)
	defer db.Close()
	beforeDB, _ := waFileHash(path)
	beforeWAL, e := waFileHash(path + "-wal")
	if e != nil {
		t.Fatal(e)
	}
	state := filepath.Join(root, "state")
	os.Mkdir(state, 0700)
	spool := filepath.Join(root, "spool")
	account := "synthetic-account"
	seedLegacyCursor(t, state, account)
	child := func() Receipt {
		r, code := captureChild(t, []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1"}, "whatsapp", "--legacy", path, "--limit", "1", "--account", account, "--spool", spool, "--state", state, "--env-file", filepath.Join(root, "absent.env"))
		if code != 0 {
			t.Fatal("legacy subprocess failed")
		}
		return r
	}
	first := child()
	if !first.OK || first.Written != 2 || !first.HasMore {
		t.Fatalf("first chunk %+v", first)
	}
	second := child()
	if !second.OK || second.Written != 1 || second.Unchanged != 1 || second.HasMore {
		t.Fatalf("second chunk %+v", second)
	}
	third := child()
	if !third.OK || third.Written != 0 || third.HasMore {
		t.Fatalf("unchanged chunk %+v", third)
	}
	checkLegacySpool(t, spool, beforeDB, beforeWAL, text)
	afterDB, _ := waFileHash(path)
	afterWAL, _ := waFileHash(path + "-wal")
	if beforeDB != afterDB || beforeWAL != afterWAL {
		t.Fatal("read-only capture changed original DB or WAL")
	}
}

func writeLegacyDB(t *testing.T, path string) (*sql.DB, string) {
	t.Helper()
	db, e := sql.Open("sqlite3", path)
	if e != nil {
		t.Fatal(e)
	}
	db.SetMaxOpenConns(1)
	for _, query := range []string{"PRAGMA journal_mode=WAL", "CREATE TABLE chats(jid TEXT,name TEXT)", "CREATE TABLE messages(wa_id TEXT,chat_jid TEXT,sender_jid TEXT,sender_name TEXT,timestamp INTEGER,raw_proto BLOB)", "INSERT INTO chats VALUES('1@lid','Synthetic chat')"} {
		if _, e = db.Exec(query); e != nil {
			t.Fatal(e)
		}
	}
	text := "synthetic legacy"
	msg := &e2e.Message{Conversation: &text}
	raw1, _ := proto.Marshal(msg)
	id := "two"
	chat := "1@lid"
	mine := true
	raw2, _ := proto.Marshal(&web.WebMessageInfo{Key: &common.MessageKey{ID: &id, RemoteJID: &chat, FromMe: &mine}, Message: msg})
	for i, raw := range [][]byte{raw1, raw2} {
		id := []string{"one", "two"}[i]
		if _, e = db.Exec("INSERT INTO messages VALUES(?,?,?,?,?,?)", id, "1@lid", "2@lid", "Synthetic sender", time.Date(2026, 10, 13, 0, 0, 0, 0, time.UTC).Unix(), raw); e != nil {
			t.Fatal(e)
		}
	}
	return db, text
}

func seedLegacyCursor(t *testing.T, state, account string) {
	t.Helper()
	c := waCursor{PairsVersion: 2, Pairs: map[string]string{"1@lid": "12025550101@s.whatsapp.net", "2@lid": "12025550102@s.whatsapp.net"}}
	cursor, _ := json.Marshal(c)
	seed, _ := json.Marshal(checkpoint{Version: 1, Cursor: cursor, Hashes: map[string]string{}})
	os.WriteFile(filepath.Join(state, "whatsapp-"+digest([]byte(account))[:16]+".json"), seed, 0600)
}

func checkLegacySpool(t *testing.T, spool, beforeDB, beforeWAL, text string) {
	t.Helper()
	index, _ := os.ReadFile(filepath.Join(spool, "index.jsonl"))
	lines := splitLines(index)
	if len(lines) != 3 {
		t.Fatal("spool must have one context marker and two rows")
	}
	for ordinal, line := range lines {
		var entry SpoolEntry
		json.Unmarshal(line, &entry)
		b, e := os.ReadFile(filepath.Join(spool, entry.Path))
		if e != nil {
			t.Fatal(e)
		}
		var payload struct {
			Raw     map[string]any `json:"raw"`
			Decoded map[string]any `json:"decoded"`
			Update  struct {
				Aliases  map[string]string `json:"aliases"`
				Contacts map[string]string `json:"contacts"`
				Groups   map[string]string `json:"groups"`
			} `json:"ctx_update"`
		}
		if json.Unmarshal(b, &payload) != nil {
			t.Fatal("payload invalid")
		}
		if payload.Raw["legacy_db_sha256"] != beforeDB || payload.Raw["legacy_wal_sha256"] != beforeWAL || payload.Raw["rowid"] != float64(ordinal) {
			t.Fatal("legacy locator invalid")
		}
		if ordinal == 0 {
			if payload.Update.Aliases["1@lid"] == "" || payload.Update.Groups["12025550101@s.whatsapp.net"] != "Synthetic chat" {
				t.Fatal("initial aliases/groups absent")
			}
			continue
		}
		if payload.Decoded["chat"] != "12025550101@s.whatsapp.net" || payload.Decoded["text"] != text || payload.Update.Contacts["12025550102@s.whatsapp.net"] != "Synthetic sender" {
			t.Fatal("decoded fields or contacts absent")
		}
		if ordinal == 1 {
			if _, exists := payload.Decoded["from_me"]; exists {
				t.Fatal("from_me invented for e2e-only bytes")
			}
		} else if payload.Decoded["from_me"] != true {
			t.Fatal("native from_me absent")
		}
	}
}
