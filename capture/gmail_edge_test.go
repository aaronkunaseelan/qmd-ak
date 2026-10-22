package fetch

import (
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestGmailHTTPSubprocess(t *testing.T) {
	for _, scenario := range []string{"failed_id_retry", "history_expiry_full_listing", "provider_deleted", "labels_only"} {
		t.Run(scenario, func(t *testing.T) {
			testGmailScenario(t, scenario)
		})
	}
}

func testGmailScenario(t *testing.T, scenario string) {
	t.Helper()
	root := t.TempDir()
	account := "synthetic@example.test"
	state, spool, checkpointPath := seedGmail(t, root, account)
	var failed atomic.Bool
	failed.Store(true)
	var messageCalls, listCalls, expiredCalls atomic.Int32
	server := gmailTestServer(t, account, scenario, &failed, &messageCalls, &listCalls, &expiredCalls)
	defer server.Close()
	child := func() (Receipt, int) {
		return captureChild(t, []string{"HOME=" + root, "QMD2_CAPTURE_TEST_CHILD=1", "QMD_GMAIL_BASE_URL=" + server.URL}, "gmail", "--account", account, "--spool", spool, "--state", state, "--env-file", filepath.Join(root, "absent.env"))
	}
	load := func() gmailCursor {
		b, e := os.ReadFile(checkpointPath)
		if e != nil {
			t.Fatal(e)
		}
		var checkpoint checkpoint
		if json.Unmarshal(b, &checkpoint) != nil {
			t.Fatal("checkpoint invalid")
		}
		var c gmailCursor
		json.Unmarshal(checkpoint.Cursor, &c)
		return c
	}
	first, code := child()
	if scenario == "failed_id_retry" {
		if code == 0 || first.OK || first.HeadComplete || first.Written != 1 || first.Failed != 1 || load().History != "10" || messageCalls.Load() != 2 {
			t.Fatalf("failed receipt=%+v cursor=%+v calls=%d", first, load(), messageCalls.Load())
		}
		failed.Store(false)
		second, code := child()
		if code != 0 || !second.OK || !second.HeadComplete || second.Written != 1 || second.Unchanged != 1 || second.Failed != 0 || load().History != "20" || messageCalls.Load() != 3 {
			t.Fatalf("retry receipt=%+v cursor=%+v calls=%d", second, load(), messageCalls.Load())
		}
	} else if scenario == "labels_only" {
		if code != 0 || !first.OK || !first.HeadComplete || first.Enumerated != 0 || first.Failed != 0 || load().History != "20" || messageCalls.Load() != 0 {
			t.Fatalf("labels receipt=%+v cursor=%+v calls=%d", first, load(), messageCalls.Load())
		}
	} else if scenario == "provider_deleted" {
		if code != 0 || !first.OK || !first.HeadComplete || first.Error != "" || first.Enumerated != 2 || first.Excluded != 1 || first.ExclusionReasons["provider_deleted"] != 1 || first.Failed != 0 || first.Written != 1 || load().History != "20" {
			t.Fatalf("deletion receipt=%+v cursor=%+v", first, load())
		}
		b, e := os.ReadFile(checkpointPath)
		var saved checkpoint
		if e != nil || json.Unmarshal(b, &saved) != nil || len(saved.Hashes) != 1 || saved.Hashes["m2"] == "" {
			t.Fatal("surviving message not saved")
		}
	} else {
		if code != 0 || !first.OK || !first.HeadComplete || first.Written != 2 || first.Failed != 0 || load().History != "20" || load().Full || listCalls.Load() != 2 || expiredCalls.Load() != 1 {
			t.Fatalf("expiry receipt=%+v cursor=%+v lists=%d", first, load(), listCalls.Load())
		}
	}
}

func gmailTestServer(t *testing.T, account, scenario string, failed *atomic.Bool, messageCalls, listCalls, expiredCalls *atomic.Int32) *httptest.Server {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		path := strings.TrimPrefix(req.URL.Path, "/gmail/v1/users/me")
		switch path {
		case "/profile":
			json.NewEncoder(w).Encode(map[string]string{"emailAddress": account, "historyId": "20"})
		case "/history":
			types := req.URL.Query()["historyTypes"]
			if len(types) != 2 || types[0] != "messageAdded" || types[1] != "messageDeleted" {
				t.Error("history must request added and deleted messages only")
			}
			if req.URL.Query().Get("startHistoryId") == "10" {
				if scenario == "history_expiry_full_listing" {
					expiredCalls.Add(1)
					w.WriteHeader(404)
					return
				}
				json.NewEncoder(w).Encode(map[string]any{"historyId": "20", "history": []any{map[string]any{"id": "11", "messages": []any{map[string]string{"id": "m1"}}, "messagesAdded": gmailTestAdded(scenario), "labelsAdded": []any{map[string]any{"message": map[string]string{"id": "m1"}}}}}})
				return
			}
			json.NewEncoder(w).Encode(map[string]any{"historyId": "20", "history": []any{}})
		case "/messages":
			listCalls.Add(1)
			if req.URL.Query().Get("pageToken") == "" {
				json.NewEncoder(w).Encode(map[string]any{"messages": []any{map[string]string{"id": "m1"}}, "nextPageToken": "page-two"})
			} else {
				json.NewEncoder(w).Encode(map[string]any{"messages": []any{map[string]string{"id": "m2"}}})
			}
		case "/messages/m1", "/messages/m2":
			id := strings.TrimPrefix(path, "/messages/")
			if scenario == "labels_only" {
				messageCalls.Add(1)
			}
			if id == "m1" && scenario == "provider_deleted" {
				w.WriteHeader(404)
				return
			}
			if id == "m2" && scenario == "failed_id_retry" {
				messageCalls.Add(1)
				if failed.Load() {
					w.WriteHeader(503)
					return
				}
			}
			mime := "From: sender@example.test\r\nSubject: Synthetic\r\n\r\n" + id + "\r\n"
			json.NewEncoder(w).Encode(map[string]string{"id": id, "raw": base64.RawURLEncoding.EncodeToString([]byte(mime)), "internalDate": "1791918000000"})
		default:
			w.WriteHeader(404)
		}
	}))
	return server
}

func seedGmail(t *testing.T, root, account string) (string, string, string) {
	t.Helper()
	state := filepath.Join(root, "state")
	spool := filepath.Join(root, "spool")
	os.Mkdir(state, 0700)
	config := filepath.Join(root, ".config/google-api")
	os.MkdirAll(config, 0700)
	token, _ := json.Marshal(map[string]any{"account": account, "access_token": "synthetic-token", "expiry_date": time.Now().Add(time.Hour).UnixMilli()})
	os.WriteFile(filepath.Join(config, "tokens-test.json"), token, 0600)
	seed := checkpoint{Version: 1, Hashes: map[string]string{}, Cursor: json.RawMessage(`{"history":"10"}`)}
	b, _ := json.Marshal(seed)
	checkpointPath := filepath.Join(state, "gmail-"+digest([]byte(account))[:16]+".json")
	os.WriteFile(checkpointPath, b, 0600)
	return state, spool, checkpointPath
}

func gmailTestAdded(scenario string) []any {
	if scenario == "labels_only" {
		return nil
	}
	return []any{map[string]any{"message": map[string]string{"id": "m1"}}, map[string]any{"message": map[string]string{"id": "m2"}}}
}
