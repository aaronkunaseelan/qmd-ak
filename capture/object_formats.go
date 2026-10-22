package fetch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	hs "go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/proto"
)

// Object formats retain explicit native coordinates; opaque catalog hashes
// never substitute for a WhatsApp message or chat identity.
func waObjectRecords(b []byte) ([]waRecord, *hs.HistorySync, error) {
	if json.Valid(b) {
		var fields map[string]json.RawMessage
		if json.Unmarshal(b, &fields) != nil || fields == nil {
			return nil, nil, fmt.Errorf("whatsapp: object JSON must be an object")
		}
		rec, e := waObjectRecord(fields)
		if e != nil {
			return nil, nil, e
		}
		return []waRecord{rec}, nil, nil
	}
	if len(bytes.TrimSpace(b)) > 0 && bytes.TrimSpace(b)[0] == '{' {
		rd := bufio.NewReader(bytes.NewReader(b))
		var records []waRecord
		for {
			line, e := waLine(rd)
			if e == io.EOF && len(line) == 0 {
				break
			}
			if e != nil && e != io.EOF {
				return nil, nil, fmt.Errorf("whatsapp: object JSONL read failed")
			}
			var fields map[string]json.RawMessage
			if json.Unmarshal(line, &fields) != nil || fields == nil {
				return nil, nil, fmt.Errorf("whatsapp: object JSONL record invalid")
			}
			rec, e := waObjectRecord(fields)
			if e != nil {
				return nil, nil, e
			}
			records = append(records, rec)
		}
		return records, nil, nil
	}
	var h hs.HistorySync
	if proto.Unmarshal(b, &h) == nil && (len(h.GetConversations()) > 0 || len(h.GetPhoneNumberToLidMappings()) > 0 || len(h.GetPushnames()) > 0) {
		return nil, &h, nil
	}
	return nil, nil, nil
}
func waObjectRecord(fields map[string]json.RawMessage) (waRecord, error) {
	b, _ := json.Marshal(fields)
	var rec waRecord
	if _, ok := fields["kind"]; ok {
		if json.Unmarshal(b, &rec) != nil || rec.Kind == "" {
			return rec, fmt.Errorf("whatsapp: object journal envelope invalid")
		}
		waNormalizeInfo(rec.Info)
		return rec, nil
	}
	if _, ok := fields["wa_id"]; ok {
		var tr struct {
			ID        string `json:"wa_id"`
			Chat      string `json:"chat_jid"`
			Sender    string `json:"sender_jid"`
			Name      string `json:"sender_name"`
			Timestamp int64  `json:"timestamp"`
			Raw       []byte `json:"raw_proto"`
		}
		if json.Unmarshal(b, &tr) != nil || len(tr.Raw) == 0 {
			return rec, fmt.Errorf("whatsapp: object transport record invalid")
		}
		return waRecord{Kind: "message", Proto: base64.StdEncoding.EncodeToString(tr.Raw), Info: map[string]any{"ID": tr.ID, "Chat": tr.Chat, "Sender": tr.Sender, "PushName": tr.Name, "Timestamp": time.Unix(tr.Timestamp, 0).UTC().Format(time.RFC3339Nano)}}, nil
	}
	if _, ok := fields["conversations"]; ok {
		var data any
		if json.Unmarshal(b, &data) != nil {
			return rec, fmt.Errorf("whatsapp: object history JSON invalid")
		}
		return waRecord{Kind: "history_sync", Info: map[string]any{"Data": data}}, nil
	}
	return rec, fmt.Errorf("whatsapp: unsupported object JSON envelope")
}
func waNormalizeInfo(info map[string]any) {
	for _, key := range []string{"Chat", "Sender", "SenderAlt", "RecipientAlt", "ChatAlt", "JID", "JIDAlt", "SenderPN"} {
		if obj, ok := info[key].(map[string]any); ok {
			user, server := waString(obj, "User"), waString(obj, "Server")
			if user != "" && server != "" {
				info[key] = user + "@" + server
			}
		}
	}
}
func waGatherObjectPairs(b []byte, pairs map[string]string) error {
	records, h, e := waObjectRecords(b)
	if e != nil {
		return e
	}
	if h != nil {
		waHistoryPairs(h, pairs)
		return nil
	}
	for _, rec := range records {
		if rec.Kind == "history_sync" {
			h, _, _, e := waHistory(rec)
			if e != nil {
				return e
			}
			waHistoryPairs(h, pairs)
		}
		waRecordContext(rec, pairs)
	}
	return nil
}
func waCaptureObject(ctx context.Context, r *run, c *waCursor, b []byte, row inventoryRow) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if len(b) == 0 {
		return fmt.Errorf("whatsapp: empty object")
	}
	if row.ObjectHash != "" && digest(b) != row.ObjectHash {
		return fmt.Errorf("whatsapp: object hash mismatch")
	}
	records, h, e := waObjectRecords(b)
	if e != nil {
		return e
	}
	if e = waGatherObjectPairs(b, c.Pairs); e != nil {
		return e
	}
	archivePairs, update, e := waObjectContext(records, h, c.Pairs)
	if e != nil {
		return e
	}
	// Bare protobuf requires explicit metadata retained in the catalog lineage.
	if h == nil && len(records) == 0 {
		info := waObjectNativeInfo(row)
		var msg e2e.Message
		if proto.Unmarshal(b, &msg) != nil || len(msg.ProtoReflect().GetUnknown()) == len(b) {
			return fmt.Errorf("whatsapp: unsupported object protobuf")
		}
		if waString(info, "ID") == "" || waString(info, "Chat") == "" {
			return fmt.Errorf("whatsapp: object native identity missing")
		}
		records = []waRecord{{Kind: "message", Info: info, Proto: base64.StdEncoding.EncodeToString(b)}}
	}
	if archivePairs != nil && len(archivePairs) == 0 {
		archivePairs = update["aliases"].(map[string]string)
	}
	hash, e := waArchiveHistory(r, b, archivePairs, update)
	if e != nil {
		return e
	}
	if h != nil {
		return waUnfold(ctx, r, h, hash, c.Pairs, false, len(h.ProtoReflect().GetUnknown()), 0)
	}
	return waObjectChildren(ctx, r, c, records, hash)
}

func waObjectContext(records []waRecord, h *hs.HistorySync, pairs map[string]string) (map[string]string, map[string]any, error) {
	archivePairs := map[string]string{}
	update := waPairsContext(archivePairs)
	if h != nil {
		archivePairs = pairs
		update = waHistoryContext(h, pairs)
	} else {
		for _, rec := range records {
			waMergeContext(update, waRecordContext(rec, pairs))
			if rec.Kind == "history_sync" {
				archivePairs = pairs
				batch, _, _, err := waHistory(rec)
				if err != nil {
					return nil, nil, err
				}
				waMergeContext(update, waHistoryContext(batch, pairs))
			}
		}
	}
	return archivePairs, update, nil
}

func waObjectChildren(ctx context.Context, r *run, c *waCursor, records []waRecord, hash string) error {
	var e error
	ordinal := 0
	for _, rec := range records {
		if e = ctx.Err(); e != nil {
			return e
		}
		switch rec.Kind {
		case "history_sync":
			batch, lower, unknown, err := waHistory(rec)
			if err != nil {
				return err
			}
			if e = waUnfold(ctx, r, batch, hash, c.Pairs, lower, unknown, ordinal); e != nil {
				return e
			}
			for _, conv := range batch.GetConversations() {
				ordinal += len(conv.GetMessages())
			}
		case "message":
			msg, err := waProto(rec.Proto)
			if err != nil {
				return err
			}
			d, err := waDecode(rec.Info, msg, c.Pairs)
			if err != nil {
				return err
			}
			_, known := rec.Info["IsFromMe"]
			if e = waEmit(r, map[string]any{"history_sha256": hash, "ordinal": ordinal}, d, known); e != nil {
				return e
			}
			ordinal++
		default: // Non-message receiver events remain in the archived object.
		}
	}
	return nil
}
func waObjectNativeInfo(row inventoryRow) map[string]any {
	out := map[string]any{}
	for _, key := range []string{"info", "identity", "Identity"} {
		if m, ok := row.LineageJSON[key].(map[string]any); ok {
			for k, v := range m {
				out[k] = v
			}
		}
	}
	for dst, keys := range map[string][]string{"ID": {"ID", "id", "wa_id", "message_id"}, "Chat": {"Chat", "chat", "chat_jid"}, "Sender": {"Sender", "sender", "sender_jid", "Participant"}, "PushName": {"PushName", "sender_name"}, "IsFromMe": {"IsFromMe", "FromMe", "from_me"}} {
		if _, ok := out[dst]; ok {
			continue
		}
		for _, key := range keys {
			if v, ok := out[key]; ok {
				out[dst] = v
				break
			}
			if v, ok := row.LineageJSON[key]; ok {
				out[dst] = v
				break
			}
		}
	}
	// Chat and ID come only from explicit message:<chat>/<id> keys.
	if waString(out, "ID") == "" || waString(out, "Chat") == "" {
		chat, id, ok := strings.Cut(strings.TrimPrefix(row.ExternalItemKey, "message:"), "/")
		if ok && strings.Contains(chat, "@") && id != "" && !strings.Contains(id, "/") {
			out["Chat"] = chat
			out["ID"] = id
		}
	}
	if waString(out, "Timestamp") == "" && !row.SourceAt.IsZero() {
		out["Timestamp"] = row.SourceAt.UTC().Format(time.RFC3339Nano)
	}
	waNormalizeInfo(out)
	return out
}
