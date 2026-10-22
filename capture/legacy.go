package fetch

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"time"

	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	web "go.mau.fi/whatsmeow/proto/waWeb"
	"google.golang.org/protobuf/proto"
)

func waFileHash(path string) (string, error) {
	f, e := os.Open(path)
	if e != nil {
		return "", e
	}
	defer f.Close()
	h := sha256.New()
	if _, e = io.Copy(h, f); e != nil {
		return "", e
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
func runLegacy(ctx context.Context, r *run, c *waCursor) error {
	dbHash, e := waFileHash(r.legacy)
	if e != nil {
		return fmt.Errorf("whatsapp: legacy database read failed")
	}
	walHash, e := waFileHash(r.legacy + "-wal")
	if e != nil && !os.IsNotExist(e) {
		return fmt.Errorf("whatsapp: legacy WAL read failed")
	}
	coordinate := dbHash + ":" + walHash
	if c.Legacy == nil {
		c.Legacy = map[string]int64{}
	}
	u := url.URL{Scheme: "file", Path: filepath.ToSlash(r.legacy)}
	q := u.Query()
	q.Set("mode", "ro")
	q.Set("_query_only", "1")
	u.RawQuery = q.Encode()
	db, e := sql.Open("sqlite3", u.String())
	if e != nil {
		return fmt.Errorf("whatsapp: legacy database open failed")
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	tx, e := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if e != nil {
		return fmt.Errorf("whatsapp: legacy snapshot failed")
	}
	defer tx.Rollback()
	if e = waLegacyContext(ctx, r, c, tx, dbHash, walHash, coordinate); e != nil {
		return e
	}
	if e = waLegacyMessages(ctx, r, c, tx, dbHash, walHash, coordinate); e != nil {
		return e
	}
	current, e := waFileHash(r.legacy)
	if e != nil || current != dbHash {
		return fmt.Errorf("whatsapp: legacy database changed")
	}
	current, e = waFileHash(r.legacy + "-wal")
	if e != nil && !os.IsNotExist(e) {
		return fmt.Errorf("whatsapp: legacy WAL read failed")
	}
	if current != walHash {
		return fmt.Errorf("whatsapp: legacy WAL changed")
	}
	return waSave(r, c)
}

func waLegacyContext(ctx context.Context, r *run, c *waCursor, tx *sql.Tx, dbHash, walHash, coordinate string) error {
	update := waPairsContext(c.Pairs)
	groups := update["groups"].(map[string]string)
	chats, e := tx.QueryContext(ctx, "SELECT jid,name FROM chats WHERE name IS NOT NULL AND name<>'' ORDER BY jid")
	if e != nil {
		return fmt.Errorf("whatsapp: legacy chats read failed")
	}
	for chats.Next() {
		var jid, name string
		if chats.Scan(&jid, &name) != nil {
			chats.Close()
			return fmt.Errorf("whatsapp: legacy chat invalid")
		}
		groups[waCanonical(jid, c.Pairs)] = name
	}
	e = chats.Err()
	chats.Close()
	if e != nil {
		return fmt.Errorf("whatsapp: legacy chats read failed")
	}
	ref := map[string]any{"legacy_db_sha256": dbHash, "rowid": int64(0)}
	if walHash != "" {
		ref["legacy_wal_sha256"] = walHash
	}
	marker, e := json.Marshal(map[string]any{"raw": ref, "decoded": map[string]any{"kind": "history_source"}, "ctx_update": update})
	if e != nil {
		return waFail(r, fmt.Errorf("whatsapp: legacy context encode failed"))
	}
	if e = r.emit("legacy_context:"+coordinate, marker, ObservationState{Version: "whatsapp-decoded-v1"}, time.Time{}); e != nil {
		return e
	}
	return nil
}

func waLegacyMessages(ctx context.Context, r *run, c *waCursor, tx *sql.Tx, dbHash, walHash, coordinate string) error {
	limit := r.limit
	if limit <= 0 {
		limit = 1000
	}
	rows, e := tx.QueryContext(ctx, "SELECT rowid,wa_id,chat_jid,COALESCE(sender_jid,''),COALESCE(sender_name,''),timestamp,raw_proto FROM messages WHERE rowid>? ORDER BY rowid LIMIT ?", c.Legacy[coordinate], limit+1)
	if e != nil {
		return fmt.Errorf("whatsapp: legacy messages read failed")
	}
	defer rows.Close()
	count := 0
	for rows.Next() {
		if count == limit {
			r.receipt.HasMore = true
			break
		}
		if e = ctx.Err(); e != nil {
			return e
		}
		rowid, err := waLegacyMessage(r, c, rows, dbHash, walHash)
		if err != nil {
			return err
		}
		c.Legacy[coordinate] = rowid
		count++
		if count%100 == 0 {
			if e = waSave(r, c); e != nil {
				return e
			}
		}
	}
	if rows.Err() != nil {
		return fmt.Errorf("whatsapp: legacy rows read failed")
	}
	rows.Close()
	return nil
}

func waLegacyMessage(r *run, c *waCursor, rows *sql.Rows, dbHash, walHash string) (int64, error) {
	var rowid, ts int64
	var id, chat, sender, name string
	var raw []byte
	if rows.Scan(&rowid, &id, &chat, &sender, &name, &ts, &raw) != nil {
		return 0, waFail(r, fmt.Errorf("whatsapp: legacy row invalid"))
	}
	info := map[string]any{"ID": id, "Chat": chat, "Sender": sender, "PushName": name, "Timestamp": time.Unix(ts, 0).UTC().Format(time.RFC3339Nano)}
	var msg e2e.Message
	var envelope web.WebMessageInfo
	known := false
	if len(raw) == 0 {
		return 0, waFail(r, fmt.Errorf("whatsapp: legacy protobuf missing"))
	}
	if proto.Unmarshal(raw, &envelope) == nil && envelope.GetKey() != nil && envelope.GetKey().GetID() != "" && envelope.GetMessage() != nil {
		key := envelope.GetKey()
		if key.GetID() != id {
			return 0, waFail(r, fmt.Errorf("whatsapp: legacy identity mismatch"))
		}
		info["IsFromMe"] = key.GetFromMe()
		known = true
		msg = *envelope.GetMessage()
	} else if proto.Unmarshal(raw, &msg) != nil {
		return 0, waFail(r, fmt.Errorf("whatsapp: legacy protobuf invalid"))
	}
	d, e := waDecode(info, &msg, c.Pairs)
	if e != nil {
		return 0, waFail(r, e)
	}
	ref := map[string]any{"legacy_db_sha256": dbHash, "rowid": rowid}
	if walHash != "" {
		ref["legacy_wal_sha256"] = walHash
	}
	if e = waEmit(r, ref, d, known); e != nil {
		return 0, e
	}
	return rowid, nil
}
