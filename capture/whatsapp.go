package fetch

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	hs "go.mau.fi/whatsmeow/proto/waHistorySync"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type waRecord struct {
	Kind     string         `json:"kind"`
	Info     map[string]any `json:"info"`
	Proto    string         `json:"proto_b64"`
	Received time.Time      `json:"received_at"`
}
type waPosition struct {
	Inode  uint64 `json:"inode"`
	Offset int64  `json:"offset"`
}
type waCursor struct {
	PairsVersion   int                     `json:"pairs_version"`
	Objects        map[string]int64        `json:"objects,omitempty"`
	ObjectFailures map[string]inventoryRow `json:"object_failures,omitempty"`
	Segments       map[string]waPosition   `json:"segments"`
	Pairs          map[string]string       `json:"pairs"`
	Legacy         map[string]int64        `json:"legacy,omitempty"`
	Connection     time.Time               `json:"connection"`
}

func waFail(r *run, e error) error { r.receipt.Enumerated++; r.receipt.Failed++; return e }
func waHistory(rec waRecord) (*hs.HistorySync, bool, int, error) {
	var h hs.HistorySync
	if rec.Proto != "" {
		b, e := base64.StdEncoding.DecodeString(rec.Proto)
		if e != nil || proto.Unmarshal(b, &h) != nil {
			return nil, false, 0, fmt.Errorf("whatsapp: invalid history protobuf")
		}
		return &h, false, len(h.ProtoReflect().GetUnknown()), nil
	}
	data := rec.Info["Data"]
	if data == nil {
		return nil, true, 0, fmt.Errorf("whatsapp: missing history data")
	}
	b, _ := json.Marshal(data)
	unknown := 0
	if protojson.Unmarshal(b, &h) != nil {
		unknown = waUnknownJSON(data, h.ProtoReflect().Descriptor())
		if (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(b, &h) != nil {
			return nil, true, unknown, fmt.Errorf("whatsapp: invalid history JSON")
		}
	}
	return &h, true, unknown, nil
}
func waHistoryPairs(h *hs.HistorySync, pairs map[string]string) {
	for _, p := range h.GetPhoneNumberToLidMappings() {
		waAddPair(pairs, waJID(p.GetLidJID(), "@lid"), waJID(p.GetPnJID(), "@s.whatsapp.net"))
	}
	for _, c := range h.GetConversations() {
		waAddPair(pairs, waJID(c.GetLidJID(), "@lid"), waJID(c.GetPnJID(), "@s.whatsapp.net"))
	}
}
func waEmit(r *run, raw any, d waDecoded, fromMeKnown ...bool) error {
	db, _ := json.Marshal(d)
	var decoded map[string]any
	json.Unmarshal(db, &decoded)
	if len(fromMeKnown) > 0 && !fromMeKnown[0] {
		delete(decoded, "from_me")
	}
	aliases := map[string]string{}
	for _, p := range d.Pairs {
		aliases[p.LID] = p.PN
	}
	contacts := map[string]string{}
	if d.Sender != "" && d.PushName != "" {
		contacts[d.Sender] = d.PushName
	}
	update := map[string]any{"aliases": aliases, "contacts": contacts}
	b, e := json.Marshal(struct {
		Raw     any            `json:"raw"`
		Decoded map[string]any `json:"decoded"`
		Context map[string]any `json:"ctx_update"`
	}{raw, decoded, update})
	if e != nil {
		return waFail(r, fmt.Errorf("whatsapp: encode failed"))
	}
	if d.LowerFidelity {
		r.receipt.LowerFidelity++
	}
	r.receipt.UnknownFields += d.UnknownFields
	head := waMessageHead(d)
	return r.emit(d.Chat+":"+d.ID, b, ObservationState{Version: "whatsapp-decoded-v1"}, head)
}
func waMessageHead(d waDecoded) time.Time {
	head := d.Timestamp
	if d.Edited != nil && d.Edited.After(head) {
		head = *d.Edited
	}
	return head
}
func waUnfold(ctx context.Context, r *run, h *hs.HistorySync, hash string, pairs map[string]string, lower bool, unknown int, ordinal int) error {
	names := map[string]string{}
	for _, n := range h.GetPushnames() {
		names[waCanonical(n.GetID(), pairs)] = n.GetPushname()
	}
	for _, c := range h.GetConversations() {
		for _, row := range c.GetMessages() {
			if e := ctx.Err(); e != nil {
				return e
			}
			m := row.GetMessage()
			if m == nil || m.GetKey() == nil {
				return waFail(r, fmt.Errorf("whatsapp: history message missing"))
			}
			k := m.GetKey()
			chat := k.GetRemoteJID()
			if chat == "" {
				chat = c.GetID()
			}
			sender := m.GetParticipant()
			if sender == "" {
				sender = k.GetParticipant()
			}
			if sender == "" && !k.GetFromMe() {
				sender = chat
			}
			info := map[string]any{"ID": k.GetID(), "Chat": chat, "Sender": sender, "IsFromMe": k.GetFromMe(), "Timestamp": time.Unix(int64(m.GetMessageTimestamp()), 0).UTC().Format(time.RFC3339Nano), "PushName": m.GetPushName()}
			if info["PushName"] == "" {
				info["PushName"] = names[waCanonical(sender, pairs)]
			}
			d, e := waDecode(info, m.GetMessage(), pairs)
			if e != nil {
				return waFail(r, e)
			}
			d.LowerFidelity = lower
			d.UnknownFields += unknown
			ref := map[string]any{"history_sha256": hash, "ordinal": ordinal}
			if e = waEmit(r, ref, d); e != nil {
				return e
			}
			ordinal++
		}
	}
	return nil
}
func waReadOnlyPairs(path string, pairs map[string]string) error {
	if path == "" {
		return nil
	}
	db, e := sql.Open("sqlite3", "file:"+filepath.ToSlash(path)+"?mode=ro&_query_only=1")
	if e != nil {
		return fmt.Errorf("whatsapp: device snapshot open failed")
	}
	defer db.Close()
	rows, e := db.Query("SELECT lid,pn FROM whatsmeow_lid_map")
	if e != nil {
		return fmt.Errorf("whatsapp: device mapping read failed")
	}
	defer rows.Close()
	for rows.Next() {
		var l, p string
		if rows.Scan(&l, &p) != nil {
			return fmt.Errorf("whatsapp: device mapping invalid")
		}
		waAddPair(pairs, waJID(l, "@lid"), waJID(p, "@s.whatsapp.net"))
	}
	return rows.Err()
}
func waLine(rd *bufio.Reader) ([]byte, error) {
	var out []byte
	for {
		b, e := rd.ReadSlice('\n')
		if len(out)+len(b) > 64<<20 {
			return nil, fmt.Errorf("whatsapp: journal line too large")
		}
		out = append(out, b...)
		if e == bufio.ErrBufferFull {
			continue
		}
		return out, e
	}
}
func runWhatsApp(ctx context.Context, r *run) error {
	// Source head tracks item time, excluding journal activity and archives.
	r.state.Head = r.state.NewestTime
	c := waCursor{}
	if len(r.state.Cursor) > 0 && json.Unmarshal(r.state.Cursor, &c) != nil {
		return fmt.Errorf("whatsapp: invalid cursor")
	}
	if c.Segments == nil {
		c.Segments = map[string]waPosition{}
	}
	if c.PairsVersion != 2 || c.Pairs == nil {
		c.Pairs = map[string]string{}
		c.PairsVersion = 2
	}
	if e := waReadOnlyPairs(r.device, c.Pairs); e != nil {
		return e
	}
	if e := waLoadContext(ctx, r, &c); e != nil {
		return e
	}
	if r.objects != "" {
		return runObjects(ctx, r, &c)
	}
	if r.legacy != "" {
		return runLegacy(ctx, r, &c)
	}
	if r.history != "" {
		return waImportHistory(ctx, r, &c)
	}
	root := r.journal
	if root == "" {
		home, _ := os.UserHomeDir()
		root = filepath.Join(home, ".local/share/qmd/wa-meow/journal")
	}
	if st, e := os.Stat(filepath.Join(root, "journal")); e == nil && st.IsDir() {
		root = filepath.Join(root, "journal")
	}
	names, e := filepath.Glob(filepath.Join(root, "*.jsonl"))
	if e != nil || len(names) == 0 {
		return fmt.Errorf("whatsapp: journal unavailable")
	}
	sort.Strings(names)
	if e = waJournalPairs(r, names, c.Pairs); e != nil {
		return e
	}
	for _, name := range names {
		if e := ctx.Err(); e != nil {
			return e
		}
		if e = waReadSegment(ctx, r, &c, name); e != nil {
			return e
		}
	}
	r.receipt.ConnectionHead = c.Connection
	r.receipt.HeadComplete = true
	return waSave(r, &c)
}

func waSave(r *run, c *waCursor) error {
	b, _ := json.Marshal(c)
	r.state.Cursor = b
	return r.save()
}

func waImportHistory(ctx context.Context, r *run, c *waCursor) error {
	b, e := os.ReadFile(r.history)
	if e != nil {
		return fmt.Errorf("whatsapp: history object read failed")
	}

	var h hs.HistorySync
	if proto.Unmarshal(b, &h) == nil && len(h.GetConversations()) > 0 {
		waHistoryPairs(&h, c.Pairs)
		hash, err := waArchiveHistory(r, b, c.Pairs, waHistoryContext(&h, c.Pairs))
		if err != nil {
			return err
		}
		if e = waUnfold(ctx, r, &h, hash, c.Pairs, false, len(h.ProtoReflect().GetUnknown()), 0); e != nil {
			return e
		}
		return waSave(r, c)
	}
	return waImportHistoryJSON(ctx, r, c, b)
}

func waImportHistoryJSON(ctx context.Context, r *run, c *waCursor, b []byte) error {
	var e error
	type historyRecord struct {
		history *hs.HistorySync
		lower   bool
		unknown int
	}
	var records []historyRecord
	rd := bufio.NewReaderSize(bytes.NewReader(b), 64*1024)
	for {
		line, e := waLine(rd)
		if e == io.EOF && len(line) == 0 {
			break
		}
		if e != nil && e != io.EOF {
			return e
		}
		var rec waRecord
		if json.Unmarshal(line, &rec) != nil {
			return waFail(r, fmt.Errorf("whatsapp: invalid history line"))
		}
		if rec.Kind == "history_sync" {
			h, lower, unknown, e := waHistory(rec)
			if e != nil {
				return waFail(r, e)
			}
			waHistoryPairs(h, c.Pairs)
			records = append(records, historyRecord{h, lower, unknown})
		}
	}
	contextUpdate := waPairsContext(c.Pairs)
	for _, rec := range records {
		waMergeContext(contextUpdate, waHistoryContext(rec.history, c.Pairs))
	}
	hash, err := waArchiveHistory(r, b, c.Pairs, contextUpdate)
	if err != nil {
		return err
	}
	ordinal := 0
	for _, rec := range records {
		if e = waUnfold(ctx, r, rec.history, hash, c.Pairs, rec.lower, rec.unknown, ordinal); e != nil {
			return e
		}
		for _, conv := range rec.history.GetConversations() {
			ordinal += len(conv.GetMessages())
		}
	}
	return waSave(r, c)
}

func waJournalPairs(r *run, names []string, pairs map[string]string) error {
	for _, name := range names {
		f, e := os.Open(name)
		if e != nil {
			return fmt.Errorf("whatsapp: journal read failed")
		}
		rd := bufio.NewReader(f)
		for {
			line, e := waLine(rd)
			if e == io.EOF {
				break
			}
			if e != nil {
				f.Close()
				return e
			}
			var rec waRecord
			if json.Unmarshal(line, &rec) != nil {
				f.Close()
				return waFail(r, fmt.Errorf("whatsapp: invalid journal line"))
			}
			if rec.Kind == "header" {
				account := waString(rec.Info, "account_jid")
				if r.account == "default" && account != "" {
					r.account = account
				}
				if account != "" && account != r.account {
					f.Close()
					return fmt.Errorf("whatsapp: account mismatch")
				}
			}
			waInfoPairs(rec.Info, pairs)
			if rec.Kind == "history_sync" {
				h, _, _, err := waHistory(rec)
				if err != nil {
					f.Close()
					return waFail(r, err)
				}
				waHistoryPairs(h, pairs)
			}
		}
		f.Close()
	}
	return nil
}

func waReadSegment(ctx context.Context, r *run, c *waCursor, name string) error {
	f, e := os.Open(name)
	if e != nil {
		return fmt.Errorf("whatsapp: segment open failed")
	}
	st, e := f.Stat()
	if e != nil {
		f.Close()
		return e
	}
	inode := st.Sys().(*syscall.Stat_t).Ino
	base := filepath.Base(name)
	pos := c.Segments[base]
	if pos.Inode != 0 && pos.Inode != inode {
		f.Close()
		return fmt.Errorf("whatsapp: segment replaced")
	}
	if st.Size() < pos.Offset {
		f.Close()
		return fmt.Errorf("whatsapp: segment truncated")
	}
	f.Seek(pos.Offset, io.SeekStart)
	rd := bufio.NewReader(f)
	batch := 0
	for {
		line, e := waLine(rd)
		if e == io.EOF {
			break
		}
		if e != nil {
			f.Close()
			return waFail(r, e)
		}
		var rec waRecord
		if json.Unmarshal(line, &rec) != nil {
			f.Close()
			return waFail(r, fmt.Errorf("whatsapp: invalid journal record"))
		}
		if e = waJournalRecord(ctx, r, c, line, rec); e != nil {
			f.Close()
			return e
		}
		pos = waPosition{inode, pos.Offset + int64(len(line))}
		c.Segments[base] = pos
		r.receipt.ConnectionHead = c.Connection
		batch++
		if batch%100 == 0 {
			if e = waSave(r, c); e != nil {
				f.Close()
				return e
			}
		}
	}
	f.Close()
	return waSave(r, c)
}

func waJournalRecord(ctx context.Context, r *run, c *waCursor, line []byte, rec waRecord) error {
	switch rec.Kind {
	case "message":
		m, e := waProto(rec.Proto)
		if e != nil {
			return waFail(r, e)
		}
		d, e := waDecode(rec.Info, m, c.Pairs)
		if e != nil {
			return waFail(r, e)
		}
		if e = waEmit(r, json.RawMessage(line), d); e != nil {
			return e
		}
	case "history_sync":
		h, lower, unknown, e := waHistory(rec)
		if e != nil {
			return waFail(r, e)
		}
		hash, err := waArchiveHistory(r, line, c.Pairs, waHistoryContext(h, c.Pairs))
		if err != nil {
			return err
		}
		if e = waUnfold(ctx, r, h, hash, c.Pairs, lower, unknown, 0); e != nil {
			return e
		}
	default:
		handled, err := waEmitContext(r, line, rec, c.Pairs)
		if err != nil {
			return err
		}
		if !handled {
			r.receipt.Enumerated++
			r.receipt.Excluded++
		}
		if rec.Received.After(c.Connection) {
			c.Connection = rec.Received
		}
	}
	return nil
}

func waUnknownJSON(value any, desc protoreflect.MessageDescriptor) int {
	m, ok := value.(map[string]any)
	if !ok {
		return 0
	}
	n := 0
	for key, v := range m {
		var field protoreflect.FieldDescriptor
		fields := desc.Fields()
		for i := 0; i < fields.Len(); i++ {
			f := fields.Get(i)
			if key == f.JSONName() || key == string(f.Name()) {
				field = f
				break
			}
		}
		if field == nil {
			n++
			continue
		}
		if field.Kind() != protoreflect.MessageKind {
			continue
		}
		if field.IsList() {
			if rows, ok := v.([]any); ok {
				for _, row := range rows {
					n += waUnknownJSON(row, field.Message())
				}
			}
		} else {
			n += waUnknownJSON(v, field.Message())
		}
	}
	return n
}

func waArchiveHistory(r *run, original []byte, pairs map[string]string, update map[string]any) (string, error) {
	hash := digest(original)
	metadata := ObservationState{Version: "whatsapp-original-v1", Labels: map[string]string{"archive_only": "true", "object_hash": hash, "size": strconv.Itoa(len(original)), "source": r.source}}
	if e := r.emit("history:"+hash, original, metadata, time.Time{}); e != nil {
		return "", e
	}
	all := make([]waPair, 0, len(pairs))
	for lid, pn := range pairs {
		all = append(all, waPair{lid, pn})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].LID < all[j].LID })
	payload, e := json.Marshal(struct {
		Raw     map[string]string `json:"raw"`
		Decoded waDecoded         `json:"decoded"`
		Context map[string]any    `json:"ctx_update"`
	}{map[string]string{"history_sha256": hash}, waDecoded{Kind: "history_source", Pairs: all}, update})
	if e != nil {
		return "", waFail(r, fmt.Errorf("whatsapp: history context encode failed"))
	}
	if e = r.emit("history_context:"+hash, payload, ObservationState{Version: "whatsapp-decoded-v1"}, time.Time{}); e != nil {
		return "", e
	}
	return hash, nil
}

func waPairsContext(pairs map[string]string) map[string]any {
	return map[string]any{"aliases": pairs, "contacts": map[string]string{}, "groups": map[string]string{}}
}
func waMergeContext(dst, src map[string]any) {
	for _, kind := range []string{"aliases", "contacts", "groups"} {
		a, _ := dst[kind].(map[string]string)
		b, _ := src[kind].(map[string]string)
		for k, v := range b {
			a[k] = v
		}
	}
}
func waHistoryContext(h *hs.HistorySync, pairs map[string]string) map[string]any {
	out := waPairsContext(pairs)
	contacts := out["contacts"].(map[string]string)
	groups := out["groups"].(map[string]string)
	if h == nil {
		return out
	}
	for _, n := range h.GetPushnames() {
		if n.GetPushname() != "" {
			contacts[waCanonical(n.GetID(), pairs)] = n.GetPushname()
		}
	}
	for _, c := range h.GetConversations() {
		if c.GetName() != "" {
			groups[waCanonical(c.GetID(), pairs)] = c.GetName()
		}
	}
	return out
}
