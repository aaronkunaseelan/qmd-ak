package fetch

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type waPair struct {
	LID string `json:"lid"`
	PN  string `json:"pn"`
}
type waDecoded struct {
	ID            string           `json:"id"`
	Chat          string           `json:"chat"`
	Sender        string           `json:"sender"`
	PushName      string           `json:"push_name,omitempty"`
	Timestamp     time.Time        `json:"timestamp"`
	Edited        *time.Time       `json:"edited,omitempty"`
	Text          string           `json:"text"`
	QuotedID      string           `json:"quoted_message_id,omitempty"`
	Media         []map[string]any `json:"media,omitempty"`
	Target        string           `json:"target,omitempty"`
	Reaction      string           `json:"reaction,omitempty"`
	Status        bool             `json:"status_broadcast"`
	FromMe        bool             `json:"from_me"`
	Kind          string           `json:"kind"`
	Pairs         []waPair         `json:"lid_pn_pairs,omitempty"`
	UnknownFields int              `json:"unknown_fields,omitempty"`
	LowerFidelity bool             `json:"lower_fidelity,omitempty"`
}

func waString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if x, ok := m[k]; ok {
			switch v := x.(type) {
			case string:
				return v
			case float64:
				return strconv.FormatInt(int64(v), 10)
			}
		}
	}
	return ""
}
func waMap(m map[string]any, k string) map[string]any { v, _ := m[k].(map[string]any); return v }
func waJID(j, suffix string) string {
	if j != "" && !strings.Contains(j, "@") {
		return j + suffix
	}
	return j
}
func waAddPair(pairs map[string]string, a, b string) {
	if strings.HasSuffix(a, "@lid") && strings.HasSuffix(b, "@s.whatsapp.net") {
		pairs[a] = b
	}
	if strings.HasSuffix(b, "@lid") && strings.HasSuffix(a, "@s.whatsapp.net") {
		pairs[b] = a
	}
}
func waCanonical(j string, pairs map[string]string) string {
	if p := pairs[j]; p != "" {
		return p
	}
	return j
}
func waDecode(info map[string]any, msg *e2e.Message, pairs map[string]string) (waDecoded, error) {
	d := waDecoded{ID: waString(info, "ID", "id"), Chat: waString(info, "Chat", "chat"), Sender: waString(info, "Sender", "sender"), PushName: waString(info, "PushName", "push_name"), Kind: "message"}
	d.FromMe, _ = info["IsFromMe"].(bool)
	d.Timestamp, _ = time.Parse(time.RFC3339Nano, waString(info, "Timestamp", "timestamp"))
	if d.Timestamp.IsZero() {
		if n, _ := strconv.ParseInt(waString(info, "Timestamp", "timestamp"), 10, 64); n > 0 {
			d.Timestamp = time.Unix(n, 0).UTC()
		}
	}
	waInfoPairs(info, pairs)
	d.Chat = waCanonical(d.Chat, pairs)
	d.Sender = waCanonical(d.Sender, pairs)
	d.Status = d.Chat == "status@broadcast"
	relevant := map[string]bool{}
	for _, key := range []string{"Chat", "Sender", "SenderAlt", "RecipientAlt", "ChatAlt"} {
		relevant[waString(info, key)] = true
	}
	for l, p := range pairs {
		if relevant[l] || relevant[p] {
			d.Pairs = append(d.Pairs, waPair{l, p})
		}
	}
	sort.Slice(d.Pairs, func(i, j int) bool { return d.Pairs[i].LID < d.Pairs[j].LID })

	if d.ID == "" || d.Chat == "" {
		return d, fmt.Errorf("whatsapp: message identity missing")
	}
	if msg == nil {
		d.Kind = "binary"
		d.LowerFidelity = true
		return d, nil
	}
	d.UnknownFields = len(msg.ProtoReflect().GetUnknown())
	raw, err := protojson.Marshal(msg)
	if err != nil {
		return d, fmt.Errorf("whatsapp: message decode failed")
	}
	var m map[string]any
	json.Unmarshal(raw, &m)
	m = waUnwrap(m)
	d.Text = waString(m, "conversation")
	if x := waMap(m, "extendedTextMessage"); x != nil {
		d.Text = waString(x, "text")
		d.QuotedID = waString(waMap(x, "contextInfo"), "stanzaID")
	}
	waMedia(&d, m)
	waControls(&d, info, m, pairs)
	return d, nil
}
func waUnwrap(m map[string]any) map[string]any {
	for depth := 0; depth < 8; depth++ {
		found := false
		for _, k := range []string{"ephemeralMessage", "viewOnceMessage", "viewOnceMessageV2", "viewOnceMessageV2Extension", "documentWithCaptionMessage", "editedMessage"} {
			if x := waMap(m, k); x != nil {
				if y := waMap(x, "message"); y != nil {
					m = y
					found = true
					break
				}
			}
		}
		if !found {
			break
		}
	}
	return m
}

func waMedia(d *waDecoded, m map[string]any) {
	for _, k := range []string{"imageMessage", "audioMessage", "videoMessage", "documentMessage", "stickerMessage"} {
		if x := waMap(m, k); x != nil {
			media := map[string]any{"type": strings.TrimSuffix(k, "Message")}
			for _, field := range []string{"mimetype", "fileLength", "fileSHA256", "seconds", "directPath", "mediaKey", "URL", "fileName", "PTT", "caption", "fileEncSHA256", "mediaKeyTimestamp"} {
				if v, ok := x[field]; ok {
					media[field] = v
				}
			}
			d.Media = append(d.Media, media)
			if d.Text == "" {
				d.Text = waString(x, "caption")
			}
			if d.QuotedID == "" {
				d.QuotedID = waString(waMap(x, "contextInfo"), "stanzaID")
			}
		}
	}
}

func waControls(d *waDecoded, info, m map[string]any, pairs map[string]string) {
	if x := waMap(m, "reactionMessage"); x != nil {
		d.Kind = "reaction"
		d.Reaction = waString(x, "text")
		d.Target = waString(waMap(x, "key"), "ID")
	}
	if x := waMap(m, "protocolMessage"); x != nil {
		d.Kind = "protocol"
		d.Target = waString(waMap(x, "key"), "ID")
		if edit := waMap(x, "editedMessage"); edit != nil {
			b, _ := json.Marshal(edit)
			var edited e2e.Message
			if protojson.Unmarshal(b, &edited) == nil {
				next, e := waDecode(info, &edited, pairs)
				if e == nil {
					d.Text = next.Text
					d.Media = next.Media
					d.Kind = "edit"
				}
			}
		}
		if n, _ := strconv.ParseInt(waString(x, "timestampMS"), 10, 64); n > 0 {
			t := time.UnixMilli(n).UTC()
			d.Edited = &t
		}
	}
	if n, _ := strconv.ParseInt(waString(info, "NativeEditTimestampMS"), 10, 64); n > 0 {
		t := time.UnixMilli(n).UTC()
		d.Edited = &t
	}
}

func waProto(encoded string) (*e2e.Message, error) {
	b, e := base64.StdEncoding.DecodeString(encoded)
	if e != nil {
		return nil, fmt.Errorf("whatsapp: invalid protobuf encoding")
	}
	var m e2e.Message
	if proto.Unmarshal(b, &m) != nil {
		return nil, fmt.Errorf("whatsapp: invalid message protobuf")
	}
	return &m, nil
}

// Each alternate address binds only its counterpart: SenderAlt to Sender, ChatAlt to Chat,
// RecipientAlt to Chat for outgoing messages.
func waInfoPairs(info map[string]any, pairs map[string]string) {
	waAddPair(pairs, waString(info, "Sender"), waString(info, "SenderAlt"))
	waAddPair(pairs, waString(info, "Chat"), waString(info, "ChatAlt"))
	if mine, _ := info["IsFromMe"].(bool); mine {
		waAddPair(pairs, waString(info, "Chat"), waString(info, "RecipientAlt"))
	}
}
