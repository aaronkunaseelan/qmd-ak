package fetch

import (
	"encoding/json"
	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	"google.golang.org/protobuf/encoding/protojson"
	"testing"
)

func TestWhatsAppDecoderTable(t *testing.T) {
	rows := []decoderCase{
		{name: "text", raw: `{"conversation":"hello"}`, text: "hello", kind: "message"},
		{name: "quoted", raw: `{"extendedTextMessage":{"text":"reply","contextInfo":{"stanzaID":"parent"}}}`, text: "reply", quoted: "parent", kind: "message"},
		{name: "voice", raw: `{"audioMessage":{"mimetype":"audio/ogg","fileLength":"42","fileSHA256":"AQI=","seconds":3,"directPath":"/media","mediaKey":"AwQ=","PTT":true}}`, kind: "message", media: true},
		{name: "status broadcast", raw: `{"conversation":"status"}`, text: "status", kind: "message", status: true},
		{name: "reaction", raw: `{"reactionMessage":{"key":{"ID":"parent"},"text":"+1"}}`, kind: "reaction", target: "parent"},
		{name: "revoke", raw: `{"protocolMessage":{"key":{"ID":"parent"},"type":"REVOKE"}}`, kind: "protocol", target: "parent"},
		{name: "protocol context", raw: `{"protocolMessage":{"type":"HISTORY_SYNC_NOTIFICATION"}}`, kind: "protocol"},
		{name: "edit", raw: `{"protocolMessage":{"key":{"ID":"parent"},"editedMessage":{"conversation":"changed"},"timestampMS":"1700000000000"}}`, text: "changed", kind: "edit", target: "parent"},
		{name: "later edit", raw: `{"protocolMessage":{"key":{"ID":"parent"},"editedMessage":{"conversation":"changed"},"timestampMS":"1893456000000"}}`, text: "changed", kind: "edit", target: "parent"},
		{name: "wrapper", raw: `{"ephemeralMessage":{"message":{"viewOnceMessage":{"message":{"conversation":"wrapped"}}}}}`, text: "wrapped", kind: "message"},
		{name: "alternate recipients", raw: `{"conversation":"addresses"}`, text: "addresses", kind: "message", alternate: true},
		{name: "unknown", raw: `{"conversation":"forward"}`, text: "forward", kind: "message", unknown: true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			var msg e2e.Message
			if e := protojson.Unmarshal([]byte(row.raw), &msg); e != nil {
				t.Fatal(e)
			}
			if row.unknown {
				msg.ProtoReflect().SetUnknown([]byte{0xf8, 0x3f, 0x01})
			}
			info := map[string]any{"ID": "m", "Chat": "1@lid", "Sender": "2@lid", "Timestamp": "2026-10-13T00:00:00Z"}
			if row.status {
				info["Chat"] = "status@broadcast"
			}
			if row.alternate {
				info["SenderAlt"] = "22@s.whatsapp.net"
				info["RecipientAlt"] = "999@lid"
			}
			pairs := map[string]string{"1@lid": "11@s.whatsapp.net", "2@lid": "22@s.whatsapp.net", "3@lid": "33@s.whatsapp.net"}
			d, e := waDecode(info, &msg, pairs)
			if e != nil {
				t.Fatal(e)
			}
			checkDecoded(t, row, d, pairs)
		})
	}
}

type decoderCase struct {
	name, raw, text, quoted, kind, target string
	media                                 bool
	unknown                               bool
	alternate                             bool
	status                                bool
}

func checkDecoded(t *testing.T, row decoderCase, d waDecoded, pairs map[string]string) {
	t.Helper()
	if d.Text != row.text || d.QuotedID != row.quoted || d.Kind != row.kind || d.Target != row.target {
		t.Fatalf("unexpected decoded fields: %#v", d)
	}
	chat, pairCount := "11@s.whatsapp.net", 2
	if row.status {
		chat, pairCount = "status@broadcast", 1
	}
	if d.Chat != chat || d.Status != row.status || d.Sender != "22@s.whatsapp.net" || len(d.Pairs) != pairCount {
		t.Fatal("canonicalization or relevant mapping failed")
	}
	if row.alternate && pairs["999@lid"] != "" {
		t.Fatal("incoming recipient wrongly paired with peer sender or chat")
	}
	if row.media {
		if len(d.Media) != 1 {
			t.Fatal("media missing")
		}
		for _, key := range []string{"type", "mimetype", "fileLength", "fileSHA256", "seconds", "directPath", "mediaKey", "PTT"} {
			if _, ok := d.Media[0][key]; !ok {
				t.Fatalf("media field %s missing", key)
			}
		}
	}
	head := waMessageHead(d)
	if head.IsZero() {
		t.Fatal("captured activity is missing its head")
	}
	if !head.IsZero() {
		want := d.Timestamp
		if d.Edited != nil && d.Edited.After(want) {
			want = *d.Edited
		}
		if !head.Equal(want) {
			t.Fatal("message head does not match the document time")
		}
	}
	if row.kind == "edit" && d.Edited == nil {
		t.Fatal("edit timestamp missing")
	}
	if row.unknown && d.UnknownFields == 0 {
		t.Fatal("unknown fields uncounted")
	}
}

func TestWhatsAppHistoryJSONTable(t *testing.T) {
	for _, row := range []struct {
		name, data string
		unknown    int
	}{
		{"plain", `{"syncType":0,"conversations":[{"ID":"1@lid","messages":[]}],"phoneNumberToLidMappings":[{"pnJID":"11","lidJID":"1"}]}`, 0},
		{"unknown", `{"syncType":0,"conversations":[{"ID":"1@lid","futureField":1}],"futureTop":true}`, 2},
	} {
		t.Run(row.name, func(t *testing.T) {
			var data any
			if e := json.Unmarshal([]byte(row.data), &data); e != nil {
				t.Fatal(e)
			}
			h, lower, unknown, e := waHistory(waRecord{Info: map[string]any{"Data": data}})
			if e != nil {
				t.Fatal(e)
			}
			if !lower || unknown != row.unknown || len(h.GetConversations()) != 1 {
				t.Fatalf("conversion counts mismatch: lower=%v unknown=%d", lower, unknown)
			}
		})
	}
}
