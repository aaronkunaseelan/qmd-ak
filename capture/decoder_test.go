package fetch

import (
	"encoding/json"
	"testing"

	"go.mau.fi/whatsmeow/proto/waCommon"
	e2e "go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
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
		{name: "device sent", raw: `{"deviceSentMessage":{"destinationJID":"1@s.whatsapp.net","message":{"extendedTextMessage":{"text":"mine","contextInfo":{"stanzaID":"parent"}}}}}`, text: "mine", quoted: "parent", kind: "message"},
		{name: "device sent voice", raw: `{"deviceSentMessage":{"message":{"audioMessage":{"mimetype":"audio/ogg","fileLength":"42","fileSHA256":"AQI=","seconds":3,"directPath":"/media","mediaKey":"AwQ=","PTT":true}}}}`, kind: "message", media: true, mediaType: "audio"},
		{name: "album child", raw: `{"associatedChildMessage":{"message":{"imageMessage":{"caption":"view"}}}}`, text: "view", kind: "message"},
		{name: "video note", raw: `{"ptvMessage":{"mimetype":"video/mp4","fileLength":"42","fileSHA256":"AQI=","seconds":3,"directPath":"/media","mediaKey":"AwQ="}}`, kind: "message", media: true, mediaType: "video"},
		{name: "location", raw: `{"locationMessage":{"degreesLatitude":1.5,"degreesLongitude":-2.25,"name":"Pier","address":"Harbour","contextInfo":{"stanzaID":"parent"}}}`, text: "[location] Pier Harbour 1.5 -2.25", quoted: "parent", kind: "message"},
		{name: "poll", raw: `{"pollCreationMessageV3":{"name":"Lunch?","options":[{"optionName":"Soup"},{"optionName":"Salad"}]}}`, text: "[poll] Lunch? Soup Salad", kind: "message"},
		{name: "album", raw: `{"albumMessage":{"expectedImageCount":2}}`, text: "[album] 2 images", kind: "message"},
		{name: "four row template", raw: `{"templateMessage":{"hydratedFourRowTemplate":{"hydratedContentText":"Your code"}}}`, text: "[template] Your code", kind: "message"},
		{name: "empty template", raw: `{"templateMessage":{"templateID":"1"}}`, kind: "binary", undecoded: "templateMessage"},
		{name: "sender key only", raw: `{"senderKeyDistributionMessage":{"groupID":"1@g.us"},"messageContextInfo":{}}`, kind: "sender_key"},
		{name: "control only", raw: `{"pinInChatMessage":{"type":"PIN_FOR_ALL"},"senderKeyDistributionMessage":{"groupID":"1@g.us"}}`, kind: "protocol"},
		{name: "edited reply", raw: `{"protocolMessage":{"key":{"ID":"parent"},"editedMessage":{"extendedTextMessage":{"text":"changed","contextInfo":{"stanzaID":"quoted"}}},"timestampMS":"1700000000000"}}`, text: "changed", quoted: "quoted", kind: "edit", target: "parent"},
		{name: "not decoded", raw: `{"placeholderMessage":{"type":"MASK_LINKED_DEVICES"}}`, kind: "binary", undecoded: "placeholderMessage"},
		{name: "edit not decoded", raw: `{"protocolMessage":{"key":{"ID":"parent"},"editedMessage":{"placeholderMessage":{}},"timestampMS":"1700000000000"}}`, kind: "protocol", target: "parent"},
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
	mediaType, undecoded                  string
	media                                 bool
	unknown                               bool
	alternate                             bool
	status                                bool
}

func checkDecoded(t *testing.T, row decoderCase, d waDecoded, pairs map[string]string) {
	t.Helper()
	if d.Text != row.text || d.QuotedID != row.quoted || d.Kind != row.kind || d.Target != row.target || d.Undecoded != row.undecoded {
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
		if len(d.Media) != 1 || (row.mediaType != "" && d.Media[0]["type"] != row.mediaType) {
			t.Fatal("media missing")
		}
		for _, key := range []string{"type", "mimetype", "fileLength", "fileSHA256", "seconds", "directPath", "mediaKey", "PTT"} {
			if _, ok := d.Media[0][key]; !ok && (key != "PTT" || row.mediaType != "video") {
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

// A message edit sent encrypted with its target's secret decodes as an ordinary edit that keeps
// the target's reply id, whichever of the target's or editor's addresses the sender used.
func TestWhatsAppSecretEdit(t *testing.T) {
	secret := []byte("0123456789abcdef0123456789abcdef")
	original := &e2e.Message{ExtendedTextMessage: &e2e.ExtendedTextMessage{Text: proto.String("before"), ContextInfo: &e2e.ContextInfo{StanzaID: proto.String("quoted")}}, MessageContextInfo: &e2e.MessageContextInfo{MessageSecret: secret}}
	pairs := map[string]string{"2@lid": "22@s.whatsapp.net"}
	originals := map[string]waOriginal{}
	waRemember(originals, "target", original, nil, "2@lid", "22@s.whatsapp.net")
	waRemember(originals, "target", &e2e.Message{SenderKeyDistributionMessage: &e2e.SenderKeyDistributionMessage{}}, nil, "2@lid")
	for _, tc := range []struct{ name, author, editor string }{{"phone numbers", "22@s.whatsapp.net", "22@s.whatsapp.net"}, {"lid", "2@lid", "2@lid"}, {"mixed", "2@lid", "22@s.whatsapp.net"}} {
		t.Run(tc.name, func(t *testing.T) {
			kind := e2e.ProtocolMessage_MESSAGE_EDIT
			key := &waCommon.MessageKey{RemoteJID: proto.String("22@s.whatsapp.net"), ID: proto.String("target")}
			plain, _ := proto.Marshal(&e2e.Message{ProtocolMessage: &e2e.ProtocolMessage{Key: key, Type: &kind, EditedMessage: &e2e.Message{Conversation: proto.String("after")}, TimestampMS: proto.Int64(1700000000000)}})
			derived := hkdfutil.SHA256(secret, nil, []byte("target"+tc.author+tc.editor+"Message Edit"), 32)
			iv := []byte("0123456789ab")
			payload, _ := gcmutil.Encrypt(derived, iv, plain, nil)
			encType := e2e.SecretEncryptedMessage_MESSAGE_EDIT
			carrier := &e2e.Message{SecretEncryptedMessage: &e2e.SecretEncryptedMessage{TargetMessageKey: key, EncPayload: payload, EncIV: iv, SecretEncType: &encType}}
			info := map[string]any{"ID": "carrier", "Chat": "2@lid", "Sender": "2:7@lid", "SenderAlt": "22:7@s.whatsapp.net", "Timestamp": "2026-10-13T00:00:00Z"}
			if d, _ := waDecode(info, carrier, pairs); d.Kind != "protocol" {
				t.Fatalf("an undecrypted carrier must stay control: %#v", d)
			}
			edit := waSecretEdit(info, carrier, originals, pairs)
			if edit == nil {
				t.Fatal("secret edit not decrypted")
			}
			d, e := waDecode(info, edit, pairs)
			if e != nil || d.Kind != "edit" || d.Text != "after" || d.Target != "target" || d.Edited == nil {
				t.Fatalf("secret edit decoded as %#v", d)
			}
			waKeepReply(&d, originals)
			if d.QuotedID != "quoted" {
				t.Fatalf("edit lost the reply id: %#v", d)
			}
			if waSecretEdit(info, carrier, map[string]waOriginal{}, pairs) != nil {
				t.Fatal("decrypted without the target's secret")
			}
		})
	}
}
