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
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/gcmutil"
	"go.mau.fi/whatsmeow/util/hkdfutil"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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
	Undecoded     string           `json:"undecoded,omitempty"`
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
	waSummary(&d, m)
	waControls(&d, info, m, pairs)
	waUndecoded(&d, m)
	return d, nil
}
func waUnwrap(m map[string]any) map[string]any {
	for depth := 0; depth < 8; depth++ {
		found := false
		for _, k := range []string{"ephemeralMessage", "viewOnceMessage", "viewOnceMessageV2", "viewOnceMessageV2Extension", "documentWithCaptionMessage", "editedMessage", "deviceSentMessage", "associatedChildMessage", "commentMessage"} {
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
	for _, k := range []string{"imageMessage", "audioMessage", "videoMessage", "ptvMessage", "documentMessage", "stickerMessage"} {
		if x := waMap(m, k); x != nil {
			media := map[string]any{"type": strings.TrimSuffix(strings.Replace(k, "ptv", "video", 1), "Message")}
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

// Content types with neither text nor media become one line built from their own fields;
// a path's last word after a space is a unit appended to its value. With no field, a marker.
var waSummaries = map[string][]string{
	"locationMessage":        {"location", "name", "address", "degreesLatitude", "degreesLongitude"},
	"liveLocationMessage":    {"live location", "caption", "degreesLatitude", "degreesLongitude"},
	"contactMessage":         {"contact", "displayName"},
	"contactsArrayMessage":   {"contacts", "displayName", "contacts.displayName"},
	"pollCreationMessage":    {"poll", "name", "options.optionName"},
	"pollCreationMessageV2":  {"poll", "name", "options.optionName"},
	"pollCreationMessageV3":  {"poll", "name", "options.optionName"},
	"eventMessage":           {"event", "name", "description", "location.name"},
	"albumMessage":           {"album", "expectedImageCount images", "expectedVideoCount videos"},
	"groupInviteMessage":     {"group invite", "groupName", "caption"},
	"buttonsMessage":         {"buttons", "contentText", "footerText"},
	"buttonsResponseMessage": {"button reply", "selectedDisplayText"},
	"listMessage":            {"list", "title", "description"},
	"listResponseMessage":    {"list reply", "title"},
	"templateMessage":        {"template", "hydratedTemplate.hydratedTitleText", "hydratedTemplate.hydratedContentText", "hydratedTemplate.hydratedFooterText", "hydratedFourRowTemplate.hydratedTitleText", "hydratedFourRowTemplate.hydratedContentText", "interactiveMessageTemplate.body.text"},
	"interactiveMessage":     {"interactive", "header.title", "body.text", "footer.text"},
	"productMessage":         {"product", "product.title", "product.description"},
}

// Messages that carry no readable owner content: keys, encrypted votes and edits, pins, notices.
var waControlOnly = map[string]bool{"senderKeyDistributionMessage": true, "pollUpdateMessage": true, "secretEncryptedMessage": true, "pinInChatMessage": true, "messageHistoryNotice": true}

func waValues(v any, path []string) []string {
	switch x := v.(type) {
	case []any:
		var out []string
		for _, y := range x {
			out = append(out, waValues(y, path)...)
		}
		return out
	case map[string]any:
		if len(path) > 0 {
			return waValues(x[path[0]], path[1:])
		}
	case string:
		if x != "" && len(path) == 0 {
			return []string{x}
		}
	case float64:
		if len(path) == 0 {
			return []string{strconv.FormatFloat(x, 'f', -1, 64)}
		}
	}
	return nil
}

func waSummary(d *waDecoded, m map[string]any) {
	for _, k := range waFields(m) {
		x := waMap(m, k)
		if d.QuotedID == "" {
			d.QuotedID = waString(waMap(x, "contextInfo"), "stanzaID")
		}
		spec, ok := waSummaries[k]
		if !ok || d.Text != "" || len(d.Media) > 0 {
			continue
		}
		line := []string{"[" + spec[0] + "]"}
		for _, p := range spec[1:] {
			p, unit, _ := strings.Cut(p, " ")
			for _, v := range waValues(x, strings.Split(p, ".")) {
				line = append(line, strings.TrimSpace(v+" "+unit))
			}
		}
		if len(line) > 1 { // a bare label is left to the not-decoded marker
			d.Text = strings.Join(line, " ")
		}
	}
}

// waUndecoded makes an ordinary message with no text and no media say which fields it carried.
func waUndecoded(d *waDecoded, m map[string]any) {
	if d.Kind != "message" || d.Text != "" || len(d.Media) > 0 {
		return
	}
	fields := waFields(m)
	control := len(fields) > 0
	for _, k := range fields {
		control = control && waControlOnly[k]
	}
	if control && len(fields) == 1 && fields[0] == "senderKeyDistributionMessage" {
		d.Kind = "sender_key" // shares its id with the group message it precedes
		return
	} else if control {
		d.Kind = "protocol"
		return
	}
	d.Kind, d.LowerFidelity, d.Undecoded = "binary", true, "message"
	if len(fields) > 0 {
		d.Undecoded = strings.Join(fields, "+")
	}
}

// waFields lists the top-level Message fields, without the metadata every message may carry.
func waFields(m map[string]any) []string {
	var out []string
	for k := range m {
		if k != "messageContextInfo" {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
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
				if e == nil && next.Kind == "message" {
					d.Text, d.Media, d.QuotedID = next.Text, next.Media, next.QuotedID
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

// waOriginal is a message the journal holds, with what a later edit of it needs: its secret,
// its sender's addresses and its reply id.
type waOriginal struct {
	Secret  []byte
	Senders []string
	Quoted  string
}

func waRemember(originals map[string]waOriginal, id string, m *e2e.Message, secret []byte, senders ...string) {
	if len(secret) == 0 {
		secret = m.GetMessageContextInfo().GetMessageSecret()
	}
	if len(secret) == 0 {
		secret = m.GetDeviceSentMessage().GetMessage().GetMessageContextInfo().GetMessageSecret()
	}
	if id == "" {
		return
	}
	// Another record under the same id (a sender key, a copy) keeps what an earlier one gave.
	old, next := originals[id], waOriginal{secret, senders, waQuoted(m, 8)}
	if len(next.Secret) == 0 {
		next.Secret, next.Senders = old.Secret, old.Senders
	}
	if next.Quoted == "" {
		next.Quoted = old.Quoted
	}
	originals[id] = next
}

// waQuoted is the reply id of the first content field that has one, inside any wrapper.
func waQuoted(m *e2e.Message, depth int) string {
	quoted := ""
	m.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() != protoreflect.MessageKind || fd.IsList() || fd.IsMap() {
			return true
		}
		switch x := v.Message().Interface().(type) {
		case interface{ GetContextInfo() *e2e.ContextInfo }:
			quoted = x.GetContextInfo().GetStanzaID()
		case interface{ GetMessage() *e2e.Message }:
			if depth > 0 {
				quoted = waQuoted(x.GetMessage(), depth-1)
			}
		}
		return quoted == ""
	})
	return quoted
}

// waKeepReply gives an edit that names no reply id the reply id of the message it edits.
func waKeepReply(d *waDecoded, originals map[string]waOriginal) {
	if d.Kind == "edit" && d.QuotedID == "" {
		d.QuotedID = originals[d.Target].Quoted
	}
}

// waJIDForms is each address without its device, and its LID or phone-number counterpart.
func waJIDForms(pairs map[string]string, jids ...string) []string {
	var out []string
	for _, j := range jids {
		parsed, e := types.ParseJID(j)
		if j == "" || e != nil {
			continue
		}
		j = parsed.ToNonAD().String()
		out = append(out, j)
		for lid, pn := range pairs {
			if lid == j {
				out = append(out, pn)
			} else if pn == j {
				out = append(out, lid)
			}
		}
	}
	return out
}

// waSecretEdit returns the protocol edit that a message edit encrypted with its target's secret
// carries, decrypted as whatsmeow's DecryptSecretEncryptedMessage does; nil when the record is
// not one or the target's secret was not seen.
func waSecretEdit(info map[string]any, m *e2e.Message, originals map[string]waOriginal, pairs map[string]string) *e2e.Message {
	enc := m.GetSecretEncryptedMessage()
	if enc == nil {
		enc = m.GetDeviceSentMessage().GetMessage().GetSecretEncryptedMessage()
	}
	key := enc.GetTargetMessageKey()
	orig := originals[key.GetID()]
	if enc.GetSecretEncType() != e2e.SecretEncryptedMessage_MESSAGE_EDIT || len(orig.Secret) == 0 {
		return nil
	}
	editors := waJIDForms(pairs, waString(info, "Sender"), waString(info, "SenderAlt"))
	authors := append([]string{key.GetParticipant(), key.GetRemoteJID()}, orig.Senders...)
	if key.GetFromMe() {
		authors = append(authors, waString(info, "Sender"), waString(info, "SenderAlt"))
	}
	for _, author := range waJIDForms(pairs, authors...) {
		for _, editor := range editors {
			secret := hkdfutil.SHA256(orig.Secret, nil, []byte(key.GetID()+author+editor+"Message Edit"), 32)
			plain, e := gcmutil.Decrypt(secret, enc.GetEncIV(), enc.GetEncPayload(), nil)
			var edited e2e.Message
			if e != nil || proto.Unmarshal(plain, &edited) != nil {
				continue
			}
			if edited.GetProtocolMessage().GetEditedMessage() != nil {
				return &edited
			}
			at, _ := time.Parse(time.RFC3339Nano, waString(info, "Timestamp"))
			kind := e2e.ProtocolMessage_MESSAGE_EDIT
			return &e2e.Message{ProtocolMessage: &e2e.ProtocolMessage{Key: key, Type: &kind, EditedMessage: &edited, TimestampMS: proto.Int64(at.UnixMilli())}}
		}
	}
	return nil
}
