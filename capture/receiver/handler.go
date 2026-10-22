package receiver

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/05802/qmd2/capture/journal"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
	"reflect"
	"time"
)

const Version = "v0.0.0-20260928140511-35f522c88ce3"

type Handler struct{ Journal *journal.Writer }

func (h Handler) Handle(event any) error {
	kind := "unknown"
	if t := reflect.TypeOf(event); t != nil {
		kind = t.String()
	}
	info := json.RawMessage(`{}`)
	raw := ""
	var err error
	if msg, ok := event.(*events.Message); ok {
		kind = "message"
		b, e := json.Marshal(msg.Info)
		if e != nil {
			return e
		}
		info = b
		if msg.RawMessage != nil {
			p, e := proto.Marshal(msg.RawMessage)
			if e != nil {
				return e
			}
			raw = base64.StdEncoding.EncodeToString(p)
		}
	} else {
		kind, info, err = eventInfo(event, kind)
	}
	if err != nil {
		return errors.New("event encoding failed")
	}
	if !json.Valid(info) {
		return fmt.Errorf("invalid event info")
	}
	if err := h.Journal.Append(journal.Record{Kind: kind, Info: info, ProtoB64: raw, ReceivedAt: time.Now().UTC(), WhatsmeowVersion: Version}); err != nil {
		return err
	}
	return nil
}

func eventInfo(event any, kind string) (string, json.RawMessage, error) {
	info := json.RawMessage(`{}`)
	var err error
	switch v := event.(type) {
	case *events.Receipt:
		kind = "receipt"
		info, err = json.Marshal(v)
	case *events.UndecryptableMessage:
		kind = "undecryptable_message"
		info, err = json.Marshal(v)
	case *events.HistorySync:
		kind = "history_sync"
		info, err = json.Marshal(v)
	case *events.GroupInfo:
		kind = "group_info"
		info, err = json.Marshal(v)
	case *events.PushName:
		kind = "push_name"
		info, err = json.Marshal(v)
	case *events.Contact:
		kind = "contact"
		info, err = json.Marshal(v)
	case *events.AppState:
		kind = "app_state"
		info, err = json.Marshal(v)
	case *events.Connected:
		kind = "connected"
	case *events.Disconnected:
		kind = "disconnected"
	case *events.LoggedOut:
		kind = "logged_out"
		info, err = json.Marshal(v)
	default:
		info, err = json.Marshal(event)
	}
	return kind, info, err
}
