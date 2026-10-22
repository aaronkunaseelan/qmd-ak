package fetch

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Frozen context and every history mapping are loaded before any import children.
func waLoadContext(ctx context.Context, r *run, c *waCursor) error {
	if r.aliasesFrom != "" {
		if e := waLoadAliases(ctx, r, c); e != nil {
			return e
		}
	}
	if r.contextPath != "" {
		if e := waLoadFrozenContext(r, c); e != nil {
			return e
		}
	}
	if r.aliasesFrom != "" || r.contextPath != "" {
		cursor, e := json.Marshal(c)
		if e != nil {
			return errors.New("WhatsApp context checkpoint invalid")
		}
		r.state.Cursor = cursor
		return r.save()
	}
	return nil
}

func waLoadAliases(ctx context.Context, r *run, c *waCursor) error {
	entries, e := os.ReadDir(r.aliasesFrom)
	if e != nil {
		return errors.New("WhatsApp alias inputs unavailable")
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for _, entry := range entries {
		if e = ctx.Err(); e != nil {
			return e
		}
		if entry.IsDir() {
			continue
		}
		b, e := inventoryReadLocal(filepath.Join(r.aliasesFrom, entry.Name()))
		if e != nil {
			return e
		}
		if e = waGatherObjectPairs(b, c.Pairs); e != nil {
			return e
		}
	}
	return nil
}

func waLoadFrozenContext(r *run, c *waCursor) error {
	b, e := inventoryReadLocal(r.contextPath)
	if e != nil {
		return errors.New("WhatsApp frozen context unreadable")
	}
	var original map[string]json.RawMessage
	if json.Unmarshal(b, &original) != nil {
		return errors.New("WhatsApp frozen context invalid")
	}
	contextBytes := b
	if update, ok := original["ctx_update"]; ok {
		contextBytes = update
	}
	var update map[string]map[string]string
	if json.Unmarshal(contextBytes, &update) != nil {
		return errors.New("WhatsApp context maps invalid")
	}
	for _, kind := range []string{"aliases", "contacts", "groups"} {
		if update[kind] == nil {
			update[kind] = map[string]string{}
		}
	}
	for lid, pn := range update["aliases"] {
		waAddPair(c.Pairs, lid, pn)
	}
	for lid, pn := range c.Pairs {
		update["aliases"][lid] = pn
	}
	payload, _ := json.Marshal(map[string]any{"raw": map[string]string{"context_sha256": digest(b)}, "decoded": map[string]string{"kind": "history_source"}, "ctx_update": update})
	if e = r.emit("context:"+digest(payload), payload, ObservationState{Version: "whatsapp-decoded-v1"}, time.Time{}); e != nil {
		return e
	}
	return nil
}

func waRecordContext(rec waRecord, pairs map[string]string) map[string]any {
	info := rec.Info
	waInfoPairs(info, pairs)
	waAddPair(pairs, waString(info, "JID"), waString(info, "JIDAlt"))
	waAddPair(pairs, waString(info, "Sender"), waString(info, "SenderPN"))
	relevant := map[string]bool{}
	for _, key := range []string{"Chat", "Sender", "SenderAlt", "RecipientAlt", "ChatAlt", "JID", "JIDAlt", "SenderPN"} {
		relevant[waString(info, key)] = true
	}
	aliases := map[string]string{}
	for lid, pn := range pairs {
		if relevant[lid] || relevant[pn] {
			aliases[lid] = pn
		}
	}
	out := waPairsContext(aliases)
	contacts := out["contacts"].(map[string]string)
	groups := out["groups"].(map[string]string)
	sender := waCanonical(waString(info, "Sender"), pairs)
	if name := waString(info, "PushName"); name != "" && sender != "" {
		contacts[sender] = name
	}
	jid := waCanonical(waString(info, "JID"), pairs)
	if jid != "" {
		switch rec.Kind {
		case "push_name":
			if name := waString(info, "NewPushName"); name != "" {
				contacts[jid] = name
			}
		case "contact":
			if name := waString(waMap(info, "Action"), "fullName", "firstName"); name != "" {
				contacts[jid] = name
			}
		case "group_info":
			if name := waString(waMap(info, "Name"), "Name"); name != "" {
				groups[jid] = name
			}
		}
	}
	return out
}
func waEmitContext(r *run, line []byte, rec waRecord, pairs map[string]string) (bool, error) {
	update := waRecordContext(rec, pairs)
	n := 0
	for _, kind := range []string{"aliases", "contacts", "groups"} {
		n += len(update[kind].(map[string]string))
	}
	if n == 0 {
		return false, nil
	}
	payload, _ := json.Marshal(map[string]any{"raw": map[string]string{"bytes_b64": base64.StdEncoding.EncodeToString(line)}, "decoded": map[string]string{"kind": "history_source"}, "ctx_update": update})
	written, unchanged := r.receipt.Written, r.receipt.Unchanged
	if e := r.emit("context:"+digest(line), payload, ObservationState{Version: "whatsapp-decoded-v1", Labels: map[string]string{"reason": "context_only"}}, time.Time{}); e != nil {
		return true, e
	}
	r.receipt.Written = written
	r.receipt.Unchanged = unchanged
	r.receipt.Excluded++
	return true, nil
}
