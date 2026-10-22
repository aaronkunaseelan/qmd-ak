package fetch

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

type inventoryLocation struct {
	Kind string `json:"kind"`
	Ref  string `json:"ref"`
}
type inventoryRow struct {
	ObjectHash      string              `json:"object_hash"`
	Locations       []inventoryLocation `json:"locations"`
	ExternalItemKey string              `json:"external_item_key"`
	CapturedAt      time.Time           `json:"captured_at"`
	SourceAt        time.Time           `json:"source_at"`
	LineageJSON     map[string]any      `json:"lineage_json"`
	SourceSystem    string              `json:"source_system"`
	SourceInstance  string              `json:"source_instance"`
	VersionID       string              `json:"version_id"`
	Size            int64               `json:"size"`
}

const inventoryObjectMax = 64 * 1024 * 1024

func inventoryLocal(ref string) string {
	if strings.HasPrefix(ref, "file://") {
		u, e := url.Parse(ref)
		if e == nil && (u.Host == "" || u.Host == "localhost") && u.RawQuery == "" && u.Fragment == "" {
			return u.Path
		}
		return ""
	}
	for _, prefix := range []string{"filesystem:", "local:"} {
		ref = strings.TrimPrefix(ref, prefix)
	}
	if strings.HasPrefix(ref, "/") {
		return ref
	}
	return ""
}

func inventoryReadLocal(path string) ([]byte, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, fmt.Errorf("whatsapp: local object unavailable")
	}
	defer f.Close()
	st, e := f.Stat()
	if e != nil || !st.Mode().IsRegular() || st.Size() > inventoryObjectMax {
		return nil, fmt.Errorf("whatsapp: local object invalid")
	}
	b, e := io.ReadAll(io.LimitReader(f, inventoryObjectMax+1))
	if e != nil || len(b) > inventoryObjectMax {
		return nil, fmt.Errorf("whatsapp: local object read failed")
	}
	return b, nil
}

func inventoryBytes(ctx context.Context, r *run, row inventoryRow) ([]byte, error) {
	for _, location := range row.Locations {
		if location.Kind != "filesystem" && location.Kind != "local" {
			continue
		}
		if path := inventoryLocal(location.Ref); path != "" {
			if b, e := inventoryReadLocal(path); e == nil && digest(b) == row.ObjectHash {
				return b, nil
			}
		}
	}
	for _, location := range row.Locations {
		if location.Kind != "b2" {
			continue
		}
		remote, ref, valid := strings.Cut(location.Ref, ":")
		if !valid || remote == "" {
			continue
		}
		ref = strings.TrimPrefix(ref, "//")
		bucket, key, ok := strings.Cut(ref, "/")
		if !ok || bucket == "" || key == "" {
			continue
		}
		b, e := r.fetchObject(ctx, remote, bucket, key)
		if e == nil && len(b) <= inventoryObjectMax && digest(b) == row.ObjectHash {
			return b, nil
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("whatsapp: object unavailable or hash mismatch")
}

// The inventory offset moves past failures only with their complete rows retained for retry.
func runObjects(ctx context.Context, r *run, c *waCursor) error {
	inventoryHash, e := waFileHash(r.objects)
	if e != nil {
		return fmt.Errorf("whatsapp: inventory unreadable")
	}
	if c.Objects == nil {
		c.Objects = map[string]int64{}
	}
	if c.ObjectFailures == nil {
		c.ObjectFailures = map[string]inventoryRow{}
	}
	failed := make([]string, 0, len(c.ObjectFailures))
	for hash := range c.ObjectFailures {
		failed = append(failed, hash)
	}
	sort.Strings(failed)
	retried := map[string]bool{}
	batch := 0
	for _, hash := range failed {
		if e = waProcessObject(ctx, r, c, c.ObjectFailures[hash]); e != nil {
			return e
		}
		retried[hash] = true
		batch++
		if batch%100 == 0 {
			if e = waSaveObjects(r, c); e != nil {
				return e
			}
		}
	}
	f, e := os.Open(r.objects)
	if e != nil {
		return fmt.Errorf("whatsapp: inventory unreadable")
	}
	defer f.Close()
	return waReadInventory(ctx, r, c, inventoryHash, retried, f, batch)
}

func waSaveObjects(r *run, c *waCursor) error {
	b, e := json.Marshal(c)
	if e != nil {
		return fmt.Errorf("whatsapp: object checkpoint encode failed")
	}
	r.state.Cursor = b
	return r.save()
}

func waProcessObject(ctx context.Context, r *run, c *waCursor, row inventoryRow) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	beforeFailed := r.receipt.Failed
	b, e := inventoryBytes(ctx, r, row)
	if e == nil {
		e = waCaptureObject(ctx, r, c, b, row)
	}
	if e != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A child decode counts its own failure; a failure with no child
		// (missing bytes) is counted here.
		if r.receipt.Failed == beforeFailed {
			r.receipt.Enumerated++
			r.receipt.Failed++
		}
		r.receipt.FailedIDs = append(r.receipt.FailedIDs, row.ObjectHash)
		at := row.CapturedAt
		if at.IsZero() {
			at = row.SourceAt
		}
		if !at.IsZero() && (r.receipt.OldestFailed.IsZero() || at.Before(r.receipt.OldestFailed)) {
			r.receipt.OldestFailed = at
		}
		c.ObjectFailures[row.ObjectHash] = row
	} else {
		delete(c.ObjectFailures, row.ObjectHash)
	}
	return nil
}

func waReadInventory(ctx context.Context, r *run, c *waCursor, inventoryHash string, retried map[string]bool, f *os.File, batch int) error {
	var e error
	offset := c.Objects[inventoryHash]
	if _, e = f.Seek(offset, io.SeekStart); e != nil {
		return fmt.Errorf("whatsapp: inventory seek failed")
	}
	reader := bufio.NewReaderSize(f, 1024*1024)
	limit := r.limit
	if limit <= 0 {
		limit = 1000
	}
	count := 0
	for {
		if e = ctx.Err(); e != nil {
			return e
		}
		if count == limit {
			_, e = reader.Peek(1)
			if e == nil {
				r.receipt.HasMore = true
			}
			if e != nil && e != io.EOF {
				return fmt.Errorf("whatsapp: inventory read failed")
			}
			return waSaveObjects(r, c)
		}
		line, e := reader.ReadSlice('\n')
		if e == io.EOF {
			if len(line) > 0 {
				r.receipt.HasMore = true
			}
			return waSaveObjects(r, c)
		}
		if e != nil || len(line) > 1024*1024 {
			return fmt.Errorf("whatsapp: inventory row exceeds bound or unreadable")
		}
		row, e := waInventoryRow(r, line)
		if e != nil {
			return e
		}
		if !retried[row.ObjectHash] {
			if e = waProcessObject(ctx, r, c, row); e != nil {
				return e
			}
		}
		offset += int64(len(line))
		c.Objects[inventoryHash] = offset
		count++
		batch++
		if batch%100 == 0 {
			if e = waSaveObjects(r, c); e != nil {
				return e
			}
		}
	}
}

func waInventoryRow(r *run, line []byte) (inventoryRow, error) {
	var row inventoryRow
	if json.Unmarshal(line, &row) != nil || row.SourceSystem != "" && row.SourceSystem != "whatsapp" || row.ExternalItemKey == "" || len(row.ObjectHash) != 64 {
		return row, waFail(r, fmt.Errorf("whatsapp: invalid inventory identity"))
	}
	hash, e := hex.DecodeString(row.ObjectHash)
	if e != nil || len(hash) != 32 || row.ObjectHash != strings.ToLower(row.ObjectHash) {
		return row, waFail(r, fmt.Errorf("whatsapp: invalid inventory hash"))
	}
	return row, nil
}
