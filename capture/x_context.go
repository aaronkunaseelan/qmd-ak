package fetch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"
)

// Only pending items retain context in the cursor. Completed context lives in packs.
// New head saves get one 24h follow-up; edits of uncached saves fetch context once.
type xContext struct {
	CapturedAt string         `json:"captured_at"`
	Posts      []xContextPost `json:"posts"`
	Notices    []string       `json:"notices"`
}
type xContextPost struct {
	Tweet    map[string]any `json:"tweet"`
	Authored bool           `json:"authored"`
	Role     string         `json:"role"`
}
type xDue struct {
	At      time.Time       `json:"at"`
	Tweet   json.RawMessage `json:"tweet"`
	Initial bool            `json:"initial"`
}

type xPageResult struct {
	Tweets []map[string]any `json:"tweets"`
	Next   string           `json:"nextCursor"`
	Limit  int              `json:"-"`
}

func (r *run) xFetch(ctx context.Context, binary, preload string, remaining *int, args ...string) (*xPageResult, error) {
	if *remaining <= 0 {
		return nil, errors.New("skipped: run request allowance spent")
	}
	pages := "3"
	if args[0] == "thread" {
		pages = "1"
	}
	args = append([]string{"--plain"}, args...)
	args = append(args, "--json-full", "--max-pages", pages)
	b, err := r.birdCommand(ctx, binary, preload, remaining, args...)
	if err != nil {
		return nil, err
	}
	var page xPageResult
	if json.Unmarshal(b, &page) != nil || page.Tweets == nil {
		return nil, errors.New("bird context page invalid")
	}
	page.Limit = 3
	if pages == "1" {
		page.Limit = 1
	}
	return &page, nil
}

func xString(tweet map[string]any, key string) string {
	value, _ := tweet[key].(string)
	return value
}
func xAuthor(tweet map[string]any) string {
	if id := xString(tweet, "authorId"); id != "" {
		return id
	}
	author, _ := tweet["author"].(map[string]any)
	return xString(author, "username")
}

func (r *run) xEnrich(ctx context.Context, binary, preload string, remaining *int, raw json.RawMessage, first ...*xPageResult) (json.RawMessage, *xContext, error) {
	var tweet map[string]any
	if json.Unmarshal(raw, &tweet) != nil || xString(tweet, "id") == "" {
		return nil, nil, errors.New("bird context tweet invalid")
	}
	out := &xContext{CapturedAt: time.Now().UTC().Format(time.RFC3339Nano), Posts: []xContextPost{}, Notices: []string{}}
	seen := map[string]bool{xString(tweet, "id"): true}
	var scopes []map[string]any
	scopes = append(scopes, tweet)
	quoted, _ := tweet["quotedTweet"].(map[string]any)
	if quoted != nil {
		scopes = append(scopes, quoted)
		seen[xString(quoted, "id")] = true
	} else if provider, ok := tweet["_raw"].(map[string]any); ok {
		legacy, _ := provider["legacy"].(map[string]any)
		if id := xString(legacy, "quoted_status_id_str"); id != "" {
			scopes = append(scopes, map[string]any{"id": id})
			seen[id] = true
		}
	}
	for i, scope := range scopes {
		id := xString(scope, "id")
		pool := map[string]map[string]any{}
		collect := func(page *xPageResult, label string) {
			for _, post := range page.Tweets {
				pool[xString(post, "id")] = post
			}
			if page.Next != "" {
				if page.Limit == 1 {
					out.Notices = append(out.Notices, label+" "+id+": continuation present; 1-page focal/parent lookup, other replies not enumerated")
				} else {
					out.Notices = append(out.Notices, label+" "+id+": 3-page cap hit; coverage incomplete")
				}
			}
		}
		pool[id] = scope
		var page *xPageResult
		var err error
		if i == 0 && len(first) > 0 {
			collect(first[0], "thread")
		} else if xAuthor(scope) == "" {
			// A missing quoted post needs one lookup to learn its identity or refusal.
			page, err = r.xFetch(ctx, binary, preload, remaining, "thread", id)
			if err != nil {
				out.Notices = append(out.Notices, "post "+id+" unavailable: "+err.Error())
			} else {
				collect(page, "thread")
			}
		}
		focal := pool[id]
		author := xAuthor(focal)
		profile, _ := focal["author"].(map[string]any)
		handle := xString(profile, "username")
		conversation := xString(focal, "conversationId")
		if handle != "" && conversation != "" {
			page, err = r.xFetch(ctx, binary, preload, remaining, "search", "conversation_id:"+conversation+" from:"+handle, "--all")
			if err != nil {
				out.Notices = append(out.Notices, "author replies "+id+" unavailable: "+err.Error())
			} else {
				collect(page, "author search")
			}
		} else {
			out.Notices = append(out.Notices, "author replies "+id+" unavailable: author or conversation identity missing; provider did not supply a usable post")
		}
		focal = pool[id]
		// Search refreshes the held post too when it is in the returned author page.
		for _, flag := range []string{"bookmarked", "favorited"} {
			if value, ok := scope[flag]; ok {
				focal[flag] = value
			}
		}
		if i == 0 {
			tweet = focal
		} else if xAuthor(focal) != "" {
			tweet["quotedTweet"] = focal
		}
		own := []string{}
		for key, post := range pool {
			if key != id && author != "" && xAuthor(post) == author && xString(post, "conversationId") == conversation {
				own = append(own, key)
			}
		}
		sort.Strings(own)
		// Only parents and posts by this scope's author leave the provider envelope.
		add := func(post map[string]any, role string) {
			key := xString(post, "id")
			if key == "" || seen[key] {
				return
			}
			seen[key] = true
			out.Posts = append(out.Posts, xContextPost{Tweet: post, Authored: author != "" && xAuthor(post) == author, Role: role})
		}
		starts := append([]string{id}, own...)
		for _, key := range starts {
			post := pool[key]
			if post == nil {
				post = focal
			}
			if key != id {
				add(post, "Author reply")
			}
			walked := map[string]bool{}
			for parent := xString(post, "inReplyToStatusId"); parent != "" && !walked[parent]; {
				walked[parent] = true
				target := pool[parent]
				if target == nil {
					page, err = r.xFetch(ctx, binary, preload, remaining, "thread", parent)
					if err != nil {
						out.Notices = append(out.Notices, "parent "+parent+" unavailable: "+err.Error())
						break
					}
					collect(page, "parent thread")
					target = pool[parent]
				}
				if target == nil {
					out.Notices = append(out.Notices, "parent "+parent+" unavailable: provider omitted post; deletion/protection/suspension reason not supplied")
					break
				}
				add(target, "Parent")
				parent = xString(target, "inReplyToStatusId")
			}
		}
	}
	updated, err := json.Marshal(tweet)
	return updated, out, err
}

func (r *run) xContexts(ctx context.Context, binary, preload string, remaining *int, c *xCursor) error {
	ids := []string{}
	for id, due := range c.Due {
		if due.Initial || !due.At.After(time.Now()) {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return c.Due[ids[i]].At.Before(c.Due[ids[j]].At) })
	for _, id := range ids {
		due := c.Due[id]
		if *remaining <= 0 {
			reason := "skipped: run request allowance spent"
			r.receipt.ContextSkipped = append(r.receipt.ContextSkipped, id+": "+reason)
			current := c.Contexts[id]
			if current == nil && !due.Initial {
				// An edited save keeps its archived context until enrichment can run.
				if err := r.xCommit(c); err != nil {
					return err
				}
				continue
			}
			if current == nil {
				current = &xContext{CapturedAt: time.Now().UTC().Format(time.RFC3339Nano), Posts: []xContextPost{}}
				c.Contexts[id] = current
			}
			present := false
			for _, notice := range current.Notices {
				if notice == reason {
					present = true
				}
			}
			if !present {
				current.Notices = append(current.Notices, reason)
			}
			due.Initial = false
			if err := r.xTweet(due.Tweet, !present); err != nil {
				return err
			}
			if err := r.xCommit(c); err != nil {
				return err
			}
			continue
		}
		held := due.Tweet
		var first []*xPageResult
		var refusal string
		if !due.Initial {
			page, err := r.xFetch(ctx, binary, preload, remaining, "thread", id)
			if err != nil {
				refusal = "post " + id + " unavailable: " + err.Error()
			} else {
				first = append(first, page)
				refusal = "post " + id + " unavailable: provider omitted focal post"
				for _, post := range page.Tweets {
					if xString(post, "id") == id {
						var previous map[string]any
						json.Unmarshal(held, &previous)
						for _, flag := range []string{"bookmarked", "favorited"} {
							if value, ok := previous[flag]; ok {
								post[flag] = value
							}
						}
						held, _ = json.Marshal(post)
						refusal = ""
						break
					}
				}
			}
		}
		raw, context, err := r.xEnrich(ctx, binary, preload, remaining, held, first...)
		if err != nil {
			return err
		}
		if refusal != "" {
			context.Notices = append(context.Notices, refusal)
		}
		c.Contexts[id] = context
		if err = r.xTweet(raw, true); err != nil {
			return err
		}
		if due.Initial {
			due.Initial = false
			due.Tweet = raw
		} else {
			delete(c.Due, id)
			delete(c.Contexts, id)
		}
		if err = r.xCommit(c); err != nil {
			return err
		}
	}
	return nil
}

func (r *run) xDemand(ctx context.Context, binary, preload string, remaining *int) error {
	// A single id is read through the same capped provider path, never a new listing.
	page, err := r.xFetch(ctx, binary, preload, remaining, "thread", r.xID)
	if err != nil {
		return err
	}
	var raw json.RawMessage
	for _, tweet := range page.Tweets {
		if xString(tweet, "id") == r.xID {
			raw, _ = json.Marshal(tweet)
			break
		}
	}
	if raw == nil {
		return fmt.Errorf("post unavailable: provider omitted focal post")
	}
	raw, context, err := r.xEnrich(ctx, binary, preload, remaining, raw, page)
	if err != nil {
		return err
	}
	r.xstate.Contexts[r.xID] = context
	if err = r.xTweet(raw, true); err != nil {
		return err
	}
	if r.xstate.Due[r.xID] == nil {
		delete(r.xstate.Contexts, r.xID)
	}
	return r.xCommit(r.xstate)
}
