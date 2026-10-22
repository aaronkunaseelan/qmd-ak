package fetch

import (
	"encoding/json"
	"testing"
)

func TestBirdFailureTable(t *testing.T) {
	for _, row := range []struct{ private, want string }{{"HTTP 404 private details", "bird provider HTTP 404"}, {"statusCode: 429 private details", "bird provider HTTP 429"}, {"unknown option private details", "bird unsupported option"}, {"unauthorized private details", "bird credential refused"}, {"private details", "bird subprocess failed"}} {
		if got := birdFailure(row.private).Error(); got != row.want {
			t.Errorf("classification %q want %q", got, row.want)
		}
	}
}
func TestTweetVersionTable(t *testing.T) {
	for _, row := range []struct {
		before, after string
		same          bool
	}{{`{"id":"1","text":"hello","likeCount":1,"_raw":{"legacy":{"favorite_count":1}}}`, `{"id":"1","text":"hello","likeCount":99,"_raw":{"legacy":{"favorite_count":99}}}`, true}, {`{"id":"1","text":"hello","_raw":{"normal_followers_count":1,"media_count":2}}`, `{"id":"1","text":"hello","_raw":{"normal_followers_count":9,"media_count":8}}`, true},
		{`{"id":"1","text":"hello","_raw":{"card":{"legacy":{"binding_values":[{"key":"image","value":{"url":"one"}}]}},"core":{"user_results":{"result":{"legacy":{"following":false}}}}}}`, `{"id":"1","text":"hello","_raw":{"card":{"legacy":{"binding_values":[{"key":"image","value":{"url":"two"}}]}},"core":{"user_results":{"result":{"relationship_perspectives":{"following":true}}}}}}`, true},
		{`{"id":"1","quotedTweet":{"id":"2","text":"quote","_raw":{"views":{"count":"1"}}}}`, `{"id":"1","quotedTweet":{"id":"2","text":"quote","_raw":{"views":{"count":"2"},"card":{"binding_values":[]}}}}`, true},
		{`{"id":"1","quotedTweet":{"id":"2","text":"quote"}}`, `{"id":"1","quotedTweet":{"id":"2","text":"edited quote"}}`, false},
		{`{"id":"1","media":[{"url":"one"}]}`, `{"id":"1","media":[{"url":"two"}]}`, false},
		{`{"id":"1","article":{"text":"one"}}`, `{"id":"1","article":{"text":"two"}}`, false},
		{`{"id":"1","createdAt":"one"}`, `{"id":"1","createdAt":"two"}`, false},
		{`{"id":"1","text":"hello","account":"same"}`, `{"id":"1","text":"changed","account":"same"}`, false}, {`{"id":"1","text":"hello","account":"one"}`, `{"id":"1","text":"hello","account":"two"}`, false}} {
		var a, b any
		json.Unmarshal([]byte(row.before), &a)
		json.Unmarshal([]byte(row.after), &b)
		x, _ := json.Marshal(stableTweet(a))
		y, _ := json.Marshal(stableTweet(b))
		if (string(x) == string(y)) != row.same {
			t.Fatal("post content/counter version contract failed")
		}
	}
}
