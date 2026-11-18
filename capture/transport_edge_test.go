package fetch

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A transport failure names its class and nothing else: no host, port or provider text.
func TestTransportFailureClass(t *testing.T) {
	closed, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	closedAddr := closed.Addr().String()
	closed.Close()
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
	}))
	defer hang.Close()
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if conn, _, err := w.(http.Hijacker).Hijack(); err == nil {
			conn.Close()
		}
	}))
	defer drop.Close()
	for _, c := range []struct{ class, endpoint, host string }{
		{"refused", "http://" + closedAddr + "/", closedAddr},
		{"timeout", hang.URL, strings.TrimPrefix(hang.URL, "http://")},
		{"reset", drop.URL, strings.TrimPrefix(drop.URL, "http://")},
		{"dns", "http://qmd-f156-transport.invalid/", "qmd-f156-transport.invalid"},
	} {
		t.Run(c.class, func(t *testing.T) {
			timeout := 5 * time.Second
			if c.class == "timeout" {
				timeout = 100 * time.Millisecond
			}
			r := &run{client: &http.Client{Timeout: timeout}}
			_, _, err := r.request(context.Background(), "GET", c.endpoint, nil, nil)
			want := "provider transport failed (" + c.class + ")"
			if err == nil || err.Error() != want {
				t.Fatalf("got %v, want %q", err, want)
			}
			_, port, _ := net.SplitHostPort(c.host)
			for _, leak := range []string{c.host, port, "127.0.0.1", "localhost"} {
				if leak != "" && strings.Contains(err.Error(), leak) {
					t.Fatalf("class message leaks %q: %v", leak, err)
				}
			}
		})
	}
}

// A run deadline is its own message, not a provider fault.
func TestTransportRunDeadline(t *testing.T) {
	hang := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(400 * time.Millisecond)
	}))
	defer hang.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	r := &run{client: &http.Client{Timeout: 5 * time.Second}}
	_, _, err := r.request(ctx, "GET", hang.URL, nil, nil)
	if err == nil || err.Error() != "request deadline reached" {
		t.Fatalf("got %v, want a run deadline message", err)
	}
}
