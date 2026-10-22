package fetch

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestFathomHTTPPacing(t *testing.T) {
	var mu sync.Mutex
	var arrivals []time.Time
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		mu.Lock()
		arrivals = append(arrivals, time.Now())
		n := len(arrivals)
		mu.Unlock()
		if n == 1 {
			w.Header().Set("Retry-After", "0.18")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()
	r := &run{client: server.Client()}
	r.client.Transport = &fathomTransport{base: http.DefaultTransport, interval: 120 * time.Millisecond, receipt: &r.receipt}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if _, status, err := r.request(ctx, "GET", server.URL, nil, nil); err != nil || status != 200 {
			t.Fatalf("HTTP status=%d error=%v", status, err)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if r.receipt.RateLimited {
		t.Fatal("recovered HTTP 429 must not mark run incomplete")
	}
	if len(arrivals) != 3 || r.receipt.RequestsSent != 3 {
		t.Fatalf("wire requests=%d receipt=%d", len(arrivals), r.receipt.RequestsSent)
	}
	if arrivals[1].Sub(arrivals[0]) < 170*time.Millisecond {
		t.Fatal("Retry-After not honoured")
	}
	if arrivals[2].Sub(arrivals[1]) < 110*time.Millisecond {
		t.Fatal("wire requests not paced")
	}
	cancelled, stop := context.WithCancel(context.Background())
	stop()
	if _, _, err := r.request(cancelled, "GET", server.URL, nil, nil); err == nil {
		t.Fatal("cancelled request succeeded")
	}
	if r.receipt.RequestsSent != 3 {
		t.Fatal("cancelled wait sent a request")
	}
}

func TestHTTPRateLimitReceipt(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Retry-After", "0.001")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	r := &run{client: server.Client()}
	if _, status, err := r.request(context.Background(), "GET", server.URL, nil, nil); err == nil || status != 429 || !r.receipt.RateLimited {
		t.Fatal("terminal HTTP 429 must mark receipt")
	}
}

func TestListingHeadHTTPEdge(t *testing.T) {
	for _, source := range []string{"fathom", "calendar"} {
		t.Run(source, func(t *testing.T) {
			root := t.TempDir()
			t.Setenv("HOME", root)
			config := filepath.Join(root, ".config/google-api")
			os.MkdirAll(config, 0700)
			os.WriteFile(filepath.Join(config, "tokens-synthetic.json"), []byte(`{"account":"synthetic","access_token":"synthetic","expiry_date":4102444800000}`), 0600)
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				switch req.URL.Path {
				case "/external/v1/team_members":
					w.Write([]byte(`{"items":[{"email":"synthetic"}]}`))
				case "/external/v1/meetings":
					w.Write([]byte(`{"items":[]}`))
				case "/gmail/v1/users/me/profile":
					w.Write([]byte(`{"emailAddress":"synthetic"}`))
				case "/calendar/v3/users/me/calendarList":
					w.Write([]byte(`{"items":[{"id":"synthetic"}]}`))
				case "/calendar/v3/calendars/synthetic/events":
					w.Write([]byte(`{"items":[],"nextSyncToken":"synthetic"}`))
				default:
					w.WriteHeader(404)
				}
			}))
			defer server.Close()
			client := server.Client()
			transport := client.Transport.(*http.Transport)
			transport.TLSClientConfig.ServerName = "example.com"
			transport.DialContext = func(ctx context.Context, network, address string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			r := &run{source: source, account: "synthetic", client: client, spool: filepath.Join(root, "spool"), statePath: filepath.Join(root, "state.json"), env: map[string]string{"QMD_FATHOM_API_KEY": "synthetic"}}
			var err error
			if source == "calendar" {
				err = runCalendar(context.Background(), r)
			} else {
				err = runFathom(context.Background(), r)
			}
			if err != nil || !r.receipt.HeadComplete {
				t.Fatalf("head incomplete: %v", err)
			}
		})
	}
}
