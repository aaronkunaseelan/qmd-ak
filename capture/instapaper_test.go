package fetch

import "testing"

func TestPermanentTextFailureTable(t *testing.T) {
	for _, row := range []struct {
		status int
		body   string
		want   bool
	}{{200, "", true}, {200, "  ", true}, {200, "<p>text</p>", false}, {204, "", false}, {400, "", true}, {404, "missing", true}, {401, "", true}, {403, "", true}, {408, "", true}, {429, "", false}, {500, "", false}, {504, "", false}, {0, "", false}} {
		if got := permanentTextFailure(row.status, []byte(row.body)); got != row.want {
			t.Errorf("status %d body %q: %v want %v", row.status, row.body, got, row.want)
		}
	}
}
