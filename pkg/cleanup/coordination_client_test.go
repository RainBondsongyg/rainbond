package cleanup

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCoordinationClientBindsPathsAndNeverRetriesWrites(t *testing.T) {
	for _, status := range []int{200, 409, 503, 307} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				if r.URL.Path != "/v2/cleanup/stores/store/operations" || r.Method != "POST" || r.Header.Get("Authorization") != "Token fixture-only" {
					t.Error("wrong request binding")
				}
				w.Header().Set("Location", "/should-not-follow")
				w.WriteHeader(status)
				fmt.Fprint(w, `{"bean":{"protocol":1,"newly_admitted":true}}`)
			}))
			defer server.Close()
			client, err := NewCoordinationClient(server.URL, "fixture-only", true)
			if err != nil {
				t.Fatal(err)
			}
			admitted, err := client.Acquire(context.Background(), operation("op", "producer", "app/a"))
			if calls != 1 {
				t.Fatal("write retried or redirected", calls)
			}
			if status == 200 && (err != nil || !admitted) {
				t.Fatal(admitted, err)
			}
			if status != 200 && (err == nil || admitted) {
				t.Fatal("invalid status admitted operation", admitted, err)
			}
		})
	}
}
func TestCoordinationClientRequiresExplicitProtocolAndAdmission(t *testing.T) {
	for _, body := range []string{`{}`, `{"bean":{"newly_admitted":true}}`, `{"bean":{"protocol":1}}`, `{"bean":{"protocol":2,"newly_admitted":true}}`, `{"bean":{"protocol":1,"newly_admitted":true}} {}`} {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
		client, err := NewCoordinationClient(server.URL, "fixture-only", true)
		if err != nil {
			t.Fatal(err)
		}
		admitted, err := client.Acquire(context.Background(), operation("op", "producer", "app/a"))
		server.Close()
		if err == nil || admitted {
			t.Fatal("invalid acknowledgement admitted operation")
		}
	}
}
