package offlinepush

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWebhookSignsAndPosts(t *testing.T) {
	var got webhookBody
	var sig, ts string
	var body []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ = io.ReadAll(r.Body)
		sig, ts = r.Header.Get("X-Nexo-Signature"), r.Header.Get("X-Nexo-Timestamp")
		_ = json.Unmarshal(body, &got)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()
	wh := NewWebhook(srv.URL, "s3cret", time.Second)
	wh.now = func() time.Time { return time.Unix(1_700_000_000, 0) }
	n := Notification{ConversationId: "c", Seq: 7, SessionType: 1, SenderId: "u___1", ContentType: 1, Content: `{"text":"hi"}`, SendTime: 5}
	if err := wh.Push(t.Context(), []string{"u___2"}, n); err != nil {
		t.Fatal(err)
	}
	if got.EventId != "c:7" || len(got.UserIds) != 1 || got.Notification != n || got.Preview != "hi" || ts != "1700000000" {
		t.Fatalf("body: %+v ts=%s", got, ts)
	}
	if sig != Sign("s3cret", ts, body) || sig == Sign("other", ts, body) {
		t.Fatalf("signature mismatch: %s", sig)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(500) }))
	defer bad.Close()
	if err := NewWebhook(bad.URL, "s", time.Second).Push(t.Context(), []string{"u___2"}, n); err == nil {
		t.Fatal("non-2xx must be an error")
	}
}
