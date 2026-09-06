package offlinepush

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestWebhookRejectsRedirects(t *testing.T) {
	for _, name := range []string{"downgrade", "cross-host", "same-host-relative"} {
		for _, status := range []int{301, 302, 303, 307, 308} {
			t.Run(fmt.Sprintf("%s/%d", name, status), func(t *testing.T) {
				var sourceCalls, targetCalls, targetBytes, targetSignatures atomic.Int64
				targetHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					targetCalls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					targetBytes.Add(int64(len(body)))
					if r.Header.Get("X-Nexo-Signature") != "" {
						targetSignatures.Add(1)
					}
					if r.Header.Get("X-Nexo-Signature") != Sign("secret", r.Header.Get("X-Nexo-Timestamp"), body) {
						w.WriteHeader(http.StatusUnauthorized)
						return
					}
					w.WriteHeader(http.StatusNoContent)
				})
				var location string
				source := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/target" {
						targetHandler.ServeHTTP(w, r)
						return
					}
					sourceCalls.Add(1)
					body, err := io.ReadAll(r.Body)
					if err != nil {
						t.Error(err)
					}
					if r.Method != http.MethodPost || len(body) == 0 {
						t.Errorf("source request: method=%s body bytes=%d", r.Method, len(body))
					}
					if r.Header.Get("X-Nexo-Signature") != Sign("secret", r.Header.Get("X-Nexo-Timestamp"), body) {
						t.Error("source signature mismatch")
					}
					http.Redirect(w, r, location, status)
				}))
				defer source.Close()
				transport := source.Client().Transport.(*http.Transport).Clone()
				transport.Proxy = nil
				defer transport.CloseIdleConnections()

				var targetUrl string
				switch name {
				case "downgrade":
					target := httptest.NewServer(targetHandler)
					defer target.Close()
					targetUrl = target.URL
					location = targetUrl
				case "cross-host":
					target := httptest.NewTLSServer(targetHandler)
					defer target.Close()
					if err := target.Certificate().VerifyHostname("example.com"); err != nil {
						t.Fatal(err)
					}
					_, port, err := net.SplitHostPort(target.Listener.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					host := net.JoinHostPort("example.com", port)
					dialer := &net.Dialer{Timeout: time.Second}
					transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
						if addr == host {
							addr = target.Listener.Addr().String()
						}
						return dialer.DialContext(ctx, network, addr)
					}
					targetUrl = "https://" + host
					location = targetUrl
				case "same-host-relative":
					location = "/target"
					targetUrl = source.URL + location
				}

				n := Notification{ConversationId: "c", Seq: 7, ContentType: 1, Content: `{"text":"private"}`}
				// A signed direct delivery proves the target is reachable with TLS verification enabled.
				control := NewWebhook(targetUrl, "secret", time.Second)
				control.client.Transport = transport
				if err := control.Push(t.Context(), []string{"u___2"}, n); err != nil {
					t.Fatalf("direct delivery: %v", err)
				}
				if targetCalls.Load() != 1 || targetBytes.Load() == 0 || targetSignatures.Load() != 1 {
					t.Fatal("direct delivery did not reach target with signed body")
				}
				targetCalls.Store(0)
				targetBytes.Store(0)
				targetSignatures.Store(0)

				wh := NewWebhook(source.URL, "secret", time.Second)
				wh.client.Transport = transport
				err := wh.Push(t.Context(), []string{"u___2"}, n)
				if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("status %d", status)) {
					t.Errorf("Push error = %v, want redirect status %d", err, status)
				}
				if got := sourceCalls.Load(); got != 1 {
					t.Errorf("source requests = %d, want exactly 1 (no retry)", got)
				}
				if targetCalls.Load() != 0 || targetBytes.Load() != 0 || targetSignatures.Load() != 0 {
					t.Errorf("redirect leaked: requests=%d body bytes=%d signatures=%d",
						targetCalls.Load(), targetBytes.Load(), targetSignatures.Load())
				}
			})
		}
	}
}
