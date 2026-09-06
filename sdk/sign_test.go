package sdk_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/sdk"
)

func TestInternalEscapedPrefixRoundTrip(t *testing.T) {
	for _, tc := range []struct{ name, prefix, wire string }{
		{"normal", "/im", "/im"},
		{"escaped letter", "/%69m", "/%69m"},
		{"escaped slash", "/im%2Fv1", "/im%2Fv1"},
		{"unicode", "/中文", "/%E4%B8%AD%E6%96%87"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cache := local.New()
			t.Cleanup(func() { cache.Close() })
			verifier := auth.NewInternal([]string{"secret"}, []string{"platform"}, time.Minute, cache)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := io.ReadAll(r.Body)
				if err != nil {
					t.Error(err)
					w.WriteHeader(500)
					return
				}
				path, query, _ := strings.Cut(r.RequestURI, "?")
				if !strings.HasPrefix(path, tc.wire+"/api/v1/internal/") {
					t.Errorf("unexpected wire path %q", path)
				}
				signed := auth.InternalRequest{
					Service: r.Header.Get("X-Service-Name"), Timestamp: r.Header.Get("X-Timestamp"), Nonce: r.Header.Get("X-Nonce"),
					Method: r.Method, RawPath: path, RawQuery: query, Body: body,
					UserId: r.Header.Get("X-User-Id"), PlatformId: r.Header.Get("X-Platform-Id"), Signature: r.Header.Get("X-Signature"),
				}
				if err := verifier.Verify(r.Context(), signed); err != nil {
					t.Errorf("wire HMAC verification: %v", err)
					w.WriteHeader(http.StatusUnauthorized)
					_, _ = io.WriteString(w, `{"code":10002,"message":"bad signature"}`)
					return
				}
				if r.Method == http.MethodGet {
					if query != "user_ids=u___1%2Cag__2" {
						t.Errorf("wire query = %q", query)
					}
					_, _ = io.WriteString(w, `{"code":0,"data":{"users":[]}}`)
					return
				}
				if len(body) == 0 {
					t.Error("missing signed body")
				}
				_, _ = io.WriteString(w, `{"code":0,"data":{}}`)
			}))
			t.Cleanup(srv.Close)
			client := sdk.New(srv.URL+tc.prefix, sdk.WithInternalAuth("platform", "secret"), sdk.WithPlatformId(5))
			if _, err := client.InternalUsers(t.Context(), []string{"u___1", "ag__2"}); err != nil {
				t.Error(err)
			}
			if _, err := client.InternalUpsertUser(t.Context(), sdk.UpsertUserRequest{Id: "u___1", Nickname: "中文"}); err != nil {
				t.Error(err)
			}
		})
	}
}
