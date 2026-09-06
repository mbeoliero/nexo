package api

import (
	"net/http"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/mbeoliero/nexo/errcode"
)

func TestInternalUpsert(t *testing.T) {
	e, _ := newEngine(t, engineOptions{})
	upsert := "/api/v1/internal/user/upsert"

	status, env := signedCall(t, e, "wrong", "POST", upsert, "", `{"id":"u___1"}`)
	if status != http.StatusUnauthorized || env.Code != errcode.ErrUnauthorized.Code {
		t.Fatalf("bad signature: %d %+v", status, env)
	}
	if _, env := signedCall(t, e, "secret", "POST", upsert, "", `{"id":"nx__1","nickname":"x"}`); env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("native id must be rejected: %+v", env)
	}
	if _, env := signedCall(t, e, "secret", "POST", upsert, "", `{"id":"u___1","nickname":"A","avatar":"http://a"}`); env.Code != 0 {
		t.Fatalf("upsert: %+v", env)
	}
	if _, env := signedCall(t, e, "secret", "POST", upsert, "", `{"id":"u___1","nickname":"B"}`); env.Code != 0 || !strings.Contains(string(env.Data), `"nickname":"B"`) {
		t.Fatalf("upsert again: %+v", env)
	}
	if _, env := signedCall(t, e, "secret", "GET", "/api/v1/internal/user/info", "user_ids=u___1,ag__2", ""); env.Code != 0 || strings.Count(string(env.Data), `"user_id"`) != 1 {
		t.Fatalf("info: %+v", env)
	}
	status, env = signedCall(
		t,
		e,
		"secret",
		"GET",
		"/api/v1/internal/health",
		"",
		"",
	)
	validHealth := env.Code == 0 && env.Message == "" && string(env.Data) == `{"status":"ok"}`
	if status != http.StatusOK || !validHealth {
		t.Fatalf("health: status=%d envelope=%+v", status, env)
	}
}

func TestInternalRequireTls(t *testing.T) {
	// ut has no connection, so the peer address is hertz's zero addr; the second engine trusts it.
	const peer = "0.0.0.0/32"
	forwarded := ut.Header{Key: "X-Forwarded-Proto", Value: "https"}

	e, _ := newEngine(t, engineOptions{requireTls: true})
	status, env := signedCall(t, e, "secret", "GET", "/api/v1/internal/health", "", "")
	if status != http.StatusForbidden || env.Code != errcode.ErrForbidden.Code {
		t.Fatalf("plain http: %d %+v", status, env)
	}
	// A signed request is not a trusted one: without server.trusted_proxies the header is a forgery
	// any caller can add, so it must not satisfy require_tls.
	if status, _ := signedCall(t, e, "secret", "GET", "/api/v1/internal/health", "", "", forwarded); status != http.StatusForbidden {
		t.Fatalf("forwarded https from an untrusted peer: %d", status)
	}

	behindProxy, _ := newEngine(t, engineOptions{requireTls: true, trustedProxies: []string{peer}})
	if status, _ := signedCall(t, behindProxy, "secret", "GET", "/api/v1/internal/health", "", "", forwarded); status != http.StatusOK {
		t.Fatalf("forwarded https from a trusted proxy: %d", status)
	}
	if status, _ := signedCall(t, behindProxy, "secret", "GET", "/api/v1/internal/health", "", ""); status != http.StatusForbidden {
		t.Fatalf("plain http behind a proxy: %d", status)
	}
}
