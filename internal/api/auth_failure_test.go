package api

import (
	"context"
	"encoding/json/v2"
	"errors"
	"maps"
	"net/http"
	"strings"
	"testing"
	"time"

	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/golang-jwt/jwt/v5"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/cache"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

var errAuthDependency = errors.New("injected auth cache failure")

type authFailureCache struct {
	cache.Cache
	fail  string
	calls map[string]int
}

func (c *authFailureCache) before(op string) error {
	c.calls[op]++
	if c.fail == op {
		return errAuthDependency
	}
	return nil
}

func (c *authFailureCache) Get(ctx context.Context, key string) (string, bool, error) {
	if err := c.before("get"); err != nil {
		return "", false, err
	}
	return c.Cache.Get(ctx, key)
}

func (c *authFailureCache) Set(ctx context.Context, key, val string, ttl time.Duration) error {
	if err := c.before("set"); err != nil {
		return err
	}
	return c.Cache.Set(ctx, key, val, ttl)
}

func (c *authFailureCache) Del(ctx context.Context, keys ...string) error {
	if err := c.before("del"); err != nil {
		return err
	}
	return c.Cache.Del(ctx, keys...)
}

func (c *authFailureCache) DelIfValue(ctx context.Context, key, expected string) error {
	if err := c.before("delifvalue"); err != nil {
		return err
	}
	return c.Cache.DelIfValue(ctx, key, expected)
}

func (c *authFailureCache) SetNX(ctx context.Context, key, val string, ttl time.Duration) (bool, error) {
	if err := c.before("setnx"); err != nil {
		return false, err
	}
	return c.Cache.SetNX(ctx, key, val, ttl)
}

const authFailureLogin = `{"username":"alice","password":"secret1","platform_id":5}`

func newAuthFailureEngine(t *testing.T) (*route.Engine, *authFailureCache, user.Session) {
	t.Helper()
	c := &authFailureCache{Cache: local.New(), calls: map[string]int{}}
	t.Cleanup(func() { _ = c.Close() })
	native := auth.NewNative("native-secret", time.Hour, tokenstore.New(c))
	// A later provider's invalid-token result must not hide a native dependency failure.
	chain := auth.Chain{native, auth.NewExternal([]string{"ext"}, "user")}
	mem := storetest.NewMem()
	svc := user.New(mem, native)
	cfg := config.Default()
	cfg.InternalAuth.RequireTls = false
	g := gateway.New(cfg, gateway.Deps{Auth: chain})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); err != nil {
			t.Error(err)
		}
	})
	e := route.NewEngine(hconfig.NewOptions(nil))
	registerRoutes(e.Group(""), cfg, Deps{
		Auth: chain, NativeLogin: true, User: svc,
		Conv:     conversation.New(mem, conversation.NoopNotifier{}),
		Internal: auth.NewInternal([]string{"secret"}, []string{"gateway"}, time.Minute, c),
	}, nil)
	e.GET("/ws", g.Handle)
	if _, err := svc.Register(t.Context(), "alice", "secret1", "Alice"); err != nil {
		t.Fatal(err)
	}
	sess, err := svc.Login(t.Context(), "alice", "secret1", 5)
	if err != nil || sess.Token == "" {
		t.Fatalf("prepare native session: %v", err)
	}
	clear(c.calls)
	return e, c, sess
}

func checkAuthResponse(t *testing.T, status int, env envelope, wantStatus, wantCode int) {
	t.Helper()
	if status != wantStatus || env.Code != wantCode {
		t.Fatalf("auth response: HTTP/code=%d/%d, want %d/%d", status, env.Code, wantStatus, wantCode)
	}
	if wantCode != 0 {
		if env.Message == "" || strings.Contains(env.Message, errAuthDependency.Error()) {
			t.Error("error response must contain a public message without the dependency cause")
		}
		if string(env.Data) != "{}" {
			t.Error("error response must not expose session or business data")
		}
	}
}

func TestNativeAuthReadFailure(t *testing.T) {
	for _, tc := range []struct {
		name, method, path string
		queryToken         bool
	}{
		{name: "bearer", method: "GET", path: "/api/v1/user/me"},
		{name: "logout bearer before delete", method: "POST", path: "/api/v1/auth/logout"},
		{name: "ws bearer", method: "GET", path: "/ws?platform_id=5"},
		{name: "ws query", method: "GET", path: "/ws?platform_id=5", queryToken: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, c, sess := newAuthFailureEngine(t)
			path, token := tc.path, sess.Token
			if tc.queryToken {
				path += "&token=" + token
				token = ""
			}
			c.fail = "get"
			status, env := call(t, e, tc.method, path, "", token)
			checkAuthResponse(t, status, env, http.StatusServiceUnavailable, 20101)
			if !maps.Equal(c.calls, map[string]int{"get": 1}) {
				t.Fatalf("read failure must stop before any write: %v", c.calls)
			}

			c.fail = ""
			status, env = call(t, e, "GET", "/api/v1/user/me", "", sess.Token)
			checkAuthResponse(t, status, env, http.StatusOK, 0)
			status, env = call(t, e, "POST", "/api/v1/auth/logout", "", sess.Token)
			checkAuthResponse(t, status, env, http.StatusOK, 0)
			// A real revocation remains 401, including both WS token transports.
			status, env = call(t, e, tc.method, path, "", token)
			checkAuthResponse(t, status, env, http.StatusUnauthorized, 10102)
		})
	}
}

func TestNativeAuthWriteFailure(t *testing.T) {
	for _, tc := range []struct {
		name, path, body, fault string
		calls                   map[string]int
	}{
		{
			name: "login set", path: "/api/v1/auth/login", body: authFailureLogin,
			fault: "set", calls: map[string]int{"set": 1},
		},
		{
			name: "logout delete", path: "/api/v1/auth/logout",
			fault: "delifvalue", calls: map[string]int{"get": 1, "delifvalue": 1},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, c, sess := newAuthFailureEngine(t)
			c.fail = tc.fault
			status, env := call(t, e, "POST", tc.path, tc.body, sess.Token)
			checkAuthResponse(t, status, env, http.StatusInternalServerError, 20001)
			if !maps.Equal(c.calls, tc.calls) {
				t.Fatalf("wrong failure stage: calls=%v, want %v", c.calls, tc.calls)
			}
			c.fail = ""
			status, env = call(t, e, "GET", "/api/v1/user/me", "", sess.Token)
			checkAuthResponse(t, status, env, http.StatusOK, 0)
			status, env = call(t, e, "POST", tc.path, tc.body, sess.Token)
			checkAuthResponse(t, status, env, http.StatusOK, 0)
			if tc.fault == "set" {
				var renewed user.Session
				if err := json.Unmarshal(env.Data, &renewed); err != nil || renewed.Token == "" {
					t.Fatalf("recovered login must return a session: %v", err)
				}
				status, env = call(t, e, "GET", "/api/v1/user/me", "", renewed.Token)
				checkAuthResponse(t, status, env, http.StatusOK, 0)
			}
			status, env = call(t, e, "GET", "/api/v1/user/me", "", sess.Token)
			checkAuthResponse(t, status, env, http.StatusUnauthorized, 10102)
		})
	}
}

func TestInternalNonceFailure(t *testing.T) {
	for _, tc := range []struct {
		name, path string
		headers    []ut.Header
	}{
		{name: "signature only", path: "/api/v1/internal/health"},
		{
			name: "as user", path: "/api/v1/internal/conversation/list",
			headers: []ut.Header{{Key: "X-User-Id", Value: "u___1"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, c, _ := newAuthFailureEngine(t)
			c.fail = "setnx"
			status, env := signedCall(t, e, "secret", "GET", tc.path, "", "", tc.headers...)
			checkAuthResponse(t, status, env, http.StatusInternalServerError, 20002)
			if !maps.Equal(c.calls, map[string]int{"setnx": 1}) {
				t.Fatalf("signed request must reach nonce storage: %v", c.calls)
			}
			clear(c.calls)
			status, env = signedCall(t, e, "wrong", "GET", tc.path, "", "", tc.headers...)
			checkAuthResponse(t, status, env, http.StatusUnauthorized, 10002)
			if len(c.calls) != 0 {
				t.Fatalf("bad signature must not access nonce storage: %v", c.calls)
			}
			c.fail = ""
			status, env = signedCall(t, e, "secret", "GET", tc.path, "", "", tc.headers...)
			checkAuthResponse(t, status, env, http.StatusOK, 0)
		})
	}
}

func TestExternalAuthDuringCacheFailure(t *testing.T) {
	e, c, _ := newAuthFailureEngine(t)
	token, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"user_id": 1, "exp": time.Now().Add(time.Hour).Unix(),
	}).SignedString([]byte("ext"))
	if err != nil {
		t.Fatal(err)
	}
	c.fail = "get"
	status, env := call(t, e, "GET", "/api/v1/user/info?user_ids=u___1", "", token)
	checkAuthResponse(t, status, env, http.StatusOK, 0)
	if len(c.calls) != 0 {
		t.Fatalf("external verification must not consult Cache: %v", c.calls)
	}
}
