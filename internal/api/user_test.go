package api

import (
	"context"
	"encoding/json/v2"
	"net/http"
	"strings"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/service/user"
)

func TestAuthAndUserFlow(t *testing.T) {
	e, _ := newEngine(t, engineOptions{nativeLogin: true})

	status, env := call(t, e, "GET", "/api/v1/user/me", "", "")
	if status != http.StatusUnauthorized || env.Code != errcode.ErrTokenMissing.Code {
		t.Fatalf("no token: %d %+v", status, env)
	}
	status, env = call(t, e, "GET", "/api/v1/user/me", "", "junk")
	if status != http.StatusUnauthorized || env.Code != errcode.ErrTokenInvalid.Code {
		t.Fatalf("bad token: %d %+v", status, env)
	}

	_, env = call(t, e, "POST", "/api/v1/auth/register", `{"username":"alice","password":"secret1","nickname":"A"}`, "")
	if env.Code != 0 {
		t.Fatalf("register: %+v", env)
	}
	_, env = call(t, e, "POST", "/api/v1/auth/login", `{"username":"alice","password":"nope","platform_id":5}`, "")
	if env.Code != errcode.ErrLoginFailed.Code {
		t.Fatalf("bad login: %+v", env)
	}
	_, env = call(t, e, "POST", "/api/v1/auth/login", `{"username":"alice","password":"secret1"}`, "")
	if env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("login without platform_id: %+v", env)
	}
	_, env = call(t, e, "POST", "/api/v1/auth/login", `{"username":"alice","password":"secret1","platform_id":5}`, "")
	var sess user.Session
	if env.Code != 0 || json.Unmarshal(env.Data, &sess) != nil || sess.Token == "" {
		t.Fatalf("login: %+v", env)
	}

	_, env = call(t, e, "GET", "/api/v1/user/me", "", sess.Token)
	var me user.Profile
	if env.Code != 0 || json.Unmarshal(env.Data, &me) != nil || me.Id != sess.UserId || me.Nickname != "A" {
		t.Fatalf("me: %+v", env)
	}
	_, env = call(t, e, "PUT", "/api/v1/user/me", `{"nickname":"B"}`, sess.Token)
	if env.Code != 0 || !strings.Contains(string(env.Data), `"nickname":"B"`) {
		t.Fatalf("update: %+v", env)
	}
	_, env = call(t, e, "GET", "/api/v1/user/info?user_ids="+sess.UserId+",u___404", "", sess.Token)
	if env.Code != 0 || strings.Count(string(env.Data), `"user_id"`) != 1 {
		t.Fatalf("info: %+v", env)
	}
	_, env = call(t, e, "GET", "/api/v1/user/info?user_ids=bad", "", sess.Token)
	if env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("info bad id: %+v", env)
	}
	// A missing user_ids must report the parameter, not fall through to per-id validation and
	// return an empty "invalid user id: ".
	_, env = call(t, e, "GET", "/api/v1/user/info", "", sess.Token)
	if env.Code != errcode.ErrInvalidParam.Code || !strings.Contains(env.Message, "comma separated") {
		t.Fatalf("info no user_ids: %+v", env)
	}

	_, env = call(t, e, "POST", "/api/v1/auth/logout", "", sess.Token)
	if env.Code != 0 {
		t.Fatalf("logout: %+v", env)
	}
	status, env = call(t, e, "GET", "/api/v1/user/me", "", sess.Token)
	if status != http.StatusUnauthorized || env.Code != errcode.ErrTokenExpired.Code {
		t.Fatalf("after logout: %d %+v", status, env)
	}
}

type stubOnline struct{}

func (stubOnline) Add(context.Context, string, onlinestore.ConnRef) error     { return nil }
func (stubOnline) Remove(context.Context, string, onlinestore.ConnRef) error  { return nil }
func (stubOnline) Renew(context.Context, string, []onlinestore.ConnRef) error { return nil }
func (stubOnline) PurgeNode(context.Context, string) error                    { return nil }
func (stubOnline) Online(_ context.Context, ids []string) (map[string][]int, error) {
	return map[string][]int{"u___1": {1, 5}}, nil
}

func TestOnlineStatus(t *testing.T) {
	e, token := newEngine(t, engineOptions{chat: true})
	_, env := call(t, e, "GET", "/api/v1/user/online_status?user_ids=u___1,u___2", "", token(1))
	if env.Code != 0 || !strings.Contains(string(env.Data), `"user_id":"u___1","online":true,"platform_ids":[1,5]`) || !strings.Contains(string(env.Data), `"user_id":"u___2","online":false,"platform_ids":[]`) {
		t.Fatalf("online_status: %+v", env)
	}
}
