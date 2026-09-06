package api

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/group"
)

func TestGroupFlow(t *testing.T) {
	e, token := newEngine(t, engineOptions{chat: true})
	post := func(path, body, tok string) envelope {
		_, env := call(t, e, "POST", "/api/v1"+path, body, tok)
		return env
	}

	env := post("/group/create", `{"name":"team","member_ids":["u___2"]}`, token(1))
	var info group.Info
	if env.Code != 0 || json.Unmarshal(env.Data, &info) != nil || info.MemberCount != 2 {
		t.Fatalf("create: %+v", env)
	}
	gid := info.Id
	if env := post("/group/create", `{"member_ids":[]}`, token(1)); env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("create no name: %+v", env)
	}
	if env := post("/group/join", `{}`, token(3)); env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("join no id: %+v", env)
	}
	if env := post("/group/join", `{"group_id":"`+gid+`"}`, token(3)); env.Code != 0 {
		t.Fatalf("join: %+v", env)
	}
	if _, env := call(t, e, "GET", "/api/v1/group/info?group_id="+gid, "", token(3)); env.Code != 0 || !strings.Contains(string(env.Data), `"member_count":3`) {
		t.Fatalf("info: %+v", env)
	}
	if env := post("/group/kick", `{"group_id":"`+gid+`","user_id":"u___3"}`, token(2)); env.Code != errcode.ErrNotGroupAdmin.Code {
		t.Fatalf("member kick: %+v", env)
	}
	if env := post("/group/kick", `{"group_id":"`+gid+`","user_id":"u___3"}`, token(1)); env.Code != 0 {
		t.Fatalf("owner kick: %+v", env)
	}
	if _, env := call(t, e, "GET", "/api/v1/group/members?group_id="+gid, "", token(3)); env.Code != errcode.ErrNotGroupMember.Code {
		t.Fatalf("kicked user members: %+v", env)
	}
	if env := post("/group/quit", `{"group_id":"`+gid+`"}`, token(2)); env.Code != 0 {
		t.Fatalf("quit: %+v", env)
	}
	if _, env := call(t, e, "GET", "/api/v1/group/members?group_id="+gid, "", token(1)); env.Code != 0 || strings.Count(string(env.Data), `"user_id"`) != 1 {
		t.Fatalf("members: %+v", env)
	}

	// Internal channel acts as u___3: create a group owned by the platform user.
	body := `{"name":"platform-made","member_ids":["u___1"]}`
	_, env = signedCall(t, e, "secret", "POST", "/api/v1/internal/group/create", "", body, ut.Header{Key: "X-User-Id", Value: "u___3"})
	if env.Code != 0 || json.Unmarshal(env.Data, &info) != nil || !strings.Contains(string(env.Data), `"owner_id":"u___3"`) {
		t.Fatalf("internal create: %+v", env)
	}
	// The platform needs a server-to-server voluntary quit too: kick writes different semantics.
	_, env = signedCall(t, e, "secret", "POST", "/api/v1/internal/group/quit", "", `{"group_id":"`+info.Id+`"}`, ut.Header{Key: "X-User-Id", Value: "u___1"})
	if env.Code != 0 {
		t.Fatalf("internal quit: %+v", env)
	}
	if _, env := call(t, e, "GET", "/api/v1/group/members?group_id="+info.Id, "", token(3)); env.Code != 0 || strings.Count(string(env.Data), `"user_id"`) != 1 {
		t.Fatalf("after internal quit: %+v", env)
	}
	_, env = signedCall(t, e, "secret", "POST", "/api/v1/internal/group/create", "", body, ut.Header{Key: "X-User-Id", Value: "nx__abc"})
	if env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("internal as native id: %+v", env)
	}
}
