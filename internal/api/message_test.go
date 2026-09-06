package api

import (
	"encoding/json/v2"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/common/ut"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/message"
)

func TestMessageAndConversationFlow(t *testing.T) {
	e, token := newEngine(t, engineOptions{chat: true})
	post := func(path, body, tok string) envelope {
		_, env := call(t, e, "POST", "/api/v1"+path, body, tok)
		return env
	}
	get := func(path, tok string) envelope {
		_, env := call(t, e, "GET", "/api/v1"+path, "", tok)
		return env
	}

	env := post("/message/send", `{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{\"text\":\"hi\"}"}`, token(1))
	var ack message.Ack
	if env.Code != 0 || json.Unmarshal(env.Data, &ack) != nil || ack.Seq != 1 {
		t.Fatalf("send: %+v", env)
	}
	if again := post("/message/send", `{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}`, token(1)); !strings.Contains(string(again.Data), ack.ServerMsgId) {
		t.Fatalf("idempotent: %+v", again)
	}
	if env := post("/message/send", `{"client_msg_id":"c2","session_type":1,"recv_id":"u___2","content_type":1,"content":"nope"}`, token(1)); env.Code != errcode.ErrInvalidParam.Code {
		t.Fatalf("bad content: %+v", env)
	}
	post("/message/send", `{"client_msg_id":"c3","session_type":1,"recv_id":"u___1","content_type":1,"content":"{}"}`, token(2))

	pull := get("/message/pull?conversation_id="+ack.ConversationId+"&begin_seq=1&end_seq=100", token(2))
	if pull.Code != 0 || strings.Count(string(pull.Data), `"seq"`) != 2 {
		t.Fatalf("pull: %+v", pull)
	}
	if env := get("/message/pull?conversation_id="+ack.ConversationId+"&begin_seq=1&end_seq=100", token(3)); env.Code != errcode.ErrNoPermission.Code {
		t.Fatalf("stranger pull: %+v", env)
	}
	if env := get("/message/max_seqs", token(2)); env.Code != 0 || !strings.Contains(string(env.Data), `"max_seq":2`) {
		t.Fatalf("max_seqs: %+v", env)
	}

	list := get("/conversation/list?with_last_message=true", token(1))
	if list.Code != 0 || !strings.Contains(string(list.Data), `"unread":1`) || !strings.Contains(string(list.Data), `"last_message"`) {
		t.Fatalf("list: %+v", list)
	}
	if env := post("/conversation/read", `{"conversation_id":"`+ack.ConversationId+`","read_seq":9}`, token(2)); env.Code != 0 || !strings.Contains(string(env.Data), `"read_seq":2`) {
		t.Fatalf("read clamps to visible max: %+v", env)
	}
	if _, env := call(t, e, "PUT", "/api/v1/conversation/opt", `{"conversation_id":"`+ack.ConversationId+`","recv_msg_opt":1}`, token(2)); env.Code != 0 {
		t.Fatalf("opt: %+v", env)
	}
	if list := get("/conversation/list", token(2)); !strings.Contains(string(list.Data), `"unread":0`) || !strings.Contains(string(list.Data), `"recv_msg_opt":1`) {
		t.Fatalf("list after read/opt: %+v", list)
	}

	// Platform pushes a custom message as u___3; sender_read defaults to false there.
	body := `{"client_msg_id":"p1","session_type":1,"recv_id":"u___1","content_type":100,"content":"{\"k\":1}"}`
	_, env = signedCall(t, e, "secret", "POST", "/api/v1/internal/message/send", "", body, ut.Header{Key: "X-User-Id", Value: "u___3"})
	if env.Code != 0 {
		t.Fatalf("internal send: %+v", env)
	}
	if list := get("/conversation/list", token(3)); !strings.Contains(string(list.Data), `"unread":1`) {
		t.Fatalf("platform sender should see its own message unread: %+v", list)
	}
}
