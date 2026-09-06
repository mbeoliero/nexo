package gateway

import (
	"encoding/json/v2"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
)

func TestMalformedFramesSpendBudgetBeforeDecode(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.WsFramesPerSec = 1
	cfg.Ws.SendQueue = 8
	g := New(cfg, Deps{})
	socket := newFakeConn()
	c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", socket)
	c.frames.SetLimit(0) // Retain the two initial tokens, never refill during the test.
	defer c.Close("test")
	for _, raw := range []string{"{", `{"op_id":"missing-id"}`} {
		if !c.admit([]byte(raw)) {
			t.Fatal("initial invalid frame closed connection")
		}
		var r Response
		if err := json.Unmarshal(<-c.send, &r); err != nil {
			t.Fatal(err)
		}
		if r.Code != errcode.ErrInvalidProtocol.Code {
			t.Fatalf("initial error: %+v", r)
		}
	}
	for i, raw := range []string{`{"req_id":1001,"op_id":"do-not-echo","msg_incr":"secret"}`, "{", `{}`} {
		keep := c.admit([]byte(raw))
		var r Response
		if err := json.Unmarshal(<-c.send, &r); err != nil {
			t.Fatal(err)
		}
		if r.Code != errcode.ErrTooManyRequests.Code || r.ReqId != 0 || r.OpId != "" || r.MsgIncr != "" {
			t.Fatalf("over-limit frame decoded: %+v", r)
		}
		if keep != (i < 2) {
			t.Fatalf("frame %d: keep=%v", i, keep)
		}
	}
	if !socket.isClosed() {
		t.Fatal("three excess frames did not close socket")
	}
}
