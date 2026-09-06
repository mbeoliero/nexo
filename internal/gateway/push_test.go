package gateway

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

// newPushGateway wires services into the gateway the way app.Build does.
func newPushGateway(t *testing.T) (*Gateway, *group.Service) {
	t.Helper()
	m := storetest.NewMem()
	for _, id := range []string{"u___1", "u___2", "u___3"} {
		_ = m.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: time.Now()})
	}
	var g *Gateway
	msg := message.New(message.Adapt(m), message.PublisherFunc(func(ctx context.Context, ev message.PushEvent) { g.Deliver(ctx, ev) }), 8192)
	conv := conversation.New(m, conversation.NotifierFunc(func(ctx context.Context, ev conversation.ReadEvent) {
		g.ConversationRead(ctx, ev.UserId, ev.ReaderConnId, ev.ConversationId, ev.ReadSeq)
	}))
	g = New(testConfig(), Deps{Auth: auth.NewExternal([]string{"ext"}, "user"), Message: msg, Conv: conv})
	return g, group.New(group.Adapt(m), group.NoopNotifier{}, 10)
}

func serveOn(t *testing.T, g *Gateway, userId string, platform int) *fakeConn {
	t.Helper()
	f := newFakeConn()
	id := auth.Identity{UserId: userId, PlatformId: platform, TokenId: "tok-" + userId}
	serveConn(t, g, id, fmt.Sprintf("conn-%s-%d", userId, platform), f)
	return f
}

func (f *fakeConn) quiet(t *testing.T) {
	t.Helper()
	select {
	case b := <-f.out:
		t.Fatalf("unexpected frame: %s", b)
	case <-time.After(100 * time.Millisecond):
	}
}

func TestSinglePushReachesPeerAndOtherDevices(t *testing.T) {
	g, _ := newPushGateway(t)
	sender := serveOn(t, g, "u___1", 1)
	senderOther := serveOn(t, g, "u___1", 2)
	peer := serveOn(t, g, "u___2", 1)
	stranger := serveOn(t, g, "u___3", 1)

	sender.in <- []byte(`{"req_id":1003,"op_id":"o","data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)
	if r := sender.next(t); r.Code != 0 || r.ReqId != 1003 {
		t.Fatalf("ack: %+v", r)
	}
	for name, f := range map[string]*fakeConn{"peer": peer, "senderOther": senderOther} {
		r := f.next(t)
		if r.ReqId != PushMsg || r.OpId == "" || !strings.Contains(fmtData(r), "client_msg_id:c1") || !strings.Contains(fmtData(r), "seq:1") {
			t.Fatalf("%s push: %+v", name, r)
		}
	}
	sender.quiet(t) // the sending connection already has the ACK
	stranger.quiet(t)
}

func fmtData(r Response) string {
	b, _ := json.Marshal(r.Data)
	return strings.NewReplacer(`"`, "", ":", ":").Replace(string(b))
}

func TestGroupPushHonoursMembershipAndVisibility(t *testing.T) {
	g, groups := newPushGateway(t)
	info, err := groups.Create(t.Context(), "u___1", group.CreateInput{Name: "g", MemberIds: []string{"u___2"}})
	if err != nil {
		t.Fatal(err)
	}
	sender := serveOn(t, g, "u___1", 1)
	member := serveOn(t, g, "u___2", 1)
	outsider := serveOn(t, g, "u___3", 1)

	send := func(id string) {
		sender.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"` + id + `","session_type":2,"group_id":"` + info.Id + `","content_type":1,"content":"{}"}}`)
		if r := sender.next(t); r.Code != 0 {
			t.Fatalf("ack: %+v", r)
		}
	}
	send("g1")
	if r := member.next(t); r.ReqId != PushMsg || !strings.Contains(fmtData(r), "seq:1") {
		t.Fatalf("member push: %+v", r)
	}
	outsider.quiet(t)

	// After quitting, u___2 is still in nobody's roster and its max_seq bounds visibility.
	if err := groups.Quit(t.Context(), info.Id, "u___2"); err != nil {
		t.Fatal(err)
	}
	send("g2")
	member.quiet(t)
}

func TestMarkReadFansOutConvRead(t *testing.T) {
	g, _ := newPushGateway(t)
	a := serveOn(t, g, "u___1", 1)
	b := serveOn(t, g, "u___1", 2)
	peer := serveOn(t, g, "u___2", 1)

	peer.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___1","content_type":1,"content":"{}"}}`)
	ack := peer.next(t)
	conv := ack.Data.(map[string]any)["conversation_id"].(string)
	a.next(t) // 2001
	b.next(t)

	a.in <- []byte(`{"req_id":1004,"op_id":"r","data":{"conversation_id":"` + conv + `","read_seq":1}}`)
	if r := a.next(t); r.ReqId != 1004 {
		t.Fatalf("device a ack: %+v", r)
	}
	a.quiet(t) // the reading device gets no 2003
	if r := b.next(t); r.ReqId != ConvRead || !strings.Contains(fmtData(r), "read_seq:1") {
		t.Fatalf("device b: %+v", r)
	}
}
