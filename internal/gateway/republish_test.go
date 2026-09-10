package gateway

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestRetryOnAnotherNodeRepublishesCommittedMessage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := storetest.NewMem()
		for _, id := range []string{"u___1", "u___2"} {
			if err := mem.UpsertUser(t.Context(), &store.User{Id: id}); err != nil {
				t.Fatal(err)
			}
		}
		b := buslocal.New()
		node := func(id string, publisher message.Publisher) *Gateway {
			cfg := testConfig()
			cfg.NodeId = id
			cfg.Ws.PingInterval = time.Hour
			g := New(cfg, Deps{Bus: b, Message: message.New(message.Adapt(mem), publisher, message.Config{MaxContentBytes: 1024})})
			done := make(chan error, 1)
			go func() { done <- g.Run(t.Context()) }()
			t.Cleanup(func() { shutdownBusGateway(t, g); waitBusExit(t, done) })
			select {
			case <-g.Ready():
			case <-time.After(time.Second):
				t.Fatal("node subscription did not start")
			}
			return g
		}
		// Suppress the first publication to reproduce the committed-but-unpublished window.
		n1 := node("n1", message.NoopPublisher{})
		n2 := node("n2", message.NewBusPublisher(b, "n2"))
		oldSocket, retrySocket, peerSocket := newFakeConn(), newFakeConn(), newFakeConn()
		old := serveConn(t, n1, auth.Identity{UserId: "u___1", PlatformId: 1}, "old", oldSocket)
		serveConn(t, n2, auth.Identity{UserId: "u___1", PlatformId: 2}, "retry", retrySocket)
		serveConn(t, n1, auth.Identity{UserId: "u___2", PlatformId: 1}, "peer", peerSocket)
		in := message.SendInput{SenderId: "u___1", SenderConnId: old.Id, RecvId: "u___2", ClientMsgId: "lost",
			SessionType: 1, ContentType: 1, Content: `{"text":"original"}`}
		ack, err := n1.deps.Message.Send(t.Context(), in)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		for _, socket := range []*fakeConn{oldSocket, retrySocket, peerSocket} {
			select {
			case frame := <-socket.out:
				t.Fatalf("suppressed publication delivered a frame: %s", frame)
			default:
			}
		}
		retrySocket.in <- []byte(`{"req_id":1003,"op_id":"retry","data":{"client_msg_id":"lost","session_type":1,"recv_id":"u___2","content_type":1,"content":"{\"text\":\"changed retry body\"}"}}`)
		reply := retrySocket.next(t)
		if reply.ReqId != ReqSendMsg || reply.Code != 0 {
			t.Fatalf("retry did not return the original ACK: %+v", reply)
		}
		data := reply.Data.(map[string]any)
		if data["server_msg_id"] != ack.ServerMsgId || data["seq"] != float64(ack.Seq) {
			t.Fatalf("retry changed the ACK: %+v, want %+v", data, ack)
		}
		for _, socket := range []*fakeConn{oldSocket, peerSocket} {
			push := socket.next(t)
			if push.ReqId != PushMsg {
				t.Fatalf("retry did not repair delivery: %+v", push)
			}
			body := push.Data.(map[string]any)
			if body["server_msg_id"] != ack.ServerMsgId || body["content"] != in.Content {
				t.Fatalf("republished payload was not the stored message: %+v", body)
			}
		}
		synctest.Wait()
		select {
		case frame := <-retrySocket.out:
			t.Fatalf("the current retry connection received its own push: %s", frame)
		default:
		}
		if n2.deps.Message.RepublishCount() != 1 || n1.deps.Message.RepublishCount() != 0 {
			t.Fatal("republish attempts must belong to the retrying service instance")
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		rows, err := mem.ListMessages(ctx, ack.ConversationId, 1, 10, 10)
		if err != nil || len(rows) != 1 {
			t.Fatalf("retry inserted another message: %+v, %v", rows, err)
		}
	})
}
