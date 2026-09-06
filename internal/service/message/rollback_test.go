package message

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/store"
)

type sendRollbackStore struct {
	store.Store
	afterInsert bool
	fail        func(store.Store) error
}

func (s sendRollbackStore) WithTx(ctx context.Context, fn func(store.Store) error) error {
	return s.Store.WithTx(ctx, func(tx store.Store) error {
		err := fn(sendRollbackStore{Store: tx, afterInsert: s.afterInsert, fail: s.fail})
		if err == nil && !s.afterInsert {
			return s.fail(tx)
		}
		return err
	})
}

func (s sendRollbackStore) InsertMessage(ctx context.Context, msg *store.Message) (bool, error) {
	inserted, err := s.Store.InsertMessage(ctx, msg)
	if err == nil && inserted && s.afterInsert {
		return inserted, s.fail(s.Store)
	}
	return inserted, err
}

type sendSnapshot struct {
	conversation *store.Conversation
	users        []*store.UserConversation
	messages     []store.Message
}

func snapshotSend(t *testing.T, ctx context.Context, st store.Store, conversationId string, owners []string) sendSnapshot {
	t.Helper()
	c, err := st.GetConversation(ctx, conversationId)
	if err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if c != nil {
		c.CreatedAt, c.UpdatedAt = c.CreatedAt.UTC(), c.UpdatedAt.UTC()
	}
	out := sendSnapshot{conversation: c}
	for _, id := range owners {
		uc, err := st.GetUserConversation(ctx, id, conversationId)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if uc != nil {
			uc.CreatedAt, uc.UpdatedAt = uc.CreatedAt.UTC(), uc.UpdatedAt.UTC()
		}
		out.users = append(out.users, uc)
	}
	out.messages, err = st.ListMessages(ctx, conversationId, 1, 100, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i := range out.messages {
		out.messages[i].CreatedAt = out.messages[i].CreatedAt.UTC()
		out.messages[i].SendTime = out.messages[i].SendTime.UTC()
	}
	return out
}

func testSendRollback(t *testing.T, st store.Store) {
	t.Helper()
	for _, session := range []struct {
		name string
		typ  int32
	}{
		{name: "single", typ: store.ConversationSingle},
		{name: "group", typ: store.ConversationGroup},
	} {
		for _, state := range []struct {
			name     string
			existing bool
		}{{name: "fresh"}, {name: "existing", existing: true}} {
			for _, stage := range []string{"after_insert", "after_conversations"} {
				t.Run(session.name+"/"+state.name+"/"+stage, func(t *testing.T) {
					testSendRollbackCase(t, st, session.typ, state.existing, stage)
				})
			}
		}
	}
}

func testSendRollbackCase(t *testing.T, st store.Store, typ int32, existing bool, stage string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	owners := []string{identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())}
	for _, id := range owners {
		if err := st.UpsertUser(ctx, &store.User{Id: id, UpdatedAt: store.NowMs()}); err != nil {
			t.Fatal(err)
		}
	}
	in := SendInput{
		SenderId: owners[0], RecvId: owners[1], SessionType: typ,
		ClientMsgId: "retry", ContentType: 1, Content: `{"text":"retry"}`, SenderRead: true,
	}
	conversationId := conv.Single(owners[0], owners[1])
	if typ == store.ConversationGroup {
		g, err := group.New(group.Adapt(st), group.NoopNotifier{}, 10).Create(ctx, owners[0], group.CreateInput{
			Name: "rollback", MemberIds: owners[1:],
		})
		if err != nil {
			t.Fatal(err)
		}
		in.GroupId, in.RecvId = g.Id, ""
		conversationId = conv.Group(g.Id)
	}
	base := store.NowMs().Add(time.Hour)
	if existing {
		seed := New(Adapt(st), NoopPublisher{}, 64)
		seed.now = func() time.Time { return base }
		first := in
		first.ClientMsgId, first.Content, first.SenderRead = "seed", `{"text":"seed"}`, false
		if a, err := seed.Send(ctx, first); err != nil || a.Seq != 1 {
			t.Fatalf("seed: %+v, %v", a, err)
		}
	}
	before := snapshotSend(t, ctx, st, conversationId, owners)
	var inside sendSnapshot
	injected := errors.New("injected send failure")
	hits := 0
	fault := sendRollbackStore{Store: st, afterInsert: stage == "after_insert", fail: func(tx store.Store) error {
		hits++
		inside = snapshotSend(t, ctx, tx, conversationId, owners)
		return injected
	}}
	pub, pusher := &recorder{}, &fakePusher{}
	svc := New(Adapt(fault), pub, 64)
	svc.now = func() time.Time { return base.Add(time.Second) }
	svc.SetOfflinePush(onlineStub{fakeOnline{}}, pusher)
	t.Cleanup(func() { pusher.wait(t, svc) })
	a, err := svc.Send(ctx, in)
	if !errors.Is(err, injected) || !errors.Is(err, errcode.ErrMessageSendFailed) || a != (Ack{}) || hits != 1 {
		t.Fatalf("injected failure: ack=%+v err=%v hits=%d", a, err, hits)
	}
	nextSeq := int64(len(before.messages) + 1)
	if len(inside.messages) != len(before.messages)+1 || inside.messages[len(inside.messages)-1].Seq != nextSeq {
		t.Fatalf("failure must follow a real insert: %+v", inside.messages)
	}
	if stage == "after_conversations" {
		if inside.conversation == nil || inside.conversation.MaxSeq != nextSeq {
			t.Fatalf("failure must follow max_seq update: %+v", inside.conversation)
		}
		for i, uc := range inside.users {
			if uc == nil || !uc.UpdatedAt.Equal(base.Add(time.Second)) || i == 0 && uc.ReadSeq != nextSeq {
				t.Fatalf("failure must follow user conversation writes: %+v", uc)
			}
		}
	}
	if got := snapshotSend(t, ctx, st, conversationId, owners); !reflect.DeepEqual(got, before) {
		t.Fatalf("rollback changed persisted state: got=%+v before=%+v", got, before)
	}
	if msg, err := st.GetMessageByClientId(ctx, conversationId, in.SenderId, in.ClientMsgId); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("failed idempotency key remains: %+v, %v", msg, err)
	}
	if calls := pusher.wait(t, svc); len(calls) != 0 || len(pub.events) != 0 {
		t.Fatalf("failed send notified: offline=%v bus=%+v", calls, pub.events)
	}

	// Disable only fault injection; retry through the same service and real Store.
	svc.store = Adapt(st)
	a, err = svc.Send(ctx, in)
	if err != nil || a.Seq != nextSeq || a.ServerMsgId == "" {
		t.Fatalf("retry consumed seq or failed: %+v, %v", a, err)
	}
	if calls := pusher.wait(t, svc); len(calls) != 1 || !slices.Equal(calls[0], owners[1:]) || len(pub.events) != 1 {
		t.Fatalf("retry notifications: offline=%v bus=%+v", calls, pub.events)
	}
	committed := snapshotSend(t, ctx, st, conversationId, owners)
	if len(committed.messages) != int(nextSeq) || !slices.Equal(committed.messages[:len(before.messages)], before.messages) {
		t.Fatalf("retry changed prior messages: %+v", committed.messages)
	}
	msg := committed.messages[len(committed.messages)-1]
	if ack(msg) != a || msg.ClientMsgId != in.ClientMsgId || msg.Content != in.Content || pub.events[0].Message != FromStore(msg) || pusher.last.EventId() != conversationId+":"+strconv.FormatInt(nextSeq, 10) {
		t.Fatalf("retry ACK, persistence and notifications disagree: ack=%+v msg=%+v", a, msg)
	}
	if committed.conversation == nil || committed.conversation.MaxSeq != nextSeq || committed.users[0] == nil || committed.users[0].ReadSeq != nextSeq {
		t.Fatalf("retry did not commit conversation state: %+v", committed)
	}
	in.Content = `{"text":"ignored retry"}`
	if again, err := svc.Send(ctx, in); err != nil || again != a {
		t.Fatalf("idempotent retry: %+v, %v; want %+v", again, err, a)
	}
	if calls := pusher.wait(t, svc); len(calls) != 1 || len(pub.events) != 1 {
		t.Fatalf("idempotent retry notified twice: offline=%v bus=%+v", calls, pub.events)
	}
	if got := snapshotSend(t, ctx, st, conversationId, owners); !reflect.DeepEqual(got, committed) {
		t.Fatal("idempotent retry changed persisted state")
	}
}
