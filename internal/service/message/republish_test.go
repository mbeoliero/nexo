package message

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/msgbody"
)

type retryLookupStore struct {
	Store
	missFirst bool
	lookups   int
	inTx      bool
	afterRead func()
}

func (s *retryLookupStore) GetMessageByClientId(
	ctx context.Context, conversationId, senderId, clientMsgId string,
) (*store.Message, error) {
	s.lookups++
	if s.missFirst && s.lookups == 1 {
		// Model the lookup racing another request that committed before our INSERT.
		return nil, store.ErrNotFound
	}
	msg, err := s.Store.GetMessageByClientId(ctx, conversationId, senderId, clientMsgId)
	if err == nil && s.afterRead != nil {
		s.afterRead()
	}
	return msg, err
}

func (s *retryLookupStore) WithTx(ctx context.Context, fn func(Tx) error) error {
	s.inTx = true
	defer func() { s.inTx = false }()
	return s.Store.WithTx(ctx, fn)
}

func TestSendRepublishesStoredMessage(t *testing.T) {
	for _, tt := range []struct {
		name      string
		missFirst bool
		session   int32
	}{
		{name: "fast single", session: store.ConversationSingle},
		{name: "transaction single", missFirst: true, session: store.ConversationSingle},
		{name: "fast group", session: store.ConversationGroup},
		{name: "transaction group", missFirst: true, session: store.ConversationGroup},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			pusher := &fakePusher{}
			svc, st, pub := setup(t, Config{Online: onlineStub{}, Pusher: pusher})
			now := store.NowMs()
			svc.now = func() time.Time { return now }
			t.Cleanup(func() { pusher.wait(t, svc) })
			in := single("original", `{"text":"original"}`)
			in.SenderConnId, in.SenderRead = "original-connection", false
			if tt.session == store.ConversationGroup {
				in.SessionType, in.GroupId, in.RecvId = tt.session, "g1", ""
			}
			first, err := svc.Send(ctx, in)
			if err != nil {
				t.Fatal(err)
			}
			if calls := pusher.wait(t, svc); len(calls) != 1 {
				t.Fatalf("first send offline calls: %v", calls)
			}
			before := snapshotSend(t, ctx, st, first.ConversationId, []string{"u___1", "u___2"})
			retries := &retryLookupStore{Store: svc.store, missFirst: tt.missFirst}
			svc.store = retries
			svc.pub = PublisherFunc(func(ctx context.Context, ev PushEvent) {
				if retries.inTx {
					t.Error("republished before the transaction ended")
				}
				pub.Publish(ctx, ev)
			})
			now = now.Add(30 * 24 * time.Hour)
			in.SenderConnId, in.SenderRead = "retry-connection", true
			in.ContentType, in.Content = msgbody.Image, `{"url":"changed"}`
			again, err := svc.Send(ctx, in)
			if err != nil || again != first {
				t.Fatalf("retry changed ACK: %+v, %v; want %+v", again, err, first)
			}
			if svc.RepublishCount() != 1 || len(pub.events) != 2 {
				t.Fatalf("republish attempts=%d events=%+v", svc.RepublishCount(), pub.events)
			}
			want := pub.events[0]
			want.SenderConnId = in.SenderConnId
			if pub.events[1] != want {
				t.Fatalf("retry replaced stored message: %+v; want %+v", pub.events[1], want)
			}
			if calls := pusher.wait(t, svc); len(calls) != 1 {
				t.Fatalf("retry ran offline push again: %v", calls)
			}
			after := snapshotSend(t, ctx, st, first.ConversationId, []string{"u___1", "u___2"})
			if !reflect.DeepEqual(after, before) {
				t.Fatalf("retry changed persisted message or conversation: before=%+v after=%+v", before, after)
			}
		})
	}
}

func TestSendRepublishSharesQuotaAndPreservesAck(t *testing.T) {
	svc, _, pub := setup(t, Config{SendPerMin: 2})
	now := store.NowMs()
	svc.now = func() time.Time { return now }
	in := single("same", `{}`)
	first, err := svc.Send(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	if again, err := svc.Send(t.Context(), in); err != nil || again != first {
		t.Fatalf("first retry: %+v, %v; want %+v", again, err, first)
	}
	if svc.RepublishCount() != 1 || len(pub.events) != 2 {
		t.Fatalf("first retry did not publish: count=%d events=%+v", svc.RepublishCount(), pub.events)
	}
	if got, err := svc.Send(t.Context(), single("new", `{}`)); !errors.Is(err, errcode.ErrTooManyRequests) || got != (Ack{}) {
		t.Fatalf("republish did not consume shared quota: %+v, %v", got, err)
	}
	if again, err := svc.Send(t.Context(), in); err != nil || again != first {
		t.Fatalf("limited retry lost its committed ACK: %+v, %v; want %+v", again, err, first)
	}
	if svc.RepublishCount() != 1 || len(pub.events) != 2 {
		t.Fatalf("limited retry published: count=%d events=%+v", svc.RepublishCount(), pub.events)
	}
	in.Unlimited = true
	if again, err := svc.Send(t.Context(), in); err != nil || again != first {
		t.Fatalf("unlimited retry: %+v, %v; want %+v", again, err, first)
	}
	if svc.RepublishCount() != 2 || len(pub.events) != 3 {
		t.Fatalf("unlimited retry did not publish: count=%d events=%+v", svc.RepublishCount(), pub.events)
	}
	now = now.Add(30 * time.Second) // Refill one token without replacing the limiter.
	if again, err := svc.Send(t.Context(), in); err != nil || again != first {
		t.Fatalf("unlimited retry with available quota: %+v, %v; want %+v", again, err, first)
	}
	if got, err := svc.Send(t.Context(), single("new", `{}`)); err != nil || got.Seq != 2 {
		t.Fatalf("unlimited retry consumed available quota: %+v, %v", got, err)
	}
}

func TestSendTransactionRepublishChargesOnce(t *testing.T) {
	for _, tt := range []struct {
		name      string
		unlimited bool
	}{
		{name: "limited"},
		{name: "unlimited", unlimited: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, pub := setup(t, Config{SendPerMin: 2})
			now := store.NowMs()
			svc.now = func() time.Time { return now }
			in := single("same", `{}`)
			first, err := svc.Send(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			now = now.Add(30 * time.Second) // Restore the token consumed by the seed send.
			svc.store = &retryLookupStore{Store: svc.store, missFirst: true}
			in.Unlimited = tt.unlimited
			if again, err := svc.Send(t.Context(), in); err != nil || again != first {
				t.Fatalf("transaction retry: %+v, %v; want %+v", again, err, first)
			}
			if svc.RepublishCount() != 1 || len(pub.events) != 2 {
				t.Fatalf("transaction retry did not republish: count=%d events=%+v", svc.RepublishCount(), pub.events)
			}
			if got, err := svc.Send(t.Context(), single("next", `{}`)); err != nil || got.Seq != 2 {
				t.Fatalf("transaction retry consumed more than one quota: %+v, %v", got, err)
			}
			got, err := svc.Send(t.Context(), single("last", `{}`))
			if tt.unlimited {
				if err != nil || got.Seq != 3 {
					t.Fatalf("unlimited retry consumed quota: %+v, %v", got, err)
				}
			} else if !errors.Is(err, errcode.ErrTooManyRequests) || got != (Ack{}) {
				t.Fatalf("transaction retry consumed no quota: %+v, %v", got, err)
			}
		})
	}
}

type failingRepublishBus struct {
	bus.Bus
	calls  int
	ctxErr error
	budget time.Duration
}

func (b *failingRepublishBus) Publish(ctx context.Context, _ bus.Event) error {
	b.calls++
	b.ctxErr = ctx.Err()
	if deadline, ok := ctx.Deadline(); ok {
		b.budget = time.Until(deadline)
	}
	return errors.New("injected publish failure")
}

func TestSendRepublishFailureKeepsAck(t *testing.T) {
	for _, tt := range []struct {
		name      string
		missFirst bool
	}{
		{name: "fast"},
		{name: "transaction", missFirst: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc, _, _ := setup(t, Config{})
			in := single("same", `{}`)
			first, err := svc.Send(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			svc.store = &retryLookupStore{Store: svc.store, missFirst: tt.missFirst, afterRead: cancel}
			b := &failingRepublishBus{}
			svc.pub = NewBusPublisher(b, "retry-node")
			again, err := svc.Send(ctx, in)
			if err != nil || again != first {
				t.Fatalf("failed publish changed committed ACK: %+v, %v; want %+v", again, err, first)
			}
			if !errors.Is(ctx.Err(), context.Canceled) || b.ctxErr != nil || b.budget <= 0 || b.budget > publishTimeout {
				t.Fatalf("republish must survive request cancellation with a budget: request=%v publish=%v budget=%v", ctx.Err(), b.ctxErr, b.budget)
			}
			if b.calls != 1 || svc.RepublishCount() != 1 {
				t.Fatalf("failed attempt was not counted once: calls=%d count=%d", b.calls, svc.RepublishCount())
			}
		})
	}
}

func TestSendConcurrentRepublishCount(t *testing.T) {
	svc, st, pub := setup(t, Config{})
	in := single("same", `{}`)
	first, err := svc.Send(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	const retries = 20
	var wg sync.WaitGroup
	for range retries {
		wg.Go(func() {
			if again, err := svc.Send(t.Context(), in); err != nil || again != first {
				t.Errorf("concurrent retry: %+v, %v; want %+v", again, err, first)
			}
		})
	}
	wg.Wait()
	if svc.RepublishCount() != retries || len(pub.events) != retries+1 {
		t.Fatalf("concurrent attempts: count=%d events=%d", svc.RepublishCount(), len(pub.events))
	}
	if other := New(Adapt(st), NoopPublisher{}, Config{MaxContentBytes: 64}); other.RepublishCount() != 0 {
		t.Fatalf("republish counter leaked between service instances: %d", other.RepublishCount())
	}
}

// An idempotent hit still fans the stored message out, so the group sender has to be re-authorized:
// otherwise someone removed from a group keeps publishing into it by retrying an old client_msg_id.
// A denied republish costs no quota, and the committed ACK is returned either way (design §8.4).
func TestSendGroupRepublishRechecksMembership(t *testing.T) {
	for _, tt := range []struct {
		name      string
		revoke    func(*testing.T, *storetest.Mem)
		republish bool
	}{
		{name: "still a member", revoke: func(*testing.T, *storetest.Mem) {}, republish: true},
		{name: "removed from the group", revoke: func(t *testing.T, m *storetest.Mem) {
			if _, err := m.RemoveGroupMember(t.Context(), "g1", "u___2"); err != nil {
				t.Fatal(err)
			}
		}},
		{name: "group dismissed", revoke: func(t *testing.T, m *storetest.Mem) {
			g, err := m.GetGroup(t.Context(), "g1")
			if err != nil {
				t.Fatal(err)
			}
			g.Status = store.GroupStatusDismissed
			m.SetGroup(*g)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc, mem, pub := setup(t, Config{SendPerMin: 2})
			in := SendInput{
				SenderId: "u___2", ClientMsgId: "same", SessionType: store.ConversationGroup,
				GroupId: "g1", ContentType: msgbody.Text, Content: `{}`, SenderRead: true,
			}
			first, err := svc.Send(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			tt.revoke(t, mem)
			in.SenderConnId = "retry-connection"
			again, err := svc.Send(t.Context(), in)
			if err != nil || again != first {
				t.Fatalf("retry changed the committed ACK: %+v, %v; want %+v", again, err, first)
			}
			want := 0
			if tt.republish {
				want = 1
			}
			if svc.RepublishCount() != int64(want) || len(pub.events) != 1+want {
				t.Fatalf("republish attempts=%d events=%+v, want %d attempts", svc.RepublishCount(), pub.events, want)
			}
			// The sender's remaining quota says whether the republish was charged: only one send is
			// left, and a single chat stays reachable after the group has rejected them.
			probe := SendInput{
				SenderId: "u___2", ClientMsgId: "probe", SessionType: store.ConversationSingle,
				RecvId: "u___1", ContentType: msgbody.Text, Content: `{}`, SenderRead: true,
			}
			got, err := svc.Send(t.Context(), probe)
			if tt.republish {
				if !errors.Is(err, errcode.ErrTooManyRequests) || got != (Ack{}) {
					t.Fatalf("republish did not consume quota: %+v, %v", got, err)
				}
			} else if err != nil || got.Seq != 1 {
				t.Fatalf("denied republish consumed quota: %+v, %v", got, err)
			}
		})
	}
}

type countingGroupStore struct {
	Store
	groups  int
	members int
}

func (s *countingGroupStore) GetGroup(ctx context.Context, id string) (*store.Group, error) {
	s.groups++
	return s.Store.GetGroup(ctx, id)
}

func (s *countingGroupStore) GetGroupMember(ctx context.Context, groupId, userId string) (*store.GroupMember, error) {
	s.members++
	return s.Store.GetGroupMember(ctx, groupId, userId)
}

// The idempotent group fast path re-checks the sender before republishing (design §8.4), but
// normalizeDestination has already read chat_groups microseconds earlier in the same request and the
// re-check is not under the conversation row lock, so a second read would buy no freshness: one
// group read per retry, not two. Freshness still comes from group_members, which is read now.
func TestSendGroupRetryReadsGroupOnce(t *testing.T) {
	for _, tt := range []struct {
		name      string
		remove    bool
		republish bool
	}{
		{name: "still a member", republish: true},
		{name: "removed before the retry", remove: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			svc, mem, pub := setup(t, Config{})
			in := SendInput{
				SenderId: "u___2", ClientMsgId: "same", SessionType: store.ConversationGroup,
				GroupId: "g1", ContentType: msgbody.Text, Content: `{}`, SenderRead: true,
			}
			first, err := svc.Send(t.Context(), in)
			if err != nil {
				t.Fatal(err)
			}
			if tt.remove {
				if _, err := mem.RemoveGroupMember(t.Context(), "g1", "u___2"); err != nil {
					t.Fatal(err)
				}
			}
			counter := &countingGroupStore{Store: svc.store}
			svc.store = counter
			in.SenderConnId = "retry-connection"
			again, err := svc.Send(t.Context(), in)
			if err != nil || again != first {
				t.Fatalf("retry changed the committed ACK: %+v, %v; want %+v", again, err, first)
			}
			if counter.groups != 1 || counter.members != 1 {
				t.Fatalf("retry read chat_groups=%d group_members=%d; want 1 and 1", counter.groups, counter.members)
			}
			want := 0
			if tt.republish {
				want = 1
			}
			if svc.RepublishCount() != int64(want) || len(pub.events) != 1+want {
				t.Fatalf("republish attempts=%d events=%d; want %d", svc.RepublishCount(), len(pub.events), want)
			}
		})
	}
}
