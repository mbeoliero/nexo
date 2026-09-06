package message

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
	"uuid"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type pausedSendStore struct {
	store.Store
	ready   chan struct{}
	release chan struct{}
}

func (s pausedSendStore) WithTx(ctx context.Context, fn func(store.Store) error) error {
	close(s.ready)
	select {
	case <-s.release:
		return s.Store.WithTx(ctx, fn)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestSendTimeDoesNotRefillLimiter(t *testing.T) {
	s, st, _ := setup(t)
	now := store.NowMs().Add(time.Hour)
	s.now = func() time.Time { return now }
	s.SetSendRateLimit(1)
	if _, err := s.Send(t.Context(), single("first", `{}`)); err != nil {
		t.Fatal(err)
	}
	remote := New(Adapt(st), NoopPublisher{}, 64)
	remote.now = func() time.Time { return now.Add(time.Hour) }
	in := single("remote", `{}`)
	in.RecvId = "u___3"
	future, err := remote.Send(t.Context(), in)
	if err != nil {
		t.Fatal(err)
	}
	in.ClientMsgId = "local"
	if got, err := s.Send(t.Context(), in); !errors.Is(err, errcode.ErrTooManyRequests) || got != (Ack{}) {
		t.Fatalf("future conversation refilled sender bucket: %+v, %v", got, err)
	}
	c, err := st.GetConversation(t.Context(), future.ConversationId)
	if err != nil || c.MaxSeq != 1 {
		t.Fatalf("rate rejection consumed seq: %+v, %v", c, err)
	}
	now = now.Add(time.Minute)
	got, err := s.Send(t.Context(), in)
	if err != nil || got.Seq != 2 || got.SendTime != future.SendTime {
		t.Fatalf("arrival clock must refill while stored time stays clamped: %+v, %v", got, err)
	}
}

func TestSendTimeMonotonic(t *testing.T) {
	testSendTimeMonotonic(t, storetest.NewMem())
}

func testSendTimeMonotonic(t *testing.T, st store.Store) {
	t.Helper()
	for _, typ := range []int32{store.ConversationSingle, store.ConversationGroup} {
		t.Run(lo.Ternary(typ == store.ConversationSingle, "single", "group"), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			owner, member := identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())
			created := store.NowMs()
			for _, id := range []string{owner, member} {
				if err := st.UpsertUser(ctx, &store.User{Id: id, CreatedAt: created, UpdatedAt: created}); err != nil {
					t.Fatal(err)
				}
			}
			in := SendInput{SenderId: owner, RecvId: member, SessionType: typ, ClientMsgId: "delayed",
				ContentType: 1, Content: `{"text":"original"}`, SenderRead: true}
			if typ == store.ConversationGroup {
				g, err := group.New(group.Adapt(st), group.NoopNotifier{}, 10).Create(ctx, owner, group.CreateInput{Name: "clock", MemberIds: []string{member}})
				if err != nil {
					t.Fatal(err)
				}
				in.GroupId, in.RecvId = g.Id, ""
			}
			base := store.NowMs().Add(time.Hour)
			pub := &recorder{}
			paused := pausedSendStore{Store: st, ready: make(chan struct{}), release: make(chan struct{})}
			resume := sync.OnceFunc(func() { close(paused.release) })
			older := New(Adapt(paused), pub, 64)
			older.now = func() time.Time { return base }
			var delayed Ack
			var delayedErr error
			var wg sync.WaitGroup
			t.Cleanup(func() { resume(); wg.Wait() })
			wg.Go(func() { delayed, delayedErr = older.Send(ctx, in) })
			select {
			case <-paused.ready:
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			}
			writer := New(Adapt(st), pub, 64)
			now := base.Add(20 * time.Millisecond)
			writer.now = func() time.Time { return now }
			firstIn := in
			firstIn.ClientMsgId = "first"
			first, err := writer.Send(ctx, firstIn)
			if err != nil || first.Seq != 1 {
				t.Fatalf("first: %+v, %v", first, err)
			}
			resume()
			wg.Wait()
			if delayedErr != nil || delayed.Seq != 2 || delayed.SendTime != first.SendTime {
				t.Fatalf("older request committed second: first=%+v delayed=%+v, %v", first, delayed, delayedErr)
			}
			check := func(a Ack, want, memberTime time.Time) {
				t.Helper()
				msgs, err := st.GetMessages(ctx, []store.MessageKey{{ConversationId: a.ConversationId, Seq: a.Seq}})
				if err != nil || len(msgs) != 1 || a.SendTime != want.UnixMilli() || !msgs[0].SendTime.Equal(want) || !msgs[0].CreatedAt.Equal(want) {
					t.Fatalf("persisted message/ACK time: ack=%+v messages=%+v want=%v, %v", a, msgs, want, err)
				}
				c, err := st.GetConversation(ctx, a.ConversationId)
				if err != nil || c.MaxSeq != a.Seq || !c.UpdatedAt.Equal(want) {
					t.Fatalf("conversation time: %+v want=%v, %v", c, want, err)
				}
				for _, id := range []string{owner, member} {
					uc, err := st.GetUserConversation(ctx, id, a.ConversationId)
					wantTime := lo.Ternary(id == member, memberTime, want)
					if err != nil || !uc.UpdatedAt.Equal(wantTime) || id == owner && uc.ReadSeq != a.Seq || id == member && uc.ReadSeq != 0 {
						t.Fatalf("user conversation: %+v want time=%v, %v", uc, wantTime, err)
					}
				}
				if len(pub.events) != int(a.Seq) || pub.events[a.Seq-1].Message.SendTime != a.SendTime {
					t.Fatalf("published time differs from ACK: %+v, %+v", pub.events, a)
				}
			}
			check(delayed, now, now)

			// A join on a faster node can leave a personal sort key ahead of the seq row.
			uc, err := st.GetUserConversation(ctx, member, first.ConversationId)
			if err != nil {
				t.Fatal(err)
			}
			uc.UpdatedAt = base.Add(100 * time.Millisecond)
			if err := st.UpsertUserConversation(ctx, uc); err != nil {
				t.Fatal(err)
			}
			now = base.Add(-time.Second)
			rollbackIn := in
			rollbackIn.ClientMsgId = "rollback"
			rolled, err := writer.Send(ctx, rollbackIn)
			if err != nil || rolled.Seq != 3 {
				t.Fatalf("clock rollback: %+v, %v", rolled, err)
			}
			check(rolled, base.Add(20*time.Millisecond), uc.UpdatedAt)
			now = base.Add(200 * time.Millisecond)
			forwardIn := in
			forwardIn.ClientMsgId = "forward"
			forward, err := writer.Send(ctx, forwardIn)
			if err != nil || forward.Seq != 4 {
				t.Fatalf("clock advances: %+v, %v", forward, err)
			}
			check(forward, now, now)
			now = base.Add(24 * time.Hour)
			retry, err := writer.Send(ctx, in)
			if err != nil || retry != delayed {
				t.Fatalf("retry changed original ACK: %+v want=%+v, %v", retry, delayed, err)
			}
			check(forward, base.Add(200*time.Millisecond), base.Add(200*time.Millisecond))
		})
	}
}
