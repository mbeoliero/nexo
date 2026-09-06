package message

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type simultaneousRetries struct {
	store.Store
	lookups atomic.Int32
	ready   chan struct{}
}

func (s *simultaneousRetries) GetMessageByClientId(ctx context.Context, conversationId, senderId, clientMsgId string) (*store.Message, error) {
	msg, err := s.Store.GetMessageByClientId(ctx, conversationId, senderId, clientMsgId)
	if errors.Is(err, store.ErrNotFound) {
		switch s.lookups.Add(1) {
		case 1:
			select {
			case <-s.ready:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		case 2:
			close(s.ready)
		}
	}
	return msg, err
}

func TestMessageDatabaseRegressions(t *testing.T) {
	for _, backend := range storetest.DbBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			st := storetest.Open(t, backend, func(driver, dsn string) (store.Store, error) {
				if driver == "" {
					return pgstore.New(t.Context(), dsn, 4)
				}
				return gormstore.New(driver, dsn, 4)
			})
			owner, member := identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())
			for _, id := range []string{owner, member} {
				if err := st.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: store.NowMs()}); err != nil {
					t.Fatal(err)
				}
			}
			t.Run("pull after quit", func(t *testing.T) {
				testPullAfterQuit(t, st)
			})
			t.Run("canonical group id", func(t *testing.T) {
				if backend.Driver != "mysql" {
					t.Skip("PAD SPACE group lookup is MySQL-specific")
				}
				g, err := group.New(group.Adapt(st), group.NoopNotifier{}, 10).Create(t.Context(), owner, group.CreateInput{
					Name: "canonical group id", MemberIds: []string{member},
				})
				if err != nil {
					t.Fatal(err)
				}
				testSendCanonicalGroupId(t, st, SendInput{
					SenderId: owner, GroupId: g.Id, SessionType: store.ConversationGroup,
					ContentType: 1, Content: `{}`, SenderRead: true,
				})
			})
			t.Run("concurrent retry", func(t *testing.T) {
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				retries := &simultaneousRetries{Store: st, ready: make(chan struct{})}
				s := New(Adapt(retries), NoopPublisher{}, 64)
				in := SendInput{SenderId: owner, RecvId: member, ClientMsgId: "same", SessionType: 1, ContentType: 1, Content: `{}`}
				acks := make([]Ack, 2)
				errs := make([]error, 2)
				var wg sync.WaitGroup
				for i := range 2 {
					wg.Go(func() { acks[i], errs[i] = s.Send(ctx, in) })
				}
				wg.Wait()
				if errs[0] != nil || errs[1] != nil || acks[0] != acks[1] || acks[0].Seq != 1 {
					t.Fatalf("retries must return the same original ACK: %+v, %v", acks, errs)
				}
				in.ClientMsgId = "next"
				next, err := New(Adapt(st), NoopPublisher{}, 64).Send(ctx, in)
				if err != nil || next.Seq != 2 {
					t.Fatalf("retry must not consume seq: %+v, %v", next, err)
				}
				msgs, err := st.ListMessages(ctx, next.ConversationId, 1, 100, 100)
				if err != nil || len(msgs) != 2 || msgs[0].ServerMsgId != acks[0].ServerMsgId || msgs[1].ServerMsgId != next.ServerMsgId {
					t.Fatalf("every ACK must identify a stored message: %+v, %v", msgs, err)
				}
			})
			t.Run("send rollback", func(t *testing.T) {
				testSendRollback(t, st)
			})
			t.Run("send time monotonic", func(t *testing.T) {
				testSendTimeMonotonic(t, st)
			})
			t.Run("client message id whitespace", func(t *testing.T) {
				testSendClientMsgIdWhitespace(t, New(Adapt(st), NoopPublisher{}, 64), SendInput{
					SenderId: owner, RecvId: member, SessionType: store.ConversationSingle,
					ContentType: 1, Content: `{"text":"original"}`, Unlimited: true,
				})
			})
		})
	}
}
