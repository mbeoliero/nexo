package conversation

import (
	"context"
	"errors"
	"sync"
	"testing"
	"uuid"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type readSnapshotStore struct {
	store.Store
	change func() error
	writes int
}

func (s *readSnapshotStore) GetUserConversation(ctx context.Context, ownerId, conversationId string) (*store.UserConversation, error) {
	uc, err := s.Store.GetUserConversation(ctx, ownerId, conversationId)
	if err != nil {
		return nil, err
	}
	if err := s.change(); err != nil {
		return nil, err
	}
	return uc, nil
}

func (s *readSnapshotStore) GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*store.UserConversationRow, error) {
	// The joint snapshot has no seam between membership and conversation reads.
	if err := s.change(); err != nil {
		return nil, err
	}
	return s.Store.GetUserConversationRow(ctx, ownerId, conversationId)
}

func (s *readSnapshotStore) AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, seq int64) error {
	s.writes++
	return s.Store.AdvanceReadSeq(ctx, ownerId, conversationId, seq)
}

func TestMarkReadQuitSnapshot(t *testing.T) {
	for _, backend := range storetest.Backends() {
		t.Run(backend.Name, func(t *testing.T) {
			for _, scenario := range []struct {
				name        string
				alreadyRead bool
			}{
				{name: "advance"},
				{name: "already-read", alreadyRead: true},
			} {
				t.Run(scenario.name, func(t *testing.T) {
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
					groups := group.New(group.Adapt(st), group.NoopNotifier{}, 10)
					g, err := groups.Create(t.Context(), owner, group.CreateInput{Name: "read snapshot", MemberIds: []string{member}})
					if err != nil {
						t.Fatal(err)
					}
					writer := message.New(message.Adapt(st), message.NoopPublisher{}, 64)
					in := message.SendInput{
						SenderId: owner, ClientMsgId: "before", SessionType: 2, GroupId: g.Id,
						ContentType: 1, Content: `{}`, Unlimited: true,
					}
					before, err := writer.Send(t.Context(), in)
					if err != nil {
						t.Fatal(err)
					}
					frozenMax := before.Seq
					if scenario.alreadyRead {
						if err := st.AdvanceReadSeq(t.Context(), member, before.ConversationId, frozenMax); err != nil {
							t.Fatal(err)
						}
					}
					changed := false
					wrapped := &readSnapshotStore{Store: st, change: sync.OnceValue(func() error {
						if err := groups.Quit(t.Context(), g.Id, member); err != nil {
							return err
						}
						in.ClientMsgId = "after"
						after, err := writer.Send(t.Context(), in)
						changed = err == nil && after.Seq > frozenMax
						return err
					})}
					rec := &recorder{}
					reader := New(wrapped, rec)
					got, err := reader.MarkRead(t.Context(), member, "reader", before.ConversationId, 100)
					if err != nil || !changed {
						t.Fatalf("interleaving did not complete: changed=%v, %v", changed, err)
					}
					if got != frozenMax {
						t.Errorf("response read_seq=%d, frozen max=%d", got, frozenMax)
					}
					uc, err := st.GetUserConversation(t.Context(), member, before.ConversationId)
					if err != nil {
						t.Fatal(err)
					}
					if uc.ReadSeq != frozenMax || uc.MaxSeq != frozenMax {
						t.Errorf("persisted read_seq=%d, max_seq=%d; frozen max=%d", uc.ReadSeq, uc.MaxSeq, frozenMax)
					}
					for _, seq := range rec.seqs {
						if seq > frozenMax {
							t.Errorf("broadcast read_seq=%d exceeds frozen max=%d", seq, frozenMax)
						}
					}
					if scenario.alreadyRead {
						if wrapped.writes != 0 || len(rec.seqs) != 0 {
							t.Errorf("already read must not write or broadcast: writes=%d, broadcasts=%v", wrapped.writes, rec.seqs)
						}
					} else if wrapped.writes != 1 || len(rec.seqs) != 1 {
						t.Errorf("advance must write and broadcast once: writes=%d, broadcasts=%v", wrapped.writes, rec.seqs)
					}
					if _, err := st.GetGroupMember(t.Context(), g.Id, member); !errors.Is(err, store.ErrNotFound) {
						t.Errorf("quit membership: %v", err)
					}
					in.SenderId, in.ClientMsgId = member, "forbidden"
					if _, err := writer.Send(t.Context(), in); !errors.Is(err, errcode.ErrNotGroupMember) {
						t.Errorf("departed member send: %v", err)
					}
					stranger := identity.NativeUserId(uuid.NewV7().String())
					if _, err := reader.MarkRead(t.Context(), stranger, "", before.ConversationId, 100); !errors.Is(err, errcode.ErrNoPermission) {
						t.Errorf("stranger read: %v", err)
					}
				})
			}
		})
	}
}

func TestMarkReadSnapshotStoreError(t *testing.T) {
	cause := errors.New("snapshot unavailable")
	st := &readSnapshotStore{Store: storetest.NewMem(), change: func() error { return cause }}
	_, err := New(st, NoopNotifier{}).MarkRead(t.Context(), "u___1", "", "sg_missing", 1)
	if !errors.Is(err, errcode.ErrStoreFailed) || !errors.Is(err, cause) {
		t.Fatalf("snapshot failure must retain store classification and cause: %v", err)
	}
}
