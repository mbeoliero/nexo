package message

import (
	"context"
	"sync"
	"testing"
	"uuid"

	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type pullInterleavingStore struct {
	store.Store
	changeMembership func() error
}

func (s pullInterleavingStore) GetUserConversation(ctx context.Context, ownerId, conversationId string) (*store.UserConversation, error) {
	uc, err := s.Store.GetUserConversation(ctx, ownerId, conversationId)
	if err != nil {
		return nil, err
	}
	if err := s.changeMembership(); err != nil {
		return nil, err
	}
	return uc, nil
}

func (s pullInterleavingStore) ListMessages(ctx context.Context, conversationId string, begin, end int64, limit int) ([]store.Message, error) {
	// A joined permission read has no intermediate seam; change membership before fetching bodies instead.
	if err := s.changeMembership(); err != nil {
		return nil, err
	}
	return s.Store.ListMessages(ctx, conversationId, begin, end, limit)
}

func TestPullDoesNotLeakMessagesAfterQuit(t *testing.T) {
	testPullAfterQuit(t, storetest.NewMem())
}

func testPullAfterQuit(t *testing.T, st store.Store) {
	t.Helper()
	ctx := t.Context()
	owner, member := identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())
	for _, id := range []string{owner, member} {
		if err := st.UpsertUser(ctx, &store.User{Id: id, UpdatedAt: store.NowMs()}); err != nil {
			t.Fatal(err)
		}
	}
	groups := group.New(group.Adapt(st), group.NoopNotifier{}, 10)
	g, err := groups.Create(ctx, owner, group.CreateInput{Name: "pull snapshot", MemberIds: []string{member}})
	if err != nil {
		t.Fatal(err)
	}
	writer := New(Adapt(st), NoopPublisher{}, 64)
	in := SendInput{SenderId: owner, ClientMsgId: "before", SessionType: store.ConversationGroup, GroupId: g.Id, ContentType: 1, Content: `{}`}
	before, err := writer.Send(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	changed := false
	change := sync.OnceValue(func() error {
		if err := groups.Quit(ctx, g.Id, member); err != nil {
			return err
		}
		in.ClientMsgId, in.Content = "after", `{"text":"after quit"}`
		_, err := writer.Send(ctx, in)
		changed = err == nil
		return err
	})
	reader := New(Adapt(pullInterleavingStore{Store: st, changeMembership: change}), NoopPublisher{}, 64)
	res, err := reader.Pull(ctx, PullInput{
		UserId: member, ConversationId: before.ConversationId, BeginSeq: 1, EndSeq: 100,
	}, 100)
	if err != nil || !changed || len(res.Messages) != 1 || res.Messages[0].Seq != 1 || res.HasMore {
		t.Fatalf("member left at seq 1; pull must not contain later bodies: %+v, changed=%v, %v", res, changed, err)
	}
}
