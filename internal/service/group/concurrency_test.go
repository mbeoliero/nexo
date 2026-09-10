package group_test

import (
	"context"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type gatedConversationStore struct {
	store.Store
	started chan struct{}
	release <-chan struct{}
}

func (s gatedConversationStore) WithTx(ctx context.Context, fn func(store.Store) error) error {
	return s.Store.WithTx(ctx, func(tx store.Store) error {
		return fn(gatedConversationStore{Store: tx, started: s.started, release: s.release})
	})
}

func (s gatedConversationStore) LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error) {
	if s.release == nil {
		// The second operation has reached the same lock while the first still holds it.
		close(s.started)
		return s.Store.LockConversation(ctx, id, typ, groupId, now)
	}
	c, err := s.Store.LockConversation(ctx, id, typ, groupId, now)
	if err != nil {
		return nil, err
	}
	close(s.started)
	select {
	case <-s.release:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestGroupMembershipAndSendOrdering(t *testing.T) {
	for _, backend := range storetest.DbBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			st := openGroupStore(t, backend)
			for _, action := range []string{"join", "kick"} {
				for _, first := range []string{"membership", "send"} {
					t.Run(action+"/"+first+"-first", func(t *testing.T) {
						ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
						defer cancel()
						owner, member := groupUsers(t, ctx, st)
						input := group.CreateInput{Name: "membership ordering"}
						if action == "kick" {
							input.MemberIds = []string{member}
						}
						g, err := group.New(group.Adapt(st), group.NoopNotifier{}, 10).Create(ctx, owner, input)
						if err != nil {
							t.Fatal(err)
						}
						writer := message.New(message.Adapt(st), message.NoopPublisher{}, message.Config{MaxContentBytes: 64})
						sendInput := message.SendInput{
							SenderId: owner, GroupId: g.Id, SessionType: store.ConversationGroup,
							ClientMsgId: "seed", ContentType: 1, Content: `{}`,
						}
						if ack, err := writer.Send(ctx, sendInput); err != nil || ack.Seq != 1 {
							t.Fatalf("seed: %+v, %v", ack, err)
						}
						sendInput.ClientMsgId = "racing"
						if action == "kick" {
							sendInput.SenderId = member
						}
						release := make(chan struct{})
						resume := sync.OnceFunc(func() { close(release) })
						firstStarted, secondStarted := make(chan struct{}), make(chan struct{})
						firstStore := gatedConversationStore{Store: st, started: firstStarted, release: release}
						secondStore := gatedConversationStore{Store: st, started: secondStarted}
						groupStore, messageStore := firstStore, secondStore
						if first == "send" {
							groupStore, messageStore = secondStore, firstStore
						}
						groups := group.New(group.Adapt(groupStore), group.NoopNotifier{}, 10)
						sender := message.New(message.Adapt(messageStore), message.NoopPublisher{}, message.Config{MaxContentBytes: 64})
						var membershipErr, sendErr error
						var sent message.Ack
						membership := func() {
							if action == "join" {
								membershipErr = groups.Join(ctx, g.Id, member)
							} else {
								membershipErr = groups.Kick(ctx, g.Id, owner, member)
							}
						}
						send := func() { sent, sendErr = sender.Send(ctx, sendInput) }
						firstOp, secondOp := membership, send
						if first == "send" {
							firstOp, secondOp = send, membership
						}
						var wg sync.WaitGroup
						t.Cleanup(func() { resume(); cancel(); wg.Wait() })
						waitStarted := func(started <-chan struct{}) {
							select {
							case <-started:
							case <-ctx.Done():
								t.Fatal("operation did not reach the conversation lock: ", ctx.Err())
							}
						}
						wg.Go(firstOp)
						waitStarted(firstStarted)
						wg.Go(secondOp)
						waitStarted(secondStarted)
						resume()
						wg.Wait()
						if membershipErr != nil {
							t.Fatalf("membership: %v", membershipErr)
						}
						kickedBeforeSend := action == "kick" && first == "membership"
						if kickedBeforeSend {
							if !errors.Is(sendErr, errcode.ErrNotGroupMember) || sent != (message.Ack{}) {
								t.Fatalf("send after kick must fail without an ACK: %+v, %v", sent, sendErr)
							}
						} else if sendErr != nil || sent.Seq != 2 {
							t.Fatalf("ordered send: %+v, %v", sent, sendErr)
						}
						sendInput.SenderId, sendInput.ClientMsgId = owner, "after"
						after, err := writer.Send(ctx, sendInput)
						wantSeq := int64(3)
						if kickedBeforeSend {
							wantSeq = 2
						}
						if err != nil || after.Seq != wantSeq {
							t.Fatalf("next send must not leave a seq gap: %+v, %v", after, err)
						}
						view, err := st.GetUserConversation(ctx, member, conv.Group(g.Id))
						wantMin, wantMax, wantRead := int64(1), int64(0), int64(0)
						wantVisible := []int64{1, 2}
						if action == "join" {
							wantMin, wantRead, wantVisible = 2, 1, []int64{2, 3}
							if first == "send" {
								wantMin, wantRead, wantVisible = 3, 2, []int64{3}
							}
						} else {
							wantMax = 2
							if kickedBeforeSend {
								wantMax, wantVisible = 1, []int64{1}
							}
						}
						if err != nil || view.MinSeq != wantMin || view.MaxSeq != wantMax || view.ReadSeq != wantRead {
							t.Fatalf("visible bounds: %+v, %v; want min=%d max=%d read=%d", view, err, wantMin, wantMax, wantRead)
						}
						pulled, err := writer.Pull(ctx, message.PullInput{
							UserId: member, ConversationId: conv.Group(g.Id), BeginSeq: 1, EndSeq: 10,
						}, 10)
						if err != nil || pulled.HasMore {
							t.Fatalf("pull: %+v, %v", pulled, err)
						}
						seqs := make([]int64, len(pulled.Messages))
						for i, msg := range pulled.Messages {
							seqs[i] = msg.Seq
						}
						if !slices.Equal(seqs, wantVisible) {
							t.Fatalf("pulled seqs=%v, want %v", seqs, wantVisible)
						}
					})
				}
			}
		})
	}
}
