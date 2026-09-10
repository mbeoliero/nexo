package group_test

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type viewFaultStore struct {
	group.Store
	fail func(context.Context, group.Tx, string) error
}

func (s viewFaultStore) WithTx(ctx context.Context, fn func(group.Tx) error) error {
	return s.Store.WithTx(ctx, func(tx group.Tx) error {
		return fn(viewFaultTx{Tx: tx, fail: s.fail})
	})
}

// Every intercepted write follows the membership change in the same real transaction.
type viewFaultTx struct {
	group.Tx
	fail func(context.Context, group.Tx, string) error
}

func (tx viewFaultTx) CreateUserConversations(ctx context.Context, rows []store.UserConversation) error {
	return tx.fail(ctx, tx.Tx, rows[0].ConversationId)
}

func (tx viewFaultTx) UpsertUserConversation(ctx context.Context, row *store.UserConversation) error {
	return tx.fail(ctx, tx.Tx, row.ConversationId)
}

func (tx viewFaultTx) SetUserConversationMaxSeq(ctx context.Context, _, conversationId string, _ int64) error {
	return tx.fail(ctx, tx.Tx, conversationId)
}

func (tx viewFaultTx) DeleteUserConversation(ctx context.Context, _, conversationId string) error {
	return tx.fail(ctx, tx.Tx, conversationId)
}

type changedGroups struct{ ids []string }

func (n *changedGroups) GroupChanged(_ context.Context, id string) { n.ids = append(n.ids, id) }

func openGroupStore(t *testing.T, backend storetest.Backend) store.Store {
	t.Helper()
	return storetest.Open(t, backend, func(driver, dsn string) (store.Store, error) {
		if driver == "" {
			return pgstore.New(t.Context(), dsn, 4)
		}
		return gormstore.New(driver, dsn, 4)
	})
}

func groupUsers(t *testing.T, ctx context.Context, st store.Store) (string, string) {
	t.Helper()
	owner, member := identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())
	now := store.NowMs()
	for _, id := range []string{owner, member} {
		if err := st.CreateUser(ctx, &store.User{Id: id, CreatedAt: now, UpdatedAt: now}); err != nil {
			t.Fatal(err)
		}
	}
	return owner, member
}

type membershipSnapshot struct {
	group        *store.Group
	members      []store.GroupMember
	conversation *store.Conversation
	views        []*store.UserConversation
}

func snapshotMembership(t *testing.T, ctx context.Context, st store.Store, groupId string, ids []string) membershipSnapshot {
	t.Helper()
	out := membershipSnapshot{}
	var err error
	if out.group, err = st.GetGroup(ctx, groupId); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if out.group != nil {
		out.group.CreatedAt, out.group.UpdatedAt = out.group.CreatedAt.UTC(), out.group.UpdatedAt.UTC()
	}
	if out.members, err = st.ListGroupMembers(ctx, groupId); err != nil {
		t.Fatal(err)
	}
	for i := range out.members {
		out.members[i].JoinedAt = out.members[i].JoinedAt.UTC()
	}
	conversationId := conv.Group(groupId)
	if out.conversation, err = st.GetConversation(ctx, conversationId); err != nil && !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
	if out.conversation != nil {
		out.conversation.CreatedAt, out.conversation.UpdatedAt = out.conversation.CreatedAt.UTC(), out.conversation.UpdatedAt.UTC()
	}
	for _, id := range ids {
		view, err := st.GetUserConversation(ctx, id, conversationId)
		if err != nil && !errors.Is(err, store.ErrNotFound) {
			t.Fatal(err)
		}
		if view != nil {
			view.CreatedAt, view.UpdatedAt = view.CreatedAt.UTC(), view.UpdatedAt.UTC()
		}
		out.views = append(out.views, view)
	}
	return out
}

func TestGroupDatabaseRollback(t *testing.T) {
	for _, backend := range storetest.DbBackends() {
		t.Run(backend.Name, func(t *testing.T) {
			st := openGroupStore(t, backend)
			for _, tc := range []struct {
				name, action string
				withMessage  bool
			}{
				{name: "create", action: "create"},
				{name: "join", action: "join", withMessage: true},
				{name: "quit-empty", action: "quit"},
				{name: "quit-with-message", action: "quit", withMessage: true},
				{name: "kick-empty", action: "kick"},
				{name: "kick-with-message", action: "kick", withMessage: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
					defer cancel()
					owner, member := groupUsers(t, ctx, st)
					ids := []string{owner, member}
					input := group.CreateInput{Name: "rollback", MemberIds: []string{member}}
					groupId := ""
					var before membershipSnapshot
					if tc.action != "create" {
						initial := input
						if tc.action == "join" {
							initial.MemberIds = nil
						}
						g, err := group.New(group.Adapt(st), group.NoopNotifier{}, 10).Create(ctx, owner, initial)
						if err != nil {
							t.Fatal(err)
						}
						groupId = g.Id
						if tc.withMessage {
							_, err := message.New(message.Adapt(st), message.NoopPublisher{}, message.Config{MaxContentBytes: 64}).Send(ctx, message.SendInput{
								SenderId: owner, GroupId: groupId, SessionType: store.ConversationGroup,
								ClientMsgId: "before-failure", ContentType: 1, Content: `{}`,
							})
							if err != nil {
								t.Fatal(err)
							}
						}
						before = snapshotMembership(t, ctx, st, groupId, ids)
					}
					run := func(svc *group.Service) error {
						switch tc.action {
						case "create":
							g, err := svc.Create(ctx, owner, input)
							if err == nil {
								groupId = g.Id
							}
							return err
						case "join":
							return svc.Join(ctx, groupId, member)
						case "quit":
							return svc.Quit(ctx, groupId, member)
						default:
							return svc.Kick(ctx, groupId, owner, member)
						}
					}
					injected := errors.New("injected user conversation write failure")
					hits := 0
					fault := viewFaultStore{Store: group.Adapt(st), fail: func(ctx context.Context, tx group.Tx, conversationId string) error {
						hits++
						groupId = strings.TrimPrefix(conversationId, "sg_")
						if _, err := tx.GetGroup(ctx, groupId); err != nil {
							t.Fatalf("fault must follow group persistence: %v", err)
						}
						_, err := tx.GetGroupMember(ctx, groupId, member)
						removed := tc.action == "quit" || tc.action == "kick"
						if removed && !errors.Is(err, store.ErrNotFound) || !removed && err != nil {
							t.Fatalf("fault must follow membership mutation: removed=%v, err=%v", removed, err)
						}
						return injected
					}}
					notifications := &changedGroups{}
					if err := run(group.New(fault, notifications, 10)); !errors.Is(err, injected) || !errors.Is(err, errcode.ErrStoreFailed) {
						t.Fatalf("injected failure: %v", err)
					}
					if hits != 1 || len(notifications.ids) != 0 {
						t.Fatalf("failure must be reached once and never notify: hits=%d, notifications=%v", hits, notifications.ids)
					}
					after := snapshotMembership(t, ctx, st, groupId, ids)
					if tc.action == "create" {
						if after.group != nil || after.conversation != nil || len(after.members) != 0 || after.views[0] != nil || after.views[1] != nil {
							t.Fatalf("failed create left persisted state: %+v", after)
						}
					} else if !reflect.DeepEqual(after, before) {
						t.Fatalf("rollback changed group, members or visibility: after=%+v before=%+v", after, before)
					}
					if err := run(group.New(group.Adapt(st), notifications, 10)); err != nil {
						t.Fatalf("retry: %v", err)
					}
					if len(notifications.ids) != 1 || notifications.ids[0] != groupId {
						t.Fatalf("retry must notify exactly once: %v", notifications.ids)
					}
					view, err := st.GetUserConversation(ctx, member, conv.Group(groupId))
					if tc.action == "quit" || tc.action == "kick" {
						if _, err := st.GetGroupMember(ctx, groupId, member); !errors.Is(err, store.ErrNotFound) {
							t.Fatalf("retry must remove membership: %v", err)
						}
						if tc.withMessage {
							if err != nil || view.MinSeq != 1 || view.MaxSeq != 1 || view.ReadSeq != 0 {
								t.Fatalf("retry must freeze the original visible range: %+v, %v", view, err)
							}
						} else if !errors.Is(err, store.ErrNotFound) {
							t.Fatalf("retry leaving an empty group must delete the view: %+v, %v", view, err)
						}
						return
					}
					wantMin, wantRead := int64(1), int64(0)
					if tc.action == "join" {
						wantMin, wantRead = 2, 1
					}
					if err != nil || view.MinSeq != wantMin || view.ReadSeq != wantRead || view.MaxSeq != 0 {
						t.Fatalf("retry must create the correct visible range: %+v, %v", view, err)
					}
					if n, err := st.CountGroupMembers(ctx, groupId); err != nil || n != 2 {
						t.Fatalf("retry membership count: %d, %v", n, err)
					}
				})
			}
		})
	}
}
