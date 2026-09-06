package group

import (
	"context"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

// Tx is the slice of store.Store this service uses, and the view its transactions run on. It has
// no WithTx, so nesting one transaction inside another is a compile error here rather than the
// runtime store.ErrNestedTx.
type Tx interface {
	GetUser(ctx context.Context, id string) (*store.User, error)
	GetUsers(ctx context.Context, ids []string) ([]store.User, error)
	CreateGroup(ctx context.Context, g *store.Group, members []store.GroupMember) error
	GetGroup(ctx context.Context, id string) (*store.Group, error)
	AddGroupMember(ctx context.Context, m *store.GroupMember) error
	RemoveGroupMember(ctx context.Context, groupId, userId string) (removed bool, err error)
	GetGroupMember(ctx context.Context, groupId, userId string) (*store.GroupMember, error)
	ListGroupMembers(ctx context.Context, groupId string) ([]store.GroupMember, error)
	CountGroupMembers(ctx context.Context, groupId string) (int64, error)
	LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error)
	UpsertUserConversation(ctx context.Context, uc *store.UserConversation) error
	CreateUserConversations(ctx context.Context, ucs []store.UserConversation) error
	SetUserConversationMaxSeq(ctx context.Context, ownerId, conversationId string, maxSeq int64) error
	DeleteUserConversation(ctx context.Context, ownerId, conversationId string) error
}

type Store interface {
	Tx
	WithTx(ctx context.Context, fn func(Tx) error) error
}

// Adapt is the only place a full store.Store enters this package. The fn(tx) below converts
// store.Store to Tx, so the compiler — not a runtime assertion — proves the two agree.
func Adapt(st store.Store) Store { return adapted{st} }

type adapted struct{ store.Store }

func (a adapted) WithTx(ctx context.Context, fn func(Tx) error) error {
	return a.Store.WithTx(ctx, func(tx store.Store) error { return fn(tx) })
}
