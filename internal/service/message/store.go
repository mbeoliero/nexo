package message

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
	GetGroup(ctx context.Context, id string) (*store.Group, error)
	GetGroupMember(ctx context.Context, groupId, userId string) (*store.GroupMember, error)
	ListGroupMembers(ctx context.Context, groupId string) ([]store.GroupMember, error)
	LockConversation(ctx context.Context, id string, typ int32, groupId string, now time.Time) (*store.Conversation, error)
	GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*store.UserConversationRow, error)
	VisibleOwners(ctx context.Context, conversationId string, ownerIds []string, seq int64) ([]string, error)
	MutedOwners(ctx context.Context, conversationId string, ownerIds []string) ([]string, error)
	GetMessageByClientId(ctx context.Context, conversationId, senderId, clientMsgId string) (*store.Message, error)
	InsertMessage(ctx context.Context, m *store.Message) (inserted bool, err error)
	SetConversationMaxSeq(ctx context.Context, conversationId string, maxSeq int64, now time.Time) error
	ListMessages(ctx context.Context, conversationId string, beginSeq, endSeq int64, limit int) ([]store.Message, error)
	GetMessages(ctx context.Context, keys []store.MessageKey) ([]store.Message, error)
	TouchUserConversation(ctx context.Context, uc *store.UserConversation, readSeq int64) error
	TouchConversationMembers(ctx context.Context, conversationId string, now time.Time) error
	AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, readSeq int64) error
	ListUserConversations(ctx context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error)
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
