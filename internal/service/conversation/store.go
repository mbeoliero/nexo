package conversation

import (
	"context"

	"github.com/mbeoliero/nexo/internal/store"
)

// Store is the slice of store.Store this service uses. Nothing here opens a transaction: every
// call is a single statement, so a fake needs five methods rather than store.Store's forty-two.
type Store interface {
	GetUserConversationRow(ctx context.Context, ownerId, conversationId string) (*store.UserConversationRow, error)
	AdvanceReadSeq(ctx context.Context, ownerId, conversationId string, readSeq int64) error
	SetUserConversationOpt(ctx context.Context, ownerId, conversationId string, recvMsgOpt *int32, isPinned *bool) error
	GetMessages(ctx context.Context, keys []store.MessageKey) ([]store.Message, error)
	ListUserConversations(ctx context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error)
}
