package conv

import (
	"context"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/store"
)

// Lister is the one store method paging needs; the conversation and message services both satisfy it.
type Lister interface {
	ListUserConversations(ctx context.Context, ownerId string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error)
}

func ListPage(ctx context.Context, st Lister, userId, cursor string, limit, pageMax int) ([]store.UserConversationRow, string, bool, error) {
	cur, ok := DecodeCursor(cursor)
	if !ok {
		return nil, "", false, errcode.ErrInvalidParam.WithMessage("bad cursor")
	}
	// Embedding hosts can pass zero: keep a row available for the continuation cursor.
	pageMax = max(pageMax, 1)
	if limit <= 0 || limit > pageMax {
		limit = pageMax
	}
	rows, err := st.ListUserConversations(ctx, userId, cur, limit+1)
	if err != nil {
		return nil, "", false, errcode.ErrStoreFailed.Wrap(err)
	}
	if len(rows) <= limit {
		return rows, "", false, nil
	}
	rows = rows[:limit]
	last := rows[len(rows)-1]
	next := EncodeCursor(store.ListCursor{UpdatedAt: last.UpdatedAt, ConversationId: last.ConversationId})
	return rows, next, true, nil
}
