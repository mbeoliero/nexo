package conv

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/store"
)

type pageStore struct {
	store.Store
	list func(context.Context, string, store.ListCursor, int) ([]store.UserConversationRow, error)
}

func (s pageStore) ListUserConversations(ctx context.Context, owner string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error) {
	return s.list(ctx, owner, cursor, limit)
}

func TestListPageEdges(t *testing.T) {
	t.Parallel()
	rows := []store.UserConversationRow{
		{ConversationId: "sg_b", UpdatedAt: time.UnixMilli(1_700_000_000_123)},
		{ConversationId: "sg_a", UpdatedAt: time.UnixMilli(1_700_000_000_123)},
	}
	for _, tc := range []struct {
		name                             string
		count, limit, pageMax, wantLimit int
		wantCount                        int
		hasMore                          bool
	}{
		{name: "empty", limit: 2, pageMax: 10, wantLimit: 3},
		{name: "exact page", count: 2, limit: 2, pageMax: 10, wantLimit: 3, wantCount: 2},
		{name: "lookahead", count: 2, limit: 1, pageMax: 10, wantLimit: 2, wantCount: 1, hasMore: true},
		{name: "default limit", count: 2, pageMax: 2, wantLimit: 3, wantCount: 2},
		{name: "negative limit", count: 2, limit: -1, pageMax: 2, wantLimit: 3, wantCount: 2},
		{name: "clamped limit", count: 2, limit: 10, pageMax: 1, wantLimit: 2, wantCount: 1, hasMore: true},
		{name: "zero maximum", count: 2, pageMax: 0, wantLimit: 2, wantCount: 1, hasMore: true},
		{name: "negative maximum", count: 2, pageMax: -1, wantLimit: 2, wantCount: 1, hasMore: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			st := pageStore{list: func(gotCtx context.Context, owner string, cursor store.ListCursor, limit int) ([]store.UserConversationRow, error) {
				if gotCtx != ctx || owner != "u___1" || cursor != store.FirstPage() || limit != tc.wantLimit {
					t.Fatalf("query: owner=%q cursor=%+v limit=%d", owner, cursor, limit)
				}
				return rows[:tc.count], nil
			}}
			got, next, hasMore, err := ListPage(ctx, st, "u___1", "", tc.limit, tc.pageMax)
			if err != nil || len(got) != tc.wantCount || hasMore != tc.hasMore {
				t.Fatalf("page: rows=%+v hasMore=%v err=%v", got, hasMore, err)
			}
			var wantNext string
			if tc.hasMore {
				last := rows[tc.wantCount-1]
				wantNext = EncodeCursor(store.ListCursor{UpdatedAt: last.UpdatedAt, ConversationId: last.ConversationId})
			}
			if next != wantNext {
				t.Fatalf("next cursor = %q, want %q", next, wantNext)
			}
		})
	}
	t.Run("malformed cursor before store access", func(t *testing.T) {
		_, _, _, err := ListPage(t.Context(), nil, "u___1", "!!", 1, 10)
		if !errors.Is(err, errcode.ErrInvalidParam) || errcode.From(err).Message != "bad cursor" {
			t.Fatalf("malformed cursor: %v", err)
		}
	})
	t.Run("store error", func(t *testing.T) {
		cause := errors.New("unavailable")
		st := pageStore{list: func(context.Context, string, store.ListCursor, int) ([]store.UserConversationRow, error) {
			return nil, cause
		}}
		_, _, _, err := ListPage(t.Context(), st, "u___1", "", 1, 10)
		if !errors.Is(err, errcode.ErrStoreFailed) || !errors.Is(err, cause) {
			t.Fatalf("store error: %v", err)
		}
	})
}
