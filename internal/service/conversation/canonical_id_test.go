package conversation

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

// paddedRowStore stands in for a MySQL PAD SPACE collation: "sg_g1 " finds sg_g1's row.
type paddedRowStore struct{ *storetest.Mem }

func (s paddedRowStore) GetUserConversationRow(ctx context.Context, ownerId, id string) (*store.UserConversationRow, error) {
	return s.Mem.GetUserConversationRow(ctx, ownerId, strings.TrimRight(id, " "))
}

type idRecorder struct{ ids []string }

func (r *idRecorder) ConversationRead(_ context.Context, ev ReadEvent) {
	r.ids = append(r.ids, ev.ConversationId)
}

// The other devices match 2003 against the stored conversation id, so the write and the fan-out
// must use the row's spelling, not the request's (design §8.7).
func TestMarkReadUsesStoredConversationId(t *testing.T) {
	ctx := t.Context()
	m := storetest.NewMem()
	r := &idRecorder{}
	s := New(paddedRowStore{Mem: m}, r)
	now := time.Now()
	m.SetConversation(store.Conversation{ConversationId: "sg_g1", Type: 2, GroupId: "g1", MaxSeq: 10, CreatedAt: now, UpdatedAt: now})
	_ = m.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___1", ConversationId: "sg_g1", Type: 2, MinSeq: 1, UpdatedAt: now})

	if got, err := s.MarkRead(ctx, "u___1", "", "sg_g1 ", 5); err != nil || got != 5 {
		t.Fatalf("mark read: %d %v", got, err)
	}
	if len(r.ids) != 1 || r.ids[0] != "sg_g1" {
		t.Fatalf("broadcast ids %q, want [sg_g1]", r.ids)
	}
	row, err := m.GetUserConversationRow(ctx, "u___1", "sg_g1")
	if err != nil || row.ReadSeq != 5 {
		t.Fatalf("stored row: %+v %v", row, err)
	}
}
