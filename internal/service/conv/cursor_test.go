package conv

import (
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

func TestCursorRoundTrip(t *testing.T) {
	c := store.ListCursor{UpdatedAt: time.UnixMilli(1_700_000_000_123).UTC(), ConversationId: "si_u___1:u___2"}
	got, ok := DecodeCursor(EncodeCursor(c))
	if !ok || !got.UpdatedAt.Equal(c.UpdatedAt) || got.ConversationId != c.ConversationId {
		t.Fatalf("round trip: %+v %v", got, ok)
	}
	if first, ok := DecodeCursor(""); !ok || first != store.FirstPage() {
		t.Fatal("empty cursor must be first page")
	}
	for _, bad := range []string{"!!", "bm9jb2xvbg", "MTIzOg"} {
		if _, ok := DecodeCursor(bad); ok {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestVisibleRange(t *testing.T) {
	uc := store.UserConversation{MinSeq: 6, MaxSeq: 0}
	if b, e := VisibleRange(uc, 20, 1, 100); b != 6 || e != 20 {
		t.Fatalf("active member: %d..%d", b, e)
	}
	uc.MaxSeq = 9
	if b, e := VisibleRange(uc, 20, 1, 100); b != 6 || e != 9 {
		t.Fatalf("left member: %d..%d", b, e)
	}
	if b, e := VisibleRange(uc, 8, 7, 8); b != 7 || e != 8 {
		t.Fatalf("narrow request: %d..%d", b, e)
	}
}
