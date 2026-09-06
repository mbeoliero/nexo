package conv

import (
	"encoding/base64"
	"strconv"
	"strings"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

// Cursor wire format: base64("<updated_at_ms>:<conversation_id>"); empty = first page.
func EncodeCursor(c store.ListCursor) string {
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.FormatInt(c.UpdatedAt.UnixMilli(), 10) + ":" + c.ConversationId))
}

func DecodeCursor(s string) (store.ListCursor, bool) {
	if s == "" {
		return store.FirstPage(), true
	}
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return store.ListCursor{}, false
	}
	ms, id, ok := strings.Cut(string(raw), ":")
	if !ok || id == "" {
		return store.ListCursor{}, false
	}
	n, err := strconv.ParseInt(ms, 10, 64)
	if err != nil {
		return store.ListCursor{}, false
	}
	return store.ListCursor{UpdatedAt: time.UnixMilli(n).UTC(), ConversationId: id}, true
}

// VisibleRange applies §5.3: [max(begin, min_seq), min(end, conv_max, uc.max_seq if set)].
func VisibleRange(uc store.UserConversation, convMax, begin, end int64) (int64, int64) {
	begin = max(begin, uc.MinSeq)
	end = min(end, VisibleMax(uc, convMax))
	return begin, end
}

func VisibleMax(uc store.UserConversation, convMax int64) int64 {
	if uc.MaxSeq > 0 {
		return min(uc.MaxSeq, convMax)
	}
	return convMax
}
