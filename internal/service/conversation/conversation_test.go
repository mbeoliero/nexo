package conversation

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type recorder struct{ seqs []int64 }

func (r *recorder) ConversationRead(_ context.Context, ev ReadEvent) {
	seq := ev.ReadSeq
	r.seqs = append(r.seqs, seq)
}

func TestMarkRead(t *testing.T) {
	ctx := t.Context()
	m := storetest.NewMem()
	r := &recorder{}
	s := New(m, r)
	now := time.Now()
	conv := "sg_g1"
	m.SetConversation(store.Conversation{ConversationId: conv, Type: 2, GroupId: "g1", MaxSeq: 10, CreatedAt: now, UpdatedAt: now})
	_ = m.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___1", ConversationId: conv, Type: 2, MinSeq: 1, ReadSeq: 3, UpdatedAt: now})

	if _, err := s.MarkRead(ctx, "u___9", "", conv, 5); !errors.Is(err, errcode.ErrNoPermission) {
		t.Fatalf("stranger: %v", err)
	}
	if got, err := s.MarkRead(ctx, "u___1", "", conv, 2); err != nil || got != 3 {
		t.Fatalf("backwards must be a no-op: %d %v", got, err)
	}
	if got, err := s.MarkRead(ctx, "u___1", "", conv, 7); err != nil || got != 7 {
		t.Fatalf("forward: %d %v", got, err)
	}
	if got, _ := s.MarkRead(ctx, "u___1", "", conv, 99); got != 10 {
		t.Fatalf("clamped to visible max: %d", got)
	}
	_ = m.SetUserConversationMaxSeq(ctx, "u___1", conv, 8)
	if got, _ := s.MarkRead(ctx, "u___1", "", conv, 99); got != 10 {
		t.Fatalf("already past the frozen bound stays: %d", got)
	}
	if len(r.seqs) != 2 || r.seqs[0] != 7 || r.seqs[1] != 10 {
		t.Fatalf("notify: %v", r.seqs)
	}
}

func TestListAndOpt(t *testing.T) {
	ctx := t.Context()
	m := storetest.NewMem()
	s := New(m, NoopNotifier{})
	base := time.UnixMilli(1_700_000_000_000).UTC()
	put := func(conv string, typ int32, convMax, minSeq, maxSeq, readSeq int64, at time.Time) {
		m.SetConversation(store.Conversation{ConversationId: conv, Type: typ, MaxSeq: convMax, CreatedAt: base, UpdatedAt: at})
		_ = m.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___1", ConversationId: conv, Type: typ, MinSeq: minSeq, MaxSeq: maxSeq, ReadSeq: readSeq, UpdatedAt: at})
		for seq := int64(1); seq <= convMax; seq++ {
			_, _ = m.InsertMessage(ctx, &store.Message{ConversationId: conv, Seq: seq, ServerMsgId: conv + "#" + strconv.FormatInt(seq, 10), ClientMsgId: strconv.FormatInt(seq, 10), SenderId: "u___2", SessionType: typ, ContentType: 1, Content: `{}`, SendTime: base, CreatedAt: base})
		}
	}
	put("sg_active", 2, 10, 1, 0, 4, base.Add(3*time.Millisecond)) // unread 6, last = 10
	put("sg_left", 2, 10, 1, 6, 6, base.Add(2*time.Millisecond))   // frozen at 6, unread 0, last = 6
	put("sg_late", 2, 10, 12, 0, 10, base.Add(1*time.Millisecond)) // joined after everything: no last message
	put("si_a:b", 1, 0, 1, 0, 0, base)                             // empty conversation

	page1, err := s.List(ctx, "u___1", "", 2, 100, true)
	if err != nil || len(page1.Conversations) != 2 || !page1.HasMore {
		t.Fatalf("page1: %+v %v", page1, err)
	}
	a, l := page1.Conversations[0], page1.Conversations[1]
	if a.ConversationId != "sg_active" || a.Unread != 6 || a.MaxSeq != 10 || a.LastMessage == nil || a.LastMessage.Seq != 10 {
		t.Fatalf("active: %+v", a)
	}
	if l.ConversationId != "sg_left" || l.Unread != 0 || l.MaxSeq != 6 || l.LastMessage == nil || l.LastMessage.Seq != 6 {
		t.Fatalf("left: %+v", l)
	}
	page2, _ := s.List(ctx, "u___1", page1.NextCursor, 10, 100, true)
	if len(page2.Conversations) != 2 || page2.HasMore || page2.Conversations[0].LastMessage != nil || page2.Conversations[1].LastMessage != nil {
		t.Fatalf("page2: %+v", page2)
	}
	if page2.Conversations[0].ConversationId != "sg_late" || page2.Conversations[0].Unread != 0 {
		t.Fatalf("late joiner: %+v", page2.Conversations[0])
	}
	noLast, _ := s.List(ctx, "u___1", "", 10, 100, false)
	if noLast.Conversations[0].LastMessage != nil {
		t.Fatal("with_last_message=false must omit last_message")
	}

	one := int32(1)
	if err := s.SetOpt(ctx, "u___1", "sg_active", Opt{RecvMsgOpt: &one}); err != nil {
		t.Fatal(err)
	}
	uc, _ := m.GetUserConversation(ctx, "u___1", "sg_active")
	if uc.RecvMsgOpt != 1 || uc.IsPinned || !uc.UpdatedAt.Equal(base.Add(3*time.Millisecond)) {
		t.Fatalf("opt partial update: %+v", uc)
	}
	bad := int32(5)
	if err := s.SetOpt(ctx, "u___1", "sg_active", Opt{RecvMsgOpt: &bad}); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("bad opt: %v", err)
	}
	if err := s.SetOpt(ctx, "u___9", "sg_active", Opt{}); !errors.Is(err, errcode.ErrNoPermission) {
		t.Fatalf("stranger opt: %v", err)
	}
}

// cancelOnAdvance kills the request context the instant read_seq is committed.
type cancelOnAdvance struct {
	store.Store
	cancel context.CancelFunc
}

func (c cancelOnAdvance) AdvanceReadSeq(ctx context.Context, userId, conversationId string, seq int64) error {
	err := c.Store.AdvanceReadSeq(ctx, userId, conversationId, seq)
	c.cancel()
	return err
}

type ctxRecorder struct {
	err      error
	deadline bool
}

func (r *ctxRecorder) ConversationRead(ctx context.Context, _ ReadEvent) {
	r.err = ctx.Err()
	_, r.deadline = ctx.Deadline()
}

// Same rule as message.Send: read_seq is durable once AdvanceReadSeq returns, so the multi-device
// read sync must not be cancelled with the request that triggered it.
func TestReadFanOutSurvivesRequestCancellation(t *testing.T) {
	m := storetest.NewMem()
	now := time.Now()
	m.SetConversation(store.Conversation{ConversationId: "sg_g1", Type: 2, GroupId: "g1", MaxSeq: 10, CreatedAt: now, UpdatedAt: now})
	_ = m.UpsertUserConversation(t.Context(), &store.UserConversation{OwnerId: "u___1", ConversationId: "sg_g1", Type: 2, MinSeq: 1, ReadSeq: 3, UpdatedAt: now})

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rec := &ctxRecorder{}
	s := New(cancelOnAdvance{Store: m, cancel: cancel}, rec)

	if _, err := s.MarkRead(ctx, "u___1", "c1", "sg_g1", 7); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("the test store must have cancelled the request context")
	}
	if rec.err != nil {
		t.Fatalf("read fan-out ran on a dead context: %v", rec.err)
	}
	if !rec.deadline {
		t.Fatal("fan-out context has no deadline")
	}
}
