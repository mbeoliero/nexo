package storetest

import (
	"errors"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

// send mirrors the service transaction: lock → alloc → insert → bump max_seq → touch rows.
func send(t *testing.T, s store.Store, conv, sender, recv, clientId string, now time.Time) (store.Message, bool) {
	t.Helper()
	var out store.Message
	var inserted bool
	err := s.WithTx(t.Context(), func(tx store.Store) error {
		c, err := tx.LockConversation(t.Context(), conv, store.ConversationSingle, "", now)
		if err != nil {
			return err
		}
		m := store.Message{ConversationId: conv, Seq: c.MaxSeq + 1, ServerMsgId: conv + ":" + strconv.FormatInt(c.MaxSeq+1, 10), ClientMsgId: clientId,
			SenderId: sender, RecvId: recv, SessionType: store.ConversationSingle, ContentType: 1, Content: "{}", SendTime: now, CreatedAt: now}
		inserted, err = tx.InsertMessage(t.Context(), &m)
		if err != nil || !inserted {
			return err
		}
		if err := tx.SetConversationMaxSeq(t.Context(), conv, m.Seq, now); err != nil {
			return err
		}
		if err := tx.TouchUserConversation(t.Context(), &store.UserConversation{OwnerId: sender, ConversationId: conv, Type: store.ConversationSingle, PeerUserId: recv, UpdatedAt: now}, m.Seq); err != nil {
			return err
		}
		out = m
		return tx.TouchUserConversation(t.Context(), &store.UserConversation{OwnerId: recv, ConversationId: conv, Type: store.ConversationSingle, PeerUserId: sender, UpdatedAt: now}, 0)
	})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	return out, inserted
}

func RunMessages(t *testing.T, s store.Store) {
	ctx := t.Context()
	now := baseTime
	conv := "si_u___1:u___2"

	m1, ok := send(t, s, conv, "u___1", "u___2", "c1", now)
	if !ok || m1.Seq != 1 {
		t.Fatalf("first send: %+v %v", m1, ok)
	}
	if _, ok := send(t, s, conv, "u___1", "u___2", "c1", now.Add(1*time.Millisecond)); ok {
		t.Fatal("duplicate client_msg_id must not insert")
	}
	if got, err := s.GetMessageByClientId(ctx, conv, "u___1", "c1"); err != nil || got.Seq != 1 {
		t.Fatalf("by client id: %+v %v", got, err)
	}
	if _, err := s.GetMessageByClientId(ctx, conv, "u___2", "c1"); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("client id is scoped to sender: %v", err)
	}

	su, _ := s.GetUserConversation(ctx, "u___1", conv)
	ru, _ := s.GetUserConversation(ctx, "u___2", conv)
	if su.ReadSeq != 1 || ru.ReadSeq != 0 || su.MinSeq != 1 || ru.PeerUserId != "u___1" || !ru.UpdatedAt.Equal(now) {
		t.Fatalf("touch: sender=%+v recv=%+v", su, ru)
	}

	// 50 concurrent sends from u___2: seq must be gapless 2..51
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() { send(t, s, conv, "u___2", "u___1", "p"+strconv.Itoa(i), now.Add(2*time.Millisecond)) })
	}
	wg.Wait()
	msgs, err := s.ListMessages(ctx, conv, 1, math.MaxInt64, 100)
	if err != nil || len(msgs) != 51 {
		t.Fatalf("list: %d %v", len(msgs), err)
	}
	for i, m := range msgs {
		if m.Seq != int64(i+1) {
			t.Fatalf("seq gap at %d: %+v", i, m)
		}
	}
	if page, _ := s.ListMessages(ctx, conv, 10, 20, 5); len(page) != 5 || page[0].Seq != 10 || page[4].Seq != 14 {
		t.Fatalf("bounded page: %+v", page)
	}
	keys := []store.MessageKey{{ConversationId: conv, Seq: 3}, {ConversationId: conv, Seq: 51}, {ConversationId: conv, Seq: 999}, {ConversationId: "sg_none", Seq: 1}}
	if got, _ := s.GetMessages(ctx, keys); len(got) != 2 {
		t.Fatalf("GetMessages: %+v", got)
	}

	row, err := s.GetUserConversationRow(ctx, "u___1", conv)
	if err != nil || row.ConvMaxSeq != 51 || row.MaxSeq != 0 || row.ReadSeq != 1 {
		t.Fatalf("joined active range after sends: %+v %v", row, err)
	}

	// u___1 received the burst: read_seq still 1 (its own first send), then advances monotonically.
	if su, _ = s.GetUserConversation(ctx, "u___1", conv); su.ReadSeq != 1 || !su.UpdatedAt.Equal(now.Add(2*time.Millisecond)) {
		t.Fatalf("receiver after burst: %+v", su)
	}
	if err := s.AdvanceReadSeq(ctx, "u___1", conv, 30); err != nil {
		t.Fatal(err)
	}
	if err := s.AdvanceReadSeq(ctx, "u___1", conv, 10); err != nil {
		t.Fatal(err)
	}
	if ru, _ = s.GetUserConversation(ctx, "u___1", conv); ru.ReadSeq != 30 {
		t.Fatalf("read_seq must be monotonic: %+v", ru)
	}

	// group touch only hits active rows (max_seq = 0)
	g := "sg_g9"
	_ = s.WithTx(ctx, func(tx store.Store) error {
		_, err := tx.LockConversation(ctx, g, store.ConversationGroup, "g9", now)
		return err
	})
	for _, u := range []string{"u___1", "u___3"} {
		_ = s.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: u, ConversationId: g, Type: store.ConversationGroup, GroupId: "g9", MinSeq: 1, UpdatedAt: now})
	}
	_ = s.SetUserConversationMaxSeq(ctx, "u___3", g, 5)
	if err := s.TouchConversationMembers(ctx, g, now.Add(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	a, _ := s.GetUserConversation(ctx, "u___1", g)
	b, _ := s.GetUserConversation(ctx, "u___3", g)
	if !a.UpdatedAt.Equal(now.Add(100*time.Millisecond)) || !b.UpdatedAt.Equal(now) {
		t.Fatalf("touch members: active=%+v left=%+v", a, b)
	}

	// listing for u___1: sg_g9 (now.Add(100*time.Millisecond)), sg_g1 from RunConversations (now.Add(5*time.Millisecond)), si (now.Add(2*time.Millisecond)); cursor pages
	first, err := s.ListUserConversations(ctx, "u___1", store.FirstPage(), 1)
	if err != nil || len(first) != 1 || first[0].ConversationId != g || first[0].ConvMaxSeq != 0 {
		t.Fatalf("page1: %+v %v", first, err)
	}
	second, _ := s.ListUserConversations(ctx, "u___1", store.ListCursor{UpdatedAt: first[0].UpdatedAt, ConversationId: first[0].ConversationId}, 10)
	if len(second) != 2 || second[0].ConversationId != "sg_g1" || second[1].ConversationId != conv || second[1].ConvMaxSeq != 51 {
		t.Fatalf("page2: %+v", second)
	}
	last := second[len(second)-1]
	if third, _ := s.ListUserConversations(ctx, "u___1", store.ListCursor{UpdatedAt: last.UpdatedAt, ConversationId: last.ConversationId}, 10); len(third) != 0 {
		t.Fatalf("page3 should be empty: %+v", third)
	}

	// Keys spanning conversations must match as pairs. A backend that degrades the row-constructor
	// into "conversation_id IN (...) AND seq IN (...)" returns the cross product instead, which is
	// one conversation's message handed to another conversation's reader.
	other := "si_u___1:u___4"
	for i := range 3 {
		send(t, s, other, "u___1", "u___4", "o"+strconv.Itoa(i), now.Add(3*time.Millisecond))
	}
	pairs, err := s.GetMessages(ctx, []store.MessageKey{{ConversationId: conv, Seq: 3}, {ConversationId: other, Seq: 1}})
	if err != nil || len(pairs) != 2 {
		t.Fatalf("cross-conversation keys: %+v %v", pairs, err)
	}
	for _, m := range pairs {
		if (m.ConversationId != conv || m.Seq != 3) && (m.ConversationId != other || m.Seq != 1) {
			t.Fatalf("cross product: %s/%d not requested", m.ConversationId, m.Seq)
		}
	}
}
