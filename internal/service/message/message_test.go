package message

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
	"github.com/mbeoliero/nexo/msgbody"
)

type recorder struct {
	mu     sync.Mutex
	events []PushEvent
}

func (r *recorder) Publish(_ context.Context, ev PushEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, ev)
}

func setup(t *testing.T) (*Service, *storetest.Mem, *recorder) {
	t.Helper()
	m := storetest.NewMem()
	now := store.NowMs()
	for i, id := range []string{"u___1", "u___2", "u___3"} {
		_ = m.UpsertUser(t.Context(), &store.User{Id: id, Nickname: []string{"one", "two", "three"}[i], UpdatedAt: now})
	}
	_ = m.CreateGroup(t.Context(), &store.Group{Id: "g1", OwnerId: "u___1", CreatedAt: now, UpdatedAt: now}, []store.GroupMember{
		{GroupId: "g1", UserId: "u___1", Role: store.RoleOwner, JoinedAt: now},
		{GroupId: "g1", UserId: "u___2", Role: store.RoleMember, JoinedAt: now},
	})
	for _, id := range []string{"u___1", "u___2"} {
		_ = m.UpsertUserConversation(t.Context(), &store.UserConversation{OwnerId: id, ConversationId: conv.Group("g1"), Type: store.ConversationGroup, GroupId: "g1", MinSeq: 1, UpdatedAt: now})
	}
	r := &recorder{}
	return New(Adapt(m), r, 64), m, r
}

func single(client, content string) SendInput {
	return SendInput{SenderId: "u___1", ClientMsgId: client, SessionType: store.ConversationSingle, RecvId: "u___2", ContentType: msgbody.Text, Content: content, SenderRead: true}
}

func TestSendSingle(t *testing.T) {
	ctx := t.Context()
	s, m, r := setup(t)

	ack1, err := s.Send(ctx, single("c1", `{"text":"hi"}`))
	if err != nil || ack1.Seq != 1 || ack1.ConversationId != "si_u___1:u___2" || ack1.ServerMsgId == "" {
		t.Fatalf("send: %+v %v", ack1, err)
	}
	again, err := s.Send(ctx, single("c1", `{"text":"changed"}`))
	if err != nil || again != ack1 {
		t.Fatalf("idempotent resend must return the same ack: %+v vs %+v (%v)", again, ack1, err)
	}
	if len(r.events) != 1 || r.events[0].Message.Seq != 1 || r.events[0].RecvId != "u___2" {
		t.Fatalf("publish once: %+v", r.events)
	}

	su, _ := m.GetUserConversation(ctx, "u___1", ack1.ConversationId)
	ru, _ := m.GetUserConversation(ctx, "u___2", ack1.ConversationId)
	if su.ReadSeq != 1 || ru.ReadSeq != 0 || ru.PeerUserId != "u___1" {
		t.Fatalf("sender_read=true: sender=%+v recv=%+v", su, ru)
	}

	in := single("c2", `{"text":"platform"}`)
	in.SenderRead = false
	if _, err := s.Send(ctx, in); err != nil {
		t.Fatal(err)
	}
	su, _ = m.GetUserConversation(ctx, "u___1", ack1.ConversationId)
	if su.ReadSeq != 1 {
		t.Fatalf("sender_read=false must leave sender read_seq: %+v", su)
	}

	// 20 concurrent distinct sends: gapless, all published
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Go(func() {
			if _, err := s.Send(ctx, single("p"+string(rune('a'+i)), `{}`)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	msgs, _ := m.ListMessages(ctx, ack1.ConversationId, 1, 1<<62, 100)
	if len(msgs) != 22 {
		t.Fatalf("messages: %d", len(msgs))
	}
	for i, msg := range msgs {
		if msg.Seq != int64(i+1) {
			t.Fatalf("gap at %d", i)
		}
	}
}

func TestSendValidation(t *testing.T) {
	ctx := t.Context()
	s, _, _ := setup(t)
	cases := map[string]struct {
		in   SendInput
		want error
	}{
		"no client id": {SendInput{SenderId: "u___1", SessionType: 1, RecvId: "u___2", ContentType: 1, Content: "{}"}, errcode.ErrInvalidParam},
		"self":         {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 1, RecvId: "u___1", ContentType: 1, Content: "{}"}, errcode.ErrInvalidParam},
		"bad type":     {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 1, RecvId: "u___2", ContentType: 7, Content: "{}"}, errcode.ErrInvalidParam},
		"not json":     {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 1, RecvId: "u___2", ContentType: 1, Content: "{oops"}, errcode.ErrInvalidParam},
		"too long":     {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 1, RecvId: "u___2", ContentType: 1, Content: `"` + strings.Repeat("a", 70) + `"`}, errcode.ErrMessageContentTooLong},
		"unknown recv": {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 1, RecvId: "u___404", ContentType: 1, Content: "{}"}, errcode.ErrUserNotFound},
		"no group":     {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 2, GroupId: "nope", ContentType: 1, Content: "{}"}, errcode.ErrGroupNotFound},
		"session 3":    {SendInput{SenderId: "u___1", ClientMsgId: "x", SessionType: 3, ContentType: 1, Content: "{}"}, errcode.ErrInvalidParam},
	}
	for name, tc := range cases {
		if _, err := s.Send(ctx, tc.in); !errors.Is(err, tc.want) {
			t.Errorf("%s: got %v, want %v", name, err, tc.want)
		}
	}
}

func TestSendRejectsTrailingWhitespace(t *testing.T) {
	for name, in := range map[string]SendInput{
		"single": single("client-id", `{"text":"original"}`),
		"group": {
			SenderId: "u___1", GroupId: "g1", SessionType: store.ConversationGroup,
			ContentType: msgbody.Text, Content: `{"text":"original"}`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			s, _, _ := setup(t)
			testSendClientMsgIdWhitespace(t, s, in)
		})
	}
}

func testSendClientMsgIdWhitespace(t *testing.T, s *Service, in SendInput) {
	t.Helper()
	ctx := t.Context()
	in.ClientMsgId = "client-id"
	first, err := s.Send(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, suffix string }{
		{name: "space", suffix: " "},
		{name: "tab", suffix: "\t"},
		{name: "line break", suffix: "\r\n"},
		{name: "non-breaking space", suffix: "\u00a0"},
		{name: "ideographic space", suffix: "\u3000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bad := in
			bad.ClientMsgId += tc.suffix
			bad.Content = `{"text":"rejected"}`
			if got, err := s.Send(t.Context(), bad); !errors.Is(err, errcode.ErrInvalidParam) || got != (Ack{}) {
				t.Errorf("client_msg_id=%q: ack=%+v err=%v, want no ACK and invalid parameter", bad.ClientMsgId, got, err)
			}
		})
	}
	if again, err := s.Send(ctx, in); err != nil || again != first {
		t.Fatalf("valid retry: %+v %v, want %+v", again, err, first)
	}
	ids := []string{"client-id", " client-id", "client id"}
	for i, id := range ids[1:] {
		valid := in
		valid.ClientMsgId = id
		got, err := s.Send(ctx, valid)
		if err != nil || got.Seq != first.Seq+int64(i)+1 {
			t.Fatalf("client_msg_id=%q: %+v %v; rejected IDs must not consume seq", id, got, err)
		}
	}
	res, err := s.Pull(ctx, PullInput{
		UserId: in.SenderId, ConversationId: first.ConversationId,
		BeginSeq: first.Seq, EndSeq: first.Seq + int64(len(ids)),
	}, 100)
	if err != nil || len(res.Messages) != len(ids) || res.HasMore {
		t.Fatalf("stored messages: %+v %v", res, err)
	}
	for i, msg := range res.Messages {
		if msg.ClientMsgId != ids[i] || msg.Content != in.Content {
			t.Errorf("message %d: %+v; ID and content must remain unchanged", i, msg)
		}
	}
}

func TestSendGroup(t *testing.T) {
	ctx := t.Context()
	s, m, r := setup(t)
	in := SendInput{SenderId: "u___1", ClientMsgId: "g1", SessionType: store.ConversationGroup, GroupId: "g1", ContentType: msgbody.Custom, Content: `{"k":1}`, SenderRead: true}
	ack, err := s.Send(ctx, in)
	if err != nil || ack.Seq != 1 || ack.ConversationId != "sg_g1" {
		t.Fatalf("group send: %+v %v", ack, err)
	}
	if r.events[0].GroupId != "g1" || r.events[0].RecvId != "" {
		t.Fatalf("group event: %+v", r.events[0])
	}
	su, _ := m.GetUserConversation(ctx, "u___1", "sg_g1")
	mu, _ := m.GetUserConversation(ctx, "u___2", "sg_g1")
	if su.ReadSeq != 1 || mu.ReadSeq != 0 || !mu.UpdatedAt.After(time.Now().Add(-time.Minute)) {
		t.Fatalf("group touch: sender=%+v member=%+v", su, mu)
	}

	in.SenderId, in.ClientMsgId = "u___3", "g2"
	if _, err := s.Send(ctx, in); !errors.Is(err, errcode.ErrNotGroupMember) {
		t.Fatalf("non-member: %v", err)
	}
	g, _ := m.GetGroup(ctx, "g1")
	g.Status = store.GroupStatusDismissed
	m.SetGroup(*g)
	in.SenderId, in.ClientMsgId = "u___1", "g3"
	if _, err := s.Send(ctx, in); !errors.Is(err, errcode.ErrGroupDismissed) {
		t.Fatalf("dismissed: %v", err)
	}
	if msgs, _ := m.ListMessages(ctx, "sg_g1", 1, 100, 100); len(msgs) != 1 {
		t.Fatalf("rejected sends must not consume seq: %d", len(msgs))
	}
}

func TestPullAndMaxSeqs(t *testing.T) {
	ctx := t.Context()
	s, m, _ := setup(t)
	for i := range 12 {
		if _, err := s.Send(ctx, SendInput{SenderId: "u___1", ClientMsgId: "m" + string(rune('a'+i)), SessionType: store.ConversationGroup, GroupId: "g1", ContentType: msgbody.Text, Content: `{}`, SenderRead: true}); err != nil {
			t.Fatal(err)
		}
	}
	conv := "sg_g1"
	// u___3 joins late (min_seq 9), u___2 left at 5
	_ = m.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: "u___3", ConversationId: conv, Type: 2, GroupId: "g1", MinSeq: 9, ReadSeq: 8, UpdatedAt: time.Now()})
	_ = m.SetUserConversationMaxSeq(ctx, "u___2", conv, 5)

	if _, err := s.Pull(ctx, PullInput{UserId: "u___9", ConversationId: conv, BeginSeq: 1, EndSeq: 100}, 100); !errors.Is(err, errcode.ErrNoPermission) {
		t.Fatalf("stranger pull: %v", err)
	}
	if _, err := s.Pull(ctx, PullInput{UserId: "u___1", ConversationId: conv, BeginSeq: 5, EndSeq: 4}, 100); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("bad range: %v", err)
	}
	full, err := s.Pull(ctx, PullInput{UserId: "u___1", ConversationId: conv, BeginSeq: 1, EndSeq: 100, Limit: 5}, 100)
	if err != nil || len(full.Messages) != 5 || !full.HasMore || full.Messages[0].Seq != 1 {
		t.Fatalf("owner page: %+v %v", full, err)
	}
	late, _ := s.Pull(ctx, PullInput{UserId: "u___3", ConversationId: conv, BeginSeq: 1, EndSeq: 100}, 100)
	if len(late.Messages) != 4 || late.Messages[0].Seq != 9 || late.HasMore {
		t.Fatalf("late joiner sees 9..12: %+v", late)
	}
	left, _ := s.Pull(ctx, PullInput{UserId: "u___2", ConversationId: conv, BeginSeq: 1, EndSeq: 100}, 100)
	if len(left.Messages) != 5 || left.Messages[4].Seq != 5 {
		t.Fatalf("left member sees 1..5: %+v", left)
	}
	if empty, _ := s.Pull(ctx, PullInput{UserId: "u___2", ConversationId: conv, BeginSeq: 6, EndSeq: 100}, 100); len(empty.Messages) != 0 {
		t.Fatalf("beyond bound must be empty: %+v", empty)
	}

	ms, err := s.MaxSeqs(ctx, "u___2", "", 10, 200)
	if err != nil || len(ms.Items) != 1 || ms.Items[0].MaxSeq != 5 || ms.HasMore {
		t.Fatalf("max seqs for left member: %+v %v", ms, err)
	}
	if _, err := s.Send(ctx, single("s1", `{}`)); err != nil {
		t.Fatal(err)
	}
	page1, _ := s.MaxSeqs(ctx, "u___1", "", 1, 200)
	if len(page1.Items) != 1 || !page1.HasMore || page1.NextCursor == "" || page1.Items[0].ConversationId != "si_u___1:u___2" {
		t.Fatalf("page1 newest first: %+v", page1)
	}
	page2, _ := s.MaxSeqs(ctx, "u___1", page1.NextCursor, 10, 200)
	if len(page2.Items) != 1 || page2.Items[0].ConversationId != conv || page2.Items[0].MaxSeq != 12 || page2.HasMore {
		t.Fatalf("page2: %+v", page2)
	}
	if _, err := s.MaxSeqs(ctx, "u___1", "!!", 10, 200); !errors.Is(err, errcode.ErrInvalidParam) {
		t.Fatalf("bad cursor: %v", err)
	}
}

// cancelOnCommit stands in for the request context dying the instant the write lands — a client
// that disconnects, or a handler that returns, right after the message is committed.
type cancelOnCommit struct {
	store.Store
	cancel context.CancelFunc
}

func (c cancelOnCommit) WithTx(ctx context.Context, fn func(store.Store) error) error {
	err := c.Store.WithTx(ctx, fn)
	c.cancel()
	return err
}

type ctxRecorder struct {
	err      error
	deadline bool
}

func (r *ctxRecorder) Publish(ctx context.Context, _ PushEvent) {
	r.err = ctx.Err()
	_, r.deadline = ctx.Deadline()
}

// The message is durable once the tx commits, so the push must not be cancelled along with the
// request: every other node would silently never see it, and only a client-side pull would recover.
func TestPublishSurvivesRequestCancellation(t *testing.T) {
	_, m, _ := setup(t)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	rec := &ctxRecorder{}
	s := New(Adapt(cancelOnCommit{Store: m, cancel: cancel}), rec, 64)

	if _, err := s.Send(ctx, single("c1", `{"text":"hi"}`)); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() == nil {
		t.Fatal("the test store must have cancelled the request context")
	}
	if rec.err != nil {
		t.Fatalf("publish ran on a dead context: %v", rec.err)
	}
	// ...and it is still bounded, so a wedged bus cannot hold the handler open forever.
	if !rec.deadline {
		t.Fatal("publish context has no deadline")
	}
}

// recv_id and group_id are both client-supplied, and validate() only checks the one that belongs
// to the session type. The other must never reach the conversation row, the message row or the
// push event, or a sender could stamp a group id of their choosing onto a private chat.
func TestSendDropsForeignRoutingField(t *testing.T) {
	ctx := t.Context()
	s, m, r := setup(t)

	in := single("c1", `{"text":"hi"}`)
	in.GroupId = "g1"
	ack, err := s.Send(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	msg, err := m.GetMessageByClientId(ctx, ack.ConversationId, "u___1", "c1")
	if err != nil {
		t.Fatal(err)
	}
	if msg.GroupId != "" {
		t.Fatalf("single chat message kept a client-supplied group_id: %q", msg.GroupId)
	}
	c, err := m.GetConversation(ctx, ack.ConversationId)
	if err != nil {
		t.Fatal(err)
	}
	if c.GroupId != "" {
		t.Fatalf("single chat conversation kept a client-supplied group_id: %q", c.GroupId)
	}
	if r.events[0].GroupId != "" {
		t.Fatalf("push event kept a client-supplied group_id: %q", r.events[0].GroupId)
	}

	gin := SendInput{SenderId: "u___1", ClientMsgId: "c2", SessionType: store.ConversationGroup,
		GroupId: "g1", RecvId: "u___3", ContentType: msgbody.Text, Content: `{"text":"hi"}`, SenderRead: true}
	gack, err := s.Send(ctx, gin)
	if err != nil {
		t.Fatal(err)
	}
	gmsg, err := m.GetMessageByClientId(ctx, gack.ConversationId, "u___1", "c2")
	if err != nil {
		t.Fatal(err)
	}
	if gmsg.RecvId != "" {
		t.Fatalf("group message kept a client-supplied recv_id: %q", gmsg.RecvId)
	}
	if r.events[1].RecvId != "" {
		t.Fatalf("push event kept a client-supplied recv_id: %q", r.events[1].RecvId)
	}
}

func TestSendSeqCollisionIsNotIdempotent(t *testing.T) {
	s, m, _ := setup(t)
	now := store.NowMs()
	// A row past the conversation's max_seq: the next allocation collides on (conversation_id, seq).
	// InsertMessage reports that exactly like a client_msg_id duplicate, but the client never sent
	// this message, so answering with an ACK would hand it someone else's seq.
	if _, err := m.InsertMessage(t.Context(), &store.Message{
		ConversationId: "si_u___1:u___2", Seq: 1, ServerMsgId: "orphan", ClientMsgId: "someone-else",
		SenderId: "u___1", RecvId: "u___2", SessionType: store.ConversationSingle,
		ContentType: msgbody.Text, Content: `{"text":"orphan"}`, SendTime: now, CreatedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	ack, err := s.Send(t.Context(), single("c1", `{"text":"hi"}`))
	if code := errcode.From(err); code == nil || code.Code != errcode.ErrSeqAllocFailed.Code {
		t.Fatalf("send = %+v, %v; want %d", ack, err, errcode.ErrSeqAllocFailed.Code)
	}
}

// The Set* methods are exported for an embedding host, which may call them while Send is running.
func TestSettersDoNotRaceSend(t *testing.T) {
	s, _, _ := setup(t)
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 50 {
			s.SetSendRateLimit(6000)
			s.SetMemberCacheTtl(time.Duration(i) * time.Millisecond)
			s.SetOfflinePush(nil, nil)
			s.InvalidateGroup("g1")
		}
	})
	for i := range 4 {
		wg.Go(func() {
			for j := range 20 {
				in := single(fmt.Sprintf("race-%d-%d", i, j), `{"text":"hi"}`)
				in.SessionType, in.RecvId, in.GroupId = store.ConversationGroup, "", "g1"
				_, _ = s.Send(t.Context(), in)
				// The roster cache is only read on the push path, which Send does not take here.
				if ids, err := s.Recipients(t.Context(), PushEvent{SessionType: store.ConversationGroup, GroupId: "g1"}); err != nil || len(ids) != 2 {
					t.Errorf("recipients = %v, %v", ids, err)
				}
			}
		})
	}
	wg.Wait()
}
