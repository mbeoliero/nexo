package message

import (
	"context"
	"strings"
	"testing"

	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/store"
)

type canonicalGroupLookup struct{ store.Store }

func (s canonicalGroupLookup) GetGroup(ctx context.Context, id string) (*store.Group, error) {
	return s.Store.GetGroup(ctx, strings.TrimRight(id, " "))
}

func TestSendUsesCanonicalGroupId(t *testing.T) {
	_, mem, _ := setup(t)
	testSendCanonicalGroupId(t, canonicalGroupLookup{Store: mem}, SendInput{
		SenderId: "u___1", GroupId: "g1", SessionType: store.ConversationGroup,
		ContentType: 1, Content: `{}`, SenderRead: true,
	})
}

func testSendCanonicalGroupId(t *testing.T, st store.Store, in SendInput) {
	t.Helper()
	ctx := t.Context()
	r := &recorder{}
	s := New(Adapt(st), r, 64)
	in.ClientMsgId = "canonical"
	first, err := s.Send(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	canonicalId := in.GroupId
	in.GroupId += " "
	in.ClientMsgId = "alias"
	second, err := s.Send(ctx, in)
	if err != nil {
		t.Fatalf("send using group alias: %v", err)
	}
	if second.ConversationId != first.ConversationId || second.Seq != first.Seq+1 {
		t.Errorf("alias ACK: %+v, want conversation %q seq %d", second, first.ConversationId, first.Seq+1)
	}
	for _, id := range []string{canonicalId, canonicalId + " "} {
		in.GroupId = id
		if retry, err := s.Send(ctx, in); err != nil || retry != second {
			t.Errorf("retry with group %q: %+v, %v; want %+v", id, retry, err, second)
		}
	}
	if len(r.events) != 2 {
		t.Fatalf("retries must not publish again: %d events", len(r.events))
	}
	ev := r.events[1]
	if ev.ConversationId != first.ConversationId || ev.GroupId != canonicalId ||
		ev.Message.ConversationId != first.ConversationId || ev.Message.GroupId != canonicalId {
		t.Errorf("noncanonical push: %+v", ev)
	}
	msgs, err := st.ListMessages(ctx, first.ConversationId, second.Seq, second.Seq, 1)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("stored message: %+v, %v", msgs, err)
	}
	if msgs[0].ConversationId != first.ConversationId || msgs[0].GroupId != canonicalId {
		t.Errorf("noncanonical stored message: %+v", msgs[0])
	}
	list, err := conversation.New(st, conversation.NoopNotifier{}).List(ctx, in.SenderId, "", 10, 100, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range list.Conversations {
		if item.ConversationId != first.ConversationId {
			continue
		}
		if item.GroupId != canonicalId || item.MaxSeq != second.Seq ||
			item.LastMessage == nil || item.LastMessage.ServerMsgId != second.ServerMsgId {
			t.Errorf("canonical conversation must include latest message: %+v", item)
		}
		return
	}
	t.Fatalf("canonical conversation %q missing from list: %+v", first.ConversationId, list)
}
