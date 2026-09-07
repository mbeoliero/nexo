package message

import (
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/store"
)

// MySQL PAD SPACE lets "nx__… " find the recipient's row, so a padded native id used to reach
// the conversation id, the message row and the push unchanged and split the single chat
// (design §5.1). identity.Valid now stops it at the door; the stored spelling is used past it.
func TestSendRejectsPaddedRecvId(t *testing.T) {
	s, mem, r := setup(t)
	ctx := t.Context()
	const recv = "nx__0190a6e1-2b3c-7d4e-8f5a-6b7c8d9e0f1a"
	if err := mem.UpsertUser(ctx, &store.User{Id: recv}); err != nil {
		t.Fatal(err)
	}
	in := single("c1", `{}`)
	for _, id := range []string{recv + " ", " " + recv, "u___2 "} {
		in.RecvId = id
		if _, err := s.Send(ctx, in); !errors.Is(err, errcode.ErrInvalidParam) {
			t.Errorf("recv_id %q: %v, want invalid param", id, err)
		}
	}
	in.RecvId = recv
	ack, err := s.Send(ctx, in)
	if err != nil {
		t.Fatal(err)
	}
	want := conv.Single("u___1", recv)
	if ack.ConversationId != want || len(r.events) != 1 || r.events[0].RecvId != recv || r.events[0].Message.RecvId != recv {
		t.Fatalf("ack %+v events %+v, want conversation %q", ack, r.events, want)
	}
}
