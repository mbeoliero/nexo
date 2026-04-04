package cluster

import (
	"encoding/json"
	"testing"
)

func TestPushMessageJSONRoundTrip(t *testing.T) {
	want := &PushMessage{
		ServerMsgId:    1,
		ConversationId: "si_u1_u2",
		Seq:            2,
		ClientMsgId:    "cm1",
		SenderId:       "u1",
		RecvId:         "u2",
		SessionType:    1,
		MsgType:        1,
		SendAt:         123,
	}
	want.Content.Text = "hello"

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal push message: %v", err)
	}

	var got PushMessage
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal push message: %v", err)
	}

	if got.ServerMsgId != want.ServerMsgId ||
		got.ConversationId != want.ConversationId ||
		got.Seq != want.Seq ||
		got.ClientMsgId != want.ClientMsgId ||
		got.SenderId != want.SenderId ||
		got.RecvId != want.RecvId ||
		got.SessionType != want.SessionType ||
		got.MsgType != want.MsgType ||
		got.Content.Text != want.Content.Text ||
		got.SendAt != want.SendAt {
		t.Fatalf("push message mismatch: got %+v want %+v", got, *want)
	}
}

func TestPushEnvelopeJSONRoundTrip(t *testing.T) {
	want := &PushEnvelope{
		Version:        1,
		PushId:         "p1",
		SourceInstance: "i1",
		SentAt:         123,
		TargetConnMap: map[string][]ConnRef{
			"i2": {
				{UserId: "u2", ConnId: "c2", PlatformId: 5},
			},
		},
		Payload: &PushPayload{
			Message: &PushMessage{
				ServerMsgId:    1,
				ConversationId: "si_u1_u2",
				Seq:            2,
				ClientMsgId:    "cm1",
				SenderId:       "u1",
				RecvId:         "u2",
				SessionType:    1,
				MsgType:        1,
				SendAt:         123,
			},
		},
	}
	want.Payload.Message.Content.Text = "hello"

	data, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal push envelope: %v", err)
	}

	var got PushEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal push envelope: %v", err)
	}

	if got.Version != want.Version ||
		got.PushId != want.PushId ||
		got.SourceInstance != want.SourceInstance ||
		got.SentAt != want.SentAt {
		t.Fatalf("push envelope header mismatch: got %+v want %+v", got, *want)
	}

	if len(got.TargetConnMap["i2"]) != 1 {
		t.Fatalf("target conn map mismatch: %+v", got.TargetConnMap)
	}

	if got.Payload == nil || got.Payload.Message == nil || got.Payload.Message.Content.Text != "hello" {
		t.Fatalf("payload mismatch: %+v", got.Payload)
	}
}
