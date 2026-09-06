package gateway

import (
	"errors"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"
)

func TestWebSocketTextFramesOnly(t *testing.T) {
	for _, tc := range []struct {
		name string
		typ  int
	}{
		{name: "text", typ: gws.TextMessage},
		{name: "binary", typ: gws.BinaryMessage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateway(t, testConfig())
			conn, _, err := dialWs(startServer(t, g) + "?platform_id=1&token=" + token(1))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			frame := []byte(`{"req_id":1003,"data":{"client_msg_id":"frame-type","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)
			if err := conn.WriteMessage(tc.typ, frame); err != nil {
				t.Fatal(err)
			}
			if tc.typ == gws.BinaryMessage {
				_, _, err := conn.ReadMessage()
				closed, ok := errors.AsType[*gws.CloseError](err)
				if !ok || closed.Code != gws.CloseUnsupportedData {
					t.Fatalf("binary frame: got %v, want close 1003", err)
				}
				seqs, err := g.deps.Message.MaxSeqs(t.Context(), "u___1", "", 10, 100)
				if err != nil || len(seqs.Items) != 0 {
					t.Fatalf("binary frame reached message service: %+v, %v", seqs, err)
				}
				return
			}
			var ack Response
			if err := conn.ReadJSON(&ack); err != nil || ack.ReqId != ReqSendMsg || ack.Code != 0 {
				t.Fatalf("text ACK: %+v, %v", ack, err)
			}
			pong := false
			conn.SetPongHandler(func(payload string) error { pong = payload == "probe"; return nil })
			if err := conn.WriteControl(gws.PingMessage, []byte("probe"), time.Now().Add(time.Second)); err != nil {
				t.Fatal(err)
			}
			if err := conn.WriteMessage(gws.TextMessage, []byte(`{"req_id":1001,"data":{}}`)); err != nil {
				t.Fatal(err)
			}
			var seqs Response
			if err := conn.ReadJSON(&seqs); err != nil || seqs.ReqId != ReqGetMaxSeqs || seqs.Code != 0 || !pong {
				t.Fatalf("text and control frames after send: %+v, pong=%v, %v", seqs, pong, err)
			}
		})
	}
}
