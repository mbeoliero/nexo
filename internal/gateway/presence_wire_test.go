package gateway

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"testing"
	"time"

	gws "github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/internal/auth"
	onlinedb "github.com/mbeoliero/nexo/internal/onlinestore/db"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestOnlineSubscriptionsOverWebSocket(t *testing.T) {
	cfg := testConfig()
	cfg.Ws.PongWait = time.Minute
	g := newSubscriptionGateway(t, onlinedb.New(storetest.NewMem(), time.Minute), cfg)
	g.deps.Auth = auth.NewExternal([]string{"ext"}, "user")
	url := startServer(t, g)
	watcher, _, err := dialWs(url + "?platform_id=1&token=" + token(1))
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()
	if err := watcher.WriteMessage(gws.TextMessage, []byte(`{"req_id":1006,"op_id":"watch","msg_incr":"1","data":{"revision":1,"user_ids":["u___2"]}}`)); err != nil {
		t.Fatal(err)
	}
	// A full snapshot can precede its acceptance response, so consume both in either order.
	var ackSeen, snapshotSeen bool
	for range 2 {
		if err := watcher.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, raw, err := watcher.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		var frame struct {
			ReqId   int            `json:"req_id"`
			OpId    string         `json:"op_id"`
			MsgIncr string         `json:"msg_incr"`
			Code    int            `json:"code"`
			Data    jsontext.Value `json:"data"`
		}
		if err := json.Unmarshal(raw, &frame); err != nil {
			t.Fatal(err)
		}
		switch frame.ReqId {
		case ReqSetOnlineSubscriptions:
			var ack onlineSubscriptionAck
			if err := json.Unmarshal(frame.Data, &ack); err != nil {
				t.Fatal(err)
			}
			identifiersMatch := frame.OpId == "watch" && frame.MsgIncr == "1"
			if frame.Code != 0 || !identifiersMatch {
				t.Fatalf("acceptance envelope: %s", raw)
			}
			if ack.Revision != 1 || ack.SnapshotIntervalMs != onlineSnapshotInterval.Milliseconds() {
				t.Fatalf("acceptance data: %s", raw)
			}
			ackSeen = true
		case OnlineChanged:
			assertOnlineSnapshot(t, decodeOnlineSnapshot(t, raw), 1, []user.OnlineStatus{{UserId: "u___2", Platforms: []int{}}})
			snapshotSeen = true
		default:
			t.Fatalf("unexpected subscription frame: %s", raw)
		}
	}
	if !ackSeen || !snapshotSeen {
		t.Fatalf("ack=%v snapshot=%v", ackSeen, snapshotSeen)
	}

	other, _, err := dialWs(url + "?platform_id=2&token=" + token(2))
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	readState := func(want user.OnlineStatus) {
		t.Helper()
		if err := watcher.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
			t.Fatal(err)
		}
		_, raw, err := watcher.ReadMessage()
		if err != nil {
			t.Fatal(err)
		}
		assertOnlineSnapshot(t, decodeOnlineSnapshot(t, raw), 1, []user.OnlineStatus{want})
	}
	readState(user.OnlineStatus{UserId: "u___2", Online: true, Platforms: []int{2}})
	if err := other.Close(); err != nil {
		t.Fatal(err)
	}
	readState(user.OnlineStatus{UserId: "u___2", Platforms: []int{}})
}
