package main

import (
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/sdk"
)

type frame struct {
	ReqId int            `json:"req_id"`
	OpId  string         `json:"op_id"`
	Code  *int           `json:"code"`
	Data  jsontext.Value `json:"data"`
}

func (r *runner) provision(ctx context.Context, i int, httpClient *http.Client, password string) error {
	u := &r.users[i]
	u.client = sdk.New(r.opts.nodes[u.node], sdk.WithHttpClient(httpClient))
	name := "load_" + r.runId + "_" + strconv.Itoa(i)
	profile, err := u.client.Register(ctx, sdk.RegisterRequest{Username: name, Password: password, Nickname: name})
	if err != nil {
		return fmt.Errorf("register user=%d: %w", i, err)
	}
	session, err := u.client.Login(ctx, sdk.LoginRequest{Username: name, Password: password, PlatformId: sdk.PlatformWeb})
	if err != nil {
		return fmt.Errorf("login user=%d: %w", i, err)
	}
	if profile.UserId == "" || session.Token == "" || session.UserId != profile.UserId {
		return fmt.Errorf("register/login identity mismatch user=%d", i)
	}
	u.id, u.token = session.UserId, session.Token
	return nil
}

func (r *runner) connect(ctx context.Context, i int) error {
	u := &r.users[i]
	endpoint, _ := url.Parse(r.opts.nodes[u.node]) // Validated before creating any accounts.
	endpoint.Scheme = lo.Ternary(endpoint.Scheme == "https", "wss", "ws")
	endpoint.Path = "/ws"
	endpoint.RawQuery = "platform_id=5"
	dialer := websocket.Dialer{HandshakeTimeout: r.opts.timeout}
	ws, resp, err := dialer.DialContext(ctx, endpoint.String(), http.Header{"Authorization": {"Bearer " + u.token}})
	if err != nil {
		if resp != nil {
			_ = resp.Body.Close()
		}
		return fmt.Errorf("connect user=%d node=%d: %w", i, u.node+1, err)
	}
	u.ws = ws
	ws.SetReadLimit(65536)
	r.readers.Go(func() {
		for {
			kind, raw, err := ws.ReadMessage()
			if err != nil {
				if !r.closing.Load() {
					r.disconnect.Add(1)
					r.fail("disconnect user=%d node=%d: %v", i, u.node+1, err)
				}
				select {
				case u.ready <- fmt.Errorf("connection closed user=%d", i):
				default:
				}
				return
			}
			if kind != websocket.TextMessage {
				r.fail("non-text WS frame user=%d", i)
				continue
			}
			r.receive(i, raw)
		}
	})
	if err := ws.SetWriteDeadline(time.Now().Add(r.opts.timeout)); err != nil {
		return err
	}
	if err := ws.WriteMessage(websocket.TextMessage, []byte(`{"req_id":1001,"op_id":"ready","data":{}}`)); err != nil {
		return err
	}
	timer := time.NewTimer(r.opts.timeout)
	defer timer.Stop()
	select {
	case err := <-u.ready:
		if err != nil {
			return err
		}
		r.connected.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("application readiness timeout user=%d node=%d", i, u.node+1)
	}
}

func (r *runner) send(i, j int, scheduled, deadline time.Time) error {
	if now := time.Now(); now.After(deadline) {
		return fmt.Errorf(
			"send window expired: message_index=%d scheduled_offset=%s late_by=%s cutoff_overrun=%s remaining=%d",
			j,
			scheduled.Sub(r.started),
			now.Sub(scheduled),
			now.Sub(deadline),
			len(r.users[i].records)-j,
		)
	}
	u := &r.users[i]
	id := r.messageId(i, j)
	raw, err := json.Marshal(struct {
		ReqId int                    `json:"req_id"`
		OpId  string                 `json:"op_id"`
		Data  sdk.SendMessageRequest `json:"data"`
	}{ReqId: 1003, OpId: id, Data: sdk.SendMessageRequest{
		ClientMsgId: id, SessionType: sdk.SessionTypeSingle, RecvId: r.users[i^1].id,
		ContentType: sdk.ContentTypeText, Content: r.content(i, j),
	}})
	if err != nil {
		return err
	}
	r.pairs[i/2].Lock()
	u.records[j].started = time.Now()
	r.pairs[i/2].Unlock()
	writeDeadline := time.Now().Add(r.opts.timeout)
	if deadline.Before(writeDeadline) {
		writeDeadline = deadline
	}
	if err := u.ws.SetWriteDeadline(writeDeadline); err != nil {
		return err
	}
	if err := u.ws.WriteMessage(websocket.TextMessage, raw); err != nil {
		return err
	}
	r.pairs[i/2].Lock()
	u.records[j].written = true
	r.pairs[i/2].Unlock()
	r.sent.Add(1)
	return nil
}

func (r *runner) receive(user int, raw []byte) {
	var f frame
	if err := json.Unmarshal(raw, &f); err != nil {
		r.fail("invalid WS JSON user=%d", user)
		return
	}
	if f.ReqId == 1001 && f.OpId == "ready" {
		var result sdk.MaxSeqsResult
		var err error
		if f.Code == nil || *f.Code != 0 || json.Unmarshal(f.Data, &result) != nil || len(result.Items) != 0 {
			err = fmt.Errorf("invalid readiness response user=%d; fresh account must have no conversations", user)
		}
		select {
		case r.users[user].ready <- err:
		default:
			r.fail("duplicate readiness response user=%d", user)
		}
		return
	}
	if f.ReqId == 2002 {
		r.fail("unexpected kick user=%d node=%d", user, r.users[user].node+1)
		return
	}
	if f.ReqId == 2004 {
		r.resync.Add(1)
		return
	}
	if f.ReqId == 2003 {
		return
	}
	if r.frozen.Load() {
		return
	}
	switch f.ReqId {
	case 1003:
		i, j, err := r.locate(f.OpId)
		if err != nil || i != user {
			r.fail("ACK op_id mismatch user=%d", user)
			return
		}
		var ack sdk.Ack
		if f.Code == nil || *f.Code != 0 || json.Unmarshal(f.Data, &ack) != nil {
			r.fail("invalid/error ACK user=%d message=%s code=%d (-1=missing)", user, f.OpId, lo.FromPtrOr(f.Code, -1))
			return
		}
		r.pairs[i/2].Lock()
		defer r.pairs[i/2].Unlock()
		if r.frozen.Load() {
			return
		}
		rec := &r.users[i].records[j]
		if rec.started.IsZero() || ack.ConversationId != r.conversation(i) || !validIdentity(identityOf(ack)) {
			r.fail("ACK identity mismatch user=%d message=%s", user, f.OpId)
			return
		}
		if rec.ackSeen {
			r.fail("duplicate ACK user=%d message=%s", user, f.OpId)
			return
		}
		rec.ackSeen, rec.ack, rec.ackLatency = true, identityOf(ack), time.Since(rec.started)
		r.acked.Add(1)
	case 2001:
		var msg sdk.Message
		if err := json.Unmarshal(f.Data, &msg); err != nil {
			r.fail("invalid push JSON user=%d", user)
			return
		}
		i, j, err := r.checkMessage(msg)
		if err != nil || i^1 != user {
			r.fail("push content/recipient mismatch recipient=%d message=%s: %v", user, msg.ClientMsgId, err)
			return
		}
		r.pairs[i/2].Lock()
		defer r.pairs[i/2].Unlock()
		if r.frozen.Load() {
			return
		}
		rec := &r.users[i].records[j]
		if rec.started.IsZero() || rec.pushSeen {
			r.fail("unsent/duplicate push recipient=%d message=%s", user, msg.ClientMsgId)
			return
		}
		rec.pushSeen, rec.push, rec.pushLatency = true, messageIdentity(msg), time.Since(rec.started)
		r.pushed.Add(1)
	default:
		r.fail("unexpected req_id=%d user=%d", f.ReqId, user)
	}
}
