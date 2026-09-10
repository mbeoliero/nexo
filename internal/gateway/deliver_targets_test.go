package gateway

import (
	"context"
	"encoding/json/v2"
	"errors"
	"slices"
	"testing"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type pushVisibilityStore struct {
	message.Store
	calls          int
	conversationId string
	seq            int64
	owners         []string
	visible        []string
	err            error
}

func (s *pushVisibilityStore) VisibleOwners(
	_ context.Context, conversationId string, owners []string, seq int64,
) ([]string, error) {
	s.calls++
	s.conversationId, s.seq, s.owners = conversationId, seq, slices.Clone(owners)
	return slices.Clone(s.visible), s.err
}

func TestDeliverFiltersConnectionsBeforeVisibility(t *testing.T) {
	for _, tt := range []struct {
		name       string
		other      bool
		peer       bool
		except     string
		owners     []string
		visible    []string
		wantPushes []string
		err        error
	}{
		{name: "origin only", except: "origin"},
		{name: "other sender device", other: true, except: "origin",
			owners: []string{"u___1"}, visible: []string{"u___1"}, wantPushes: []string{"other"}},
		{name: "local peer", peer: true, except: "origin",
			owners: []string{"u___2"}, visible: []string{"u___2"}, wantPushes: []string{"peer"}},
		{name: "invisible peer", peer: true, except: "origin", owners: []string{"u___2"}},
		{name: "visibility error", peer: true, except: "origin",
			owners: []string{"u___2"}, err: errors.New("visibility unavailable")},
		{name: "no excluded connection", peer: true,
			owners: []string{"u___1", "u___2"}, visible: []string{"u___1", "u___2"}, wantPushes: []string{"origin", "peer"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			st := &pushVisibilityStore{Store: message.Adapt(storetest.NewMem()), visible: tt.visible, err: tt.err}
			g := New(testConfig(), Deps{Message: message.New(st, message.NoopPublisher{}, message.Config{MaxContentBytes: 8192})})
			t.Cleanup(g.cancel)
			t.Cleanup(g.cancelRun)
			clients := []*Client{}
			add := func(userId, connId string) {
				c := g.newClient(auth.Identity{UserId: userId}, connId, "", newFakeConn())
				if err := g.users.Register(c); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { c.Close("test") })
				clients = append(clients, c)
			}
			add("u___1", "origin")
			if tt.other {
				add("u___1", "other")
			}
			if tt.peer {
				add("u___2", "peer")
			}
			g.Deliver(t.Context(), message.PushEvent{
				ConversationId: "si_u___1:u___2", SessionType: store.ConversationSingle,
				SenderId: "u___1", RecvId: "u___2", SenderConnId: tt.except,
				Message: message.Message{Seq: 7},
			})
			if st.calls != min(len(tt.owners), 1) || !slices.Equal(st.owners, tt.owners) {
				t.Fatalf("visibility calls = %d, owners = %v, want only %v", st.calls, st.owners, tt.owners)
			}
			if st.calls > 0 && (st.seq != 7 || st.conversationId != "si_u___1:u___2") {
				t.Fatalf("visibility scope = %q seq=%d", st.conversationId, st.seq)
			}
			for _, c := range clients {
				select {
				case raw := <-c.send:
					var frame Response
					if err := json.Unmarshal(raw, &frame); err != nil {
						t.Fatal(err)
					}
					if !slices.Contains(tt.wantPushes, c.Id) || frame.ReqId != PushMsg {
						t.Fatalf("unexpected push to %s: %+v", c.Id, frame)
					}
				default:
					if slices.Contains(tt.wantPushes, c.Id) {
						t.Fatalf("missing push to %s", c.Id)
					}
				}
			}
		})
	}
}
