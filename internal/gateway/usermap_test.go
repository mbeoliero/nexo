package gateway

import (
	"slices"
	"testing"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/config"
)

// Reserve, not Adopt, must be what counts against limits.ws_conns_per_user: concurrent handshakes
// all reserve before any of them adopts, so counting adopted clients let a user open any number of
// connections at once.
func TestReserveCountsUnadoptedConnections(t *testing.T) {
	m := NewUserMap(config.LimitsConfig{WsConnsPerUser: 2, WsConnsTotal: 100})
	s := Slot{UserId: "u___1", Ip: "10.0.0.1"}
	for i := range 2 {
		if err := m.Reserve(s); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	err := m.Reserve(s)
	if e := errcode.From(err); e.Code != errcode.ErrConnOverLimit.Code {
		t.Fatalf("third concurrent handshake: %v", err)
	}
	m.Release(s)
	if err := m.Reserve(s); err != nil {
		t.Fatalf("reserve after release: %v", err)
	}
}

// A drained node answers 503 (retry another node now), not 429 (back off).
func TestReserveWhileClosing(t *testing.T) {
	m := NewUserMap(config.LimitsConfig{WsConnsPerUser: 2, WsConnsTotal: 100})
	m.Close()
	err := m.Reserve(Slot{UserId: "u___1"})
	if e := errcode.From(err); e.Code != errcode.ErrNodeDraining.Code {
		t.Fatalf("draining node: %v", err)
	}
	if got := handshakeStatus(err); got != 503 {
		t.Fatalf("handshake status = %d, want 503", got)
	}
}

// Every path that gives a slot back must give back all four counters, or a node leaks capacity
// until it starts refusing connections it has room for.
func TestReleaseRestoresEveryCounter(t *testing.T) {
	m := NewUserMap(config.LimitsConfig{WsConnsPerUser: 1, WsConnsPerToken: 1, WsConnsPerIp: 1, WsConnsTotal: 1})
	s := Slot{UserId: "u___1", TokenId: "t1", Ip: "10.0.0.1"}
	for range 3 {
		if err := m.Reserve(s); err != nil {
			t.Fatalf("reserve: %v", err)
		}
		m.Release(s)
	}
	if m.Count() != 0 || len(m.byUserN) != 0 || len(m.byToken) != 0 || len(m.byIp) != 0 {
		t.Fatalf("leaked: total=%d user=%v token=%v ip=%v", m.total, m.byUserN, m.byToken, m.byIp)
	}
}

// exceptConnId names one connection, never a user: a user is dropped only when that connection is
// the single one they have here, and the surviving ids keep the caller's order.
func TestOnlineExceptSemanticsAndInputOrder(t *testing.T) {
	g := New(testConfig(), Deps{})
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	add := func(userId, connId string) {
		c := g.newClient(auth.Identity{UserId: userId}, connId, "", newFakeConn())
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close("test") })
	}
	add("u___1", "only")     // one connection, and it is the excluded one
	add("u___2", "excluded") // two connections, one of them excluded
	add("u___2", "sibling")
	add("u___3", "unrelated") // no connection matches the exclusion
	ids := []string{"u___3", "u___4", "u___1", "u___2"}
	for _, tc := range []struct {
		name   string
		except string
		want   []string
	}{
		{name: "no exclusion", except: "", want: []string{"u___3", "u___1", "u___2"}},
		{name: "sole connection excluded", except: "only", want: []string{"u___3", "u___2"}},
		{name: "one of two connections excluded", except: "excluded", want: []string{"u___3", "u___1", "u___2"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := g.users.OnlineExcept(ids, tc.except); !slices.Equal(got, tc.want) {
				t.Fatalf("OnlineExcept(%v, %q) = %v, want %v", ids, tc.except, got, tc.want)
			}
		})
	}
}

// The candidate filter, not just fanout, must drop the sending connection: that is what lets
// Deliver skip the visibility query on a node whose only local connection is the sender.
func TestOnlineExceptSkipsTheSendingConnection(t *testing.T) {
	g := New(testConfig(), Deps{})
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	add := func(userId, connId string) {
		c := g.newClient(auth.Identity{UserId: userId}, connId, "", newFakeConn())
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close("test") })
	}
	add("u___1", "origin")
	add("u___2", "peer")
	ids := []string{"u___1", "u___2", "u___3"}
	if got := g.users.OnlineExcept(ids, "origin"); !slices.Equal(got, []string{"u___2"}) {
		t.Fatalf("sender's only connection excluded: %v", got)
	}
	add("u___1", "other")
	if got := g.users.OnlineExcept(ids, "origin"); !slices.Equal(got, []string{"u___1", "u___2"}) {
		t.Fatalf("sender's second device kept: %v", got)
	}
	// HTTP and internal sends carry no connection id, so every online candidate stays.
	if got := g.users.OnlineExcept(ids, ""); !slices.Equal(got, []string{"u___1", "u___2"}) {
		t.Fatalf("no excluded connection: %v", got)
	}
}
