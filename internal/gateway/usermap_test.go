package gateway

import (
	"testing"

	"github.com/mbeoliero/nexo/errcode"
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
