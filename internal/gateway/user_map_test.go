package gateway

import (
	"context"
	"testing"

	"github.com/mbeoliero/nexo/internal/cluster"
)

func TestRegisterAndUnregisterAreLocalOnly(t *testing.T) {
	userMap := NewUserMap(nil)
	client := &Client{UserId: "u1", PlatformId: 5, ConnId: "c1"}

	userMap.Register(context.Background(), client)

	clients, ok := userMap.GetAll("u1")
	if !ok || len(clients) != 1 {
		t.Fatalf("register failed: ok=%v len=%d", ok, len(clients))
	}

	removed, offline := userMap.Unregister(context.Background(), client)
	if !removed {
		t.Fatal("expected unregister to remove local connection")
	}
	if !offline {
		t.Fatal("expected unregister to mark user offline")
	}

	if _, ok := userMap.GetAll("u1"); ok {
		t.Fatal("expected local clients to be removed")
	}
}

func TestSnapshotRouteConnsReturnsAllLocalConnections(t *testing.T) {
	userMap := NewUserMap(nil)
	userMap.Register(context.Background(), &Client{UserId: "u1", PlatformId: 5, ConnId: "c1"})
	userMap.Register(context.Background(), &Client{UserId: "u2", PlatformId: 1, ConnId: "c2"})

	got := userMap.SnapshotRouteConns("i1")
	if len(got) != 2 {
		t.Fatalf("snapshot len mismatch: got %d", len(got))
	}

	want := map[string]cluster.RouteConn{
		"c1": {UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5},
		"c2": {UserId: "u2", ConnId: "c2", InstanceId: "i1", PlatformId: 1},
	}
	for _, conn := range got {
		if want[conn.ConnId] != conn {
			t.Fatalf("route conn mismatch for %s: got %+v want %+v", conn.ConnId, conn, want[conn.ConnId])
		}
	}
}

func TestGetByConnIDReturnsExactClient(t *testing.T) {
	userMap := NewUserMap(nil)
	client := &Client{UserId: "u1", PlatformId: 5, ConnId: "c1"}
	userMap.Register(context.Background(), client)

	got, ok := userMap.GetByConnId("c1")
	if !ok || got != client {
		t.Fatalf("get by conn id mismatch: ok=%v got=%p want=%p", ok, got, client)
	}
}
