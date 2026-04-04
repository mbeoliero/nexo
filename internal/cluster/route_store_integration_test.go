package cluster

import (
	"context"
	"testing"
	"time"
)

func TestRouteStoreRegisterUnregisterRoundTripWithRedis(t *testing.T) {
	_, rdb := newTestRedis(t)
	manager := NewInstanceManager(rdb, time.Minute)
	store := NewRouteStore(rdb, time.Minute, manager)
	ctx := context.Background()
	conn := RouteConn{UserId: "u1", ConnId: "c1", InstanceId: "i1", PlatformId: 5}

	if err := manager.WriteState(ctx, InstanceState{InstanceId: "i1", Ready: true, Routeable: true}); err != nil {
		t.Fatalf("write state: %v", err)
	}
	if err := store.RegisterConn(ctx, conn); err != nil {
		t.Fatalf("register conn: %v", err)
	}

	got, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs: %v", err)
	}
	if len(got["u1"]) != 1 || got["u1"][0] != conn {
		t.Fatalf("push refs mismatch: %+v", got["u1"])
	}

	if err := store.UnregisterConn(ctx, conn); err != nil {
		t.Fatalf("unregister conn: %v", err)
	}

	got, err = store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs after unregister: %v", err)
	}
	if len(got["u1"]) != 0 {
		t.Fatalf("expected empty refs after unregister: %+v", got["u1"])
	}
}

func TestRouteStoreReadPathCleansDanglingIndexEntriesWithRedis(t *testing.T) {
	_, rdb := newTestRedis(t)
	store := NewRouteStore(rdb, time.Minute, nil)
	ctx := context.Background()
	danglingKey := routeConnKey("i1", "missing")

	if err := rdb.SAdd(ctx, routeUserKey("u1"), danglingKey).Err(); err != nil {
		t.Fatalf("seed user index: %v", err)
	}
	if err := rdb.SAdd(ctx, routeInstanceKey("i1"), danglingKey).Err(); err != nil {
		t.Fatalf("seed instance index: %v", err)
	}

	got, err := store.GetUsersConnRefs(ctx, []string{"u1"})
	if err != nil {
		t.Fatalf("get users conn refs: %v", err)
	}
	if len(got["u1"]) != 0 {
		t.Fatalf("expected empty refs: %+v", got["u1"])
	}
	if members := rdb.SMembers(ctx, routeUserKey("u1")).Val(); len(members) != 0 {
		t.Fatalf("dangling user members not cleaned: %+v", members)
	}
	if members := rdb.SMembers(ctx, routeInstanceKey("i1")).Val(); len(members) != 0 {
		t.Fatalf("dangling instance members not cleaned: %+v", members)
	}
}
