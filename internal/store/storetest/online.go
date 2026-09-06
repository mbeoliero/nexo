package storetest

import (
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/store"
)

func RunOnline(t *testing.T, s store.Store) {
	ctx := t.Context()
	now := baseTime
	add := func(conn, user string, platform int32, node string, at time.Time) {
		if err := s.UpsertOnlineConn(ctx, &store.OnlineConn{ConnId: conn, UserId: user, PlatformId: platform, NodeId: node, HeartbeatAt: at}); err != nil {
			t.Fatal(err)
		}
	}
	add("c1", "u___1", 1, "n1", now)
	add("c2", "u___1", 2, "n2", now)
	add("c3", "u___2", 5, "n2", now.Add(-2*time.Minute))
	add("c1", "u___1", 3, "n1", now) // upsert moves the platform

	rows, err := s.ListOnlineConns(ctx, []string{"u___1", "u___2"}, now.Add(-time.Minute))
	if err != nil || len(rows) != 2 {
		t.Fatalf("list: %v %+v", err, rows)
	}
	for _, r := range rows {
		if r.ConnId == "c1" && r.PlatformId != 3 {
			t.Fatalf("upsert did not update: %+v", r)
		}
	}
	// Renew covers only the listed conns: c3 (a leftover row) stays stale and expires by TTL.
	if err := s.RenewOnlineConns(ctx, "n2", []string{"c2"}, now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if rows, _ = s.ListOnlineConns(ctx, []string{"u___1"}, now); len(rows) != 1 || rows[0].ConnId != "c2" || !rows[0].HeartbeatAt.Equal(now.Add(time.Minute)) {
		t.Fatalf("renew listed: %+v", rows)
	}
	if rows, _ = s.ListOnlineConns(ctx, []string{"u___2"}, now); len(rows) != 0 {
		t.Fatalf("unlisted conn must not be renewed: %+v", rows)
	}
	if err := s.DeleteOnlineConnsByNode(ctx, "n2"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteOnlineConn(ctx, "c1"); err != nil {
		t.Fatal(err)
	}
	if rows, _ = s.ListOnlineConns(ctx, []string{"u___1", "u___2"}, now.Add(-time.Hour)); len(rows) != 0 {
		t.Fatalf("after deletes: %+v", rows)
	}
}
