package db

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/onlinestore/onlinetest"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func TestStore(t *testing.T) {
	s := New(storetest.NewMem(), 60*time.Second)
	onlinetest.Run(t, s, func(now func() time.Time) { s.now = now }, onlinetest.Opts{})
}

type chunkRecorder struct {
	store.Store
	batches [][]string
	stamps  []time.Time
}

func (c *chunkRecorder) RenewOnlineConns(_ context.Context, _ string, connIds []string, now time.Time) error {
	c.batches = append(c.batches, slices.Clone(connIds))
	c.stamps = append(c.stamps, now)
	return nil
}

// gormstore renders `conn_id IN ?` as one placeholder per id, and MySQL refuses a statement with
// more than 65535 of them: a node holding that many connections would silently stop renewing.
func TestRenewChunksLargeBatches(t *testing.T) {
	rec := &chunkRecorder{Store: storetest.NewMem()}
	s := New(rec, 60*time.Second)
	conns := make([]onlinestore.ConnRef, 1200)
	for i := range conns {
		conns[i] = onlinestore.ConnRef{ConnId: strconv.Itoa(i), UserId: "u___1", PlatformId: 1}
	}
	if err := s.Renew(t.Context(), "n1", conns); err != nil {
		t.Fatal(err)
	}
	if got := []int{len(rec.batches), len(rec.batches[0]), len(rec.batches[1]), len(rec.batches[2])}; !slices.Equal(got, []int{3, 500, 500, 200}) {
		t.Fatalf("chunks = %v, want [3 500 500 200]", got)
	}
	// One clock reading for the whole batch: otherwise a slow renew makes the last connections look
	// fresher than the first, and ttl expiry no longer means the same thing across a node.
	if !slices.Equal(rec.stamps, []time.Time{rec.stamps[0], rec.stamps[0], rec.stamps[0]}) {
		t.Fatalf("stamps drift across chunks: %v", rec.stamps)
	}
	if len(rec.batches[0]) > 0 && rec.batches[0][0] != "0" {
		t.Fatalf("first chunk starts at %q", rec.batches[0][0])
	}
}
