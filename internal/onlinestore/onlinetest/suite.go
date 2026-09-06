package onlinetest

import (
	"context"
	"slices"
	"strconv"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/onlinestore"
)

// Opts describes what the implementation under test does not guarantee.
type Opts struct {
	// Purgeless: entries carry their own expiry, so PurgeNode leaves them in place.
	Purgeless bool
}

// Run drives the OnlineStore contract with a store clock the suite controls: setClock
// installs it, and the contract runs twice against the same store, so anything the first
// pass leaves behind breaks the second. A connection from another node, stamped far
// enough ahead to outlive every expiry step, must still be online at the end: a store
// that expires or purges more than its own keys fails here.
func Run(t *testing.T, s onlinestore.OnlineStore, setClock func(now func() time.Time), o Opts) {
	t.Helper()
	base := time.Now().Truncate(time.Millisecond)
	setClock(func() time.Time { return base })
	node := uuid.NewV7().String()
	foreign := onlinestore.ConnRef{UserId: identity.NativeUserId(uuid.NewV7().String()), PlatformId: 99, ConnId: uuid.NewV7().String()}
	base = base.Add(time.Hour)
	if err := s.Add(t.Context(), node, foreign); err != nil {
		t.Fatal(err)
	}
	base = base.Add(-time.Hour)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(t.Context()), 5*time.Second)
		defer cancel()
		if err := s.Remove(ctx, node, foreign); err != nil {
			t.Error(err)
		}
	})
	for i := range 2 {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			runContract(t, s, func() { base = base.Add(61 * time.Second) }, o)
		})
	}
	got, err := s.Online(t.Context(), []string{foreign.UserId})
	if err != nil || !slices.Equal(got[foreign.UserId], []int{foreign.PlatformId}) {
		t.Fatalf("unrelated presence lost: got=%v err=%v", got, err)
	}
}

// runContract checks the OnlineStore contract. expire advances the store's clock past ttl.
func runContract(t *testing.T, s onlinestore.OnlineStore, expire func(), o Opts) {
	t.Helper()
	ctx := t.Context()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	u1, u2, missing := identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String()), identity.NativeUserId(uuid.NewV7().String())
	n1, n2 := uuid.NewV7().String(), uuid.NewV7().String()
	a1 := onlinestore.ConnRef{UserId: u1, PlatformId: 1, ConnId: uuid.NewV7().String()}
	a2 := onlinestore.ConnRef{UserId: u1, PlatformId: 2, ConnId: uuid.NewV7().String()}
	a3 := onlinestore.ConnRef{UserId: u1, PlatformId: 2, ConnId: uuid.NewV7().String()} // same platform twice
	b1 := onlinestore.ConnRef{UserId: u2, PlatformId: 5, ConnId: uuid.NewV7().String()}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		for _, ref := range []struct {
			node string
			conn onlinestore.ConnRef
		}{{n1, a1}, {n1, a2}, {n2, a3}, {n2, b1}} {
			if err := s.Remove(ctx, ref.node, ref.conn); err != nil {
				t.Errorf("cleanup online ref: %v", err)
			}
		}
	})
	must(s.Add(ctx, n1, a1))
	must(s.Add(ctx, n1, a2))
	must(s.Add(ctx, n2, a3))
	must(s.Add(ctx, n2, b1))

	got, err := s.Online(ctx, []string{u1, u2, missing})
	must(err)
	if p := sorted(got[u1]); !slices.Equal(p, []int{1, 2}) {
		t.Fatalf("u1 platforms: %v", got)
	}
	if !slices.Equal(got[u2], []int{5}) || len(got[missing]) != 0 {
		t.Fatalf("others: %v", got)
	}

	must(s.Remove(ctx, n1, a2))
	got, _ = s.Online(ctx, []string{u1})
	if p := sorted(got[u1]); !slices.Equal(p, []int{1, 2}) {
		t.Fatalf("c3 still keeps platform 2 alive: %v", got)
	}
	must(s.Remove(ctx, n2, a3))
	got, _ = s.Online(ctx, []string{u1})
	if !slices.Equal(got[u1], []int{1}) {
		t.Fatalf("after removes: %v", got)
	}

	must(s.PurgeNode(ctx, n2))
	got, _ = s.Online(ctx, []string{u2})
	if len(got[u2]) != 0 && !o.Purgeless {
		t.Fatalf("purge: %v", got)
	}

	// Renew keeps n1 alive across an expiry window; n2's leftovers die.
	must(s.Add(ctx, n2, b1))
	expire()
	must(s.Renew(ctx, n1, []onlinestore.ConnRef{a1}))
	got, _ = s.Online(ctx, []string{u1, u2})
	if !slices.Equal(got[u1], []int{1}) || len(got[u2]) != 0 {
		t.Fatalf("after expiry: %v", got)
	}
}

func sorted(xs []int) []int { return slices.Sorted(slices.Values(xs)) }
