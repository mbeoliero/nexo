package gateway

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
)

type onlineHintBus struct {
	events   []bus.Event
	contexts []context.Context
	err      error
}

func (b *onlineHintBus) Publish(ctx context.Context, ev bus.Event) error {
	b.events = append(b.events, ev)
	b.contexts = append(b.contexts, ctx)
	return b.err
}

func (*onlineHintBus) DegradedPublishes() (int64, bool)                         { return 0, false }
func (*onlineHintBus) Subscribe(context.Context, func(bus.Event), func()) error { return nil }

func TestOnlineChangedChunksPreserveUsersAndByteLimit(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{name: "identity ids", prefix: "u___"},
		{name: "unicode and json escapes", prefix: "在线\"\\\n\t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ids := make([]string, 0, 4000)
			for i := range 2000 {
				id := fmt.Sprintf("%s%d", tc.prefix, i+1)
				ids = append(ids, id, id)
			}
			slices.Reverse(ids)
			original := slices.Clone(ids)
			b := &onlineHintBus{}
			cfg := testConfig()
			cfg.NodeId = "node-\"在线\\"
			g := New(cfg, Deps{Bus: b})
			t.Cleanup(func() { shutdownBusGateway(t, g) })
			g.publishOnlineChanged(t.Context(), ids)
			if !slices.Equal(ids, original) {
				t.Fatal("publication mutated the caller's user IDs")
			}
			if len(b.events) < 2 {
				t.Fatalf("got %d events, want multiple bounded chunks", len(b.events))
			}
			got := []string{}
			for i, ev := range b.events {
				if ev.Type != bus.TypePresenceChanged || ev.NodeId != cfg.NodeId {
					t.Fatalf("unexpected event: type=%q node=%q", ev.Type, ev.NodeId)
				}
				raw, err := json.Marshal(ev)
				if err != nil {
					t.Fatal(err)
				}
				if len(raw) > bus.MaxPayloadBytes {
					t.Fatalf("event %d has %d bytes, limit %d", i, len(raw), bus.MaxPayloadBytes)
				}
				var p bus.PresenceChanged
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatal(err)
				}
				if len(p.UserIds) == 0 {
					t.Fatalf("empty chunk %d", i)
				}
				got = append(got, p.UserIds...)
				if b.contexts[i] != t.Context() {
					t.Fatal("publication replaced the caller's operation context")
				}
			}
			slices.Sort(original)
			want := slices.Compact(original)
			if !slices.Equal(got, want) {
				t.Fatalf("published %d users, want the exact sorted union of %d users", len(got), len(want))
			}
		})
	}
}

// Every chunk is attempted exactly once and counted exactly once: no chunk is retried, and a chunk
// that fails does not cancel the ones behind it, whose users would otherwise wait a whole snapshot
// interval for their reread (design §7.4).
func TestOnlineChangedPublishFailureIsCountedPerChunkWithoutRetry(t *testing.T) {
	t.Parallel()
	ids := make([]string, 2000)
	for i := range ids {
		ids[i] = fmt.Sprintf("u___%d", i+1)
	}
	ok := &onlineHintBus{}
	g := New(testConfig(), Deps{Bus: ok})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	g.publishOnlineChanged(t.Context(), ids)
	chunks := len(ok.events)
	if chunks < 2 {
		t.Fatalf("chunks=%d, want a list that spans several chunks", chunks)
	}
	b := &onlineHintBus{err: errors.New("bus unavailable")}
	f := New(testConfig(), Deps{Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, f) })
	f.publishOnlineChanged(t.Context(), ids)
	if len(b.events) != chunks || f.onlinePublishFails.Load() != int64(chunks) {
		t.Fatalf("attempts=%d failures=%d, want %d of each", len(b.events), f.onlinePublishFails.Load(), chunks)
	}
}

func TestOnlineChangedRejectsOversizedEvent(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		nodeId string
		userId string
	}{
		{name: "user id", nodeId: "n1", userId: strings.Repeat("x", bus.MaxPayloadBytes)},
		{name: "envelope", nodeId: strings.Repeat("n", bus.MaxPayloadBytes), userId: "u___1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := &onlineHintBus{}
			cfg := testConfig()
			cfg.NodeId = tc.nodeId
			g := New(cfg, Deps{Bus: b})
			t.Cleanup(func() { shutdownBusGateway(t, g) })
			g.publishOnlineChanged(t.Context(), []string{tc.userId})
			if len(b.events) != 0 || g.onlinePublishFails.Load() != 1 {
				t.Fatalf("published=%d failures=%d, want no oversized event", len(b.events), g.onlinePublishFails.Load())
			}
		})
	}
}

// A long node_id must not silently shrink chunks toward one id per bus event: the chunk size is
// fixed and only the envelope around it is checked, so any node_id that still fits keeps full chunks.
// worstCaseIds are the longest ids identity.Valid admits, which is what maxUserIdJsonBytes is sized
// for: the native prefix plus a canonical UUID, pure ASCII so nothing is escaped.
func worstCaseIds(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("nx__%08x-0000-4000-8000-%012x", i+1, i+1)
	}
	return ids
}

func publishToNode(t *testing.T, nodeIdLen int, ids []string) (*Gateway, *onlineHintBus) {
	t.Helper()
	b := &onlineHintBus{}
	cfg := testConfig()
	cfg.NodeId = strings.Repeat("n", nodeIdLen)
	g := New(cfg, Deps{Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	g.publishOnlineChanged(t.Context(), ids)
	return g, b
}

// longestNodeIdWithinBudget probes for the boundary instead of recomputing it, so the assertions
// around it measure the envelope publishOnlineChanged actually builds rather than a second copy of
// the budget arithmetic. Acceptance only shrinks as the node_id grows, so a bisection finds it.
func longestNodeIdWithinBudget(t *testing.T, ids []string) int {
	t.Helper()
	fits := func(n int) bool {
		g, _ := publishToNode(t, n, ids)
		return g.onlinePublishFails.Load() == 0
	}
	// A node_id of presenceEnvelopeBudget bytes cannot fit: the envelope holds it plus its own keys.
	low, high := 0, presenceEnvelopeBudget
	if !fits(low) {
		t.Fatal("a node without a node_id cannot publish a full chunk of longest-possible ids")
	}
	for low+1 < high {
		if mid := (low + high) / 2; fits(mid) {
			low = mid
		} else {
			high = mid
		}
	}
	return low
}

// The budget must admit exactly the node ids whose events fit, so it is asserted at the boundary and
// with the longest ids the gateway can issue: a chunk is the ids plus the object around them, and
// omitting that wrapper leaves an accepted node_id whose every chunk then overruns MaxPayloadBytes.
func TestOnlineChangedLongNodeIdKeepsFullChunks(t *testing.T) {
	t.Parallel()
	ids := worstCaseIds(250)
	// The budget only means anything if what it reserves for a chunk holds a real one: the ids are
	// not alone on the wire, they sit inside an object that costs bytes of its own.
	full, err := json.Marshal(bus.PresenceChanged{UserIds: ids[:presenceChunkSize]})
	if err != nil {
		t.Fatal(err)
	}
	if len(full) > presenceChunkJsonBytes {
		t.Fatalf("a %d-id chunk encodes to %d bytes, past the %d reserved for it", presenceChunkSize, len(full), presenceChunkJsonBytes)
	}
	for name, nodeIdLen := range map[string]int{
		"half the budget":       presenceEnvelopeBudget / 2,
		"longest within budget": longestNodeIdWithinBudget(t, ids),
	} {
		t.Run(name, func(t *testing.T) {
			g, b := publishToNode(t, nodeIdLen, ids)
			sizes := []int{}
			for _, ev := range b.events {
				var p bus.PresenceChanged
				if err := json.Unmarshal(ev.Payload, &p); err != nil {
					t.Fatal(err)
				}
				sizes = append(sizes, len(p.UserIds))
				wire, err := json.Marshal(ev)
				if err != nil {
					t.Fatal(err)
				}
				if len(wire) > bus.MaxPayloadBytes {
					t.Fatalf("event of %d ids is %d bytes, over the %d the bus carries", len(p.UserIds), len(wire), bus.MaxPayloadBytes)
				}
			}
			want := []int{presenceChunkSize, presenceChunkSize, len(ids) - 2*presenceChunkSize}
			if !slices.Equal(sizes, want) || g.onlinePublishFails.Load() != 0 {
				t.Fatalf("chunk sizes %v failures=%d, want %v and none", sizes, g.onlinePublishFails.Load(), want)
			}
		})
	}
}

// One byte past that budget the publish fails once and visibly, rather than degrading to a chunk per
// id, and it keeps failing that way however much longer the node_id gets.
func TestOnlineChangedOverBudgetNodeIdFailsOnce(t *testing.T) {
	t.Parallel()
	ids := worstCaseIds(250)
	for name, nodeIdLen := range map[string]int{
		"one byte over":    longestNodeIdWithinBudget(t, ids) + 1,
		"the whole budget": presenceEnvelopeBudget,
	} {
		t.Run(name, func(t *testing.T) {
			g, b := publishToNode(t, nodeIdLen, ids)
			if len(b.events) != 0 || g.onlinePublishFails.Load() != 1 {
				t.Fatalf("published=%d failures=%d, want no event and a single failure", len(b.events), g.onlinePublishFails.Load())
			}
		})
	}
}

func TestOnlineChangedEmptyListDoesNotPublish(t *testing.T) {
	t.Parallel()
	b := &onlineHintBus{}
	g := New(testConfig(), Deps{Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	g.publishOnlineChanged(t.Context(), nil)
	if len(b.events) != 0 || g.onlinePublishFails.Load() != 0 {
		t.Fatalf("empty list published=%d failures=%d", len(b.events), g.onlinePublishFails.Load())
	}
}

// presence_changed announces a registration this node just created, so it must follow the write and
// not the attempt: Add returns without writing for a connection that is already registered or that
// lost its context to a kick, and neither no-op is a presence change (design §7.4).
func TestOnlineAddPublishesOnlyTheRegistrationsItWrote(t *testing.T) {
	t.Parallel()
	online, b := &fakeOnline{}, &onlineHintBus{}
	g := New(testConfig(), Deps{Online: online, Bus: b})
	t.Cleanup(func() { shutdownBusGateway(t, g) })
	assert := func(what string, writes, events int) {
		t.Helper()
		if len(online.added) != writes || len(b.events) != events {
			t.Fatalf("%s: writes=%d events=%d, want %d and %d", what, len(online.added), len(b.events), writes, events)
		}
	}

	c := g.newClient(auth.Identity{UserId: "u___1"}, "conn-1", "", newFakeConn())
	g.onlineAdd(c)
	assert("first registration", 1, 1)
	g.onlineAdd(c)
	assert("already registered", 1, 1)

	kicked := g.newClient(auth.Identity{UserId: "u___2"}, "conn-2", "", newFakeConn())
	kicked.cancelActive()
	if added, err := g.addOnline(t.Context(), kicked); added || err != nil {
		t.Fatalf("kicked connection reported a write: added=%v err=%v", added, err)
	}
	g.onlineAdd(kicked)
	assert("kicked connection", 1, 1)
}
