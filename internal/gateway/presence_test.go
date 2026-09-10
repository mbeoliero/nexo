package gateway

import (
	"cmp"
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	onlinedb "github.com/mbeoliero/nexo/internal/onlinestore/db"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type subscriptionOnline struct {
	fakeOnline
	read  func(context.Context, []string) (map[string][]int, error)
	calls atomic.Int32
}

func (s *subscriptionOnline) Online(ctx context.Context, ids []string) (map[string][]int, error) {
	s.calls.Add(1)
	if s.read == nil {
		return map[string][]int{}, nil
	}
	return s.read(ctx, ids)
}

type subscriptionReply struct {
	ReqId int            `json:"req_id"`
	Code  int            `json:"code"`
	Data  jsontext.Value `json:"data"`
}

func newSubscriptionGateway(t *testing.T, online onlinestore.OnlineStore, cfg *config.Config) *Gateway {
	t.Helper()
	cfg = cmp.Or(cfg, testConfig())
	cfg.NodeId = cmp.Or(cfg.NodeId, "watcher")
	cfg.Ws.PingInterval = time.Hour
	cfg.OnlineStore.RenewInterval = time.Hour
	users := user.New(storetest.NewMem(), nil, online)
	g := New(cfg, Deps{Online: online, User: users})
	startSubscriptionGateway(t, g)
	return g
}

func startSubscriptionGateway(t *testing.T, g *Gateway) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- g.Run(t.Context()) }()
	t.Cleanup(func() {
		shutdownBusGateway(t, g)
		waitBusExit(t, done)
	})
	select {
	case <-g.Ready():
	case <-time.After(2 * time.Second):
		t.Fatal("online subscription workers did not start")
	}
	// Cleanups run in reverse, so this one inspects the scheduler while it is still live: every
	// subscription test doubles as a check that its own traffic left the refresh queue consistent.
	t.Cleanup(func() { checkOnlineInvariants(t, g.onlineSubs) })
}

func submitSubscriptions(t *testing.T, c *Client, revision int64, ids []string) subscriptionReply {
	t.Helper()
	data, err := json.Marshal(onlineSubscriptionRequest{Revision: revision, UserIds: ids})
	if err != nil {
		t.Fatal(err)
	}
	raw := c.gw.dispatch(c, Request{ReqId: ReqSetOnlineSubscriptions, OpId: "subscribe", Data: data})
	var reply subscriptionReply
	if err := json.Unmarshal(raw, &reply); err != nil {
		t.Fatalf("invalid subscription response %s: %v", raw, err)
	}
	if reply.ReqId != ReqSetOnlineSubscriptions {
		t.Fatalf("subscription response req_id=%d", reply.ReqId)
	}
	return reply
}

func nextOnlineSnapshot(t *testing.T, f *fakeConn) onlineSnapshot {
	t.Helper()
	select {
	case raw := <-f.out:
		return decodeOnlineSnapshot(t, raw)
	case <-time.After(2 * time.Second):
		t.Fatal("no online snapshot within 2s")
	}
	return onlineSnapshot{}
}

func decodeOnlineSnapshot(t *testing.T, raw []byte) onlineSnapshot {
	t.Helper()
	var frame struct {
		ReqId int            `json:"req_id"`
		Data  onlineSnapshot `json:"data"`
	}
	if err := json.Unmarshal(raw, &frame); err != nil {
		t.Fatalf("invalid online frame %s: %v", raw, err)
	}
	if frame.ReqId != OnlineChanged {
		t.Fatalf("want 2005, received %s", raw)
	}
	return frame.Data
}

func assertOnlineSnapshot(t *testing.T, got onlineSnapshot, revision int64, want []user.OnlineStatus) {
	t.Helper()
	if got.Revision != revision || got.Stale || len(got.Items) != len(want) {
		t.Fatalf("snapshot=%+v, want fresh revision=%d items=%+v", got, revision, want)
	}
	for _, item := range want {
		i := slices.IndexFunc(got.Items, func(got user.OnlineStatus) bool { return got.UserId == item.UserId })
		if i < 0 {
			t.Fatalf("snapshot omitted %s: %+v", item.UserId, got)
		}
		actual := got.Items[i]
		actualPlatforms := slices.Sorted(slices.Values(actual.Platforms))
		wantPlatforms := slices.Sorted(slices.Values(item.Platforms))
		if actual.Online != item.Online || !slices.Equal(actualPlatforms, wantPlatforms) {
			t.Fatalf("status=%+v, want %+v", actual, item)
		}
	}
}

func assertNoOnlineFrame(t *testing.T, f *fakeConn) {
	t.Helper()
	select {
	case raw := <-f.out:
		t.Fatalf("unexpected frame: %s", raw)
	default:
	}
}

// A finished read wakes the dispatcher itself, so Wait covers every hand-off of a refresh slot.
// Only the per-connection minimum read interval still holds admission back on the clock, so the
// loop advances that one interval at a time instead of polling a scheduler tick.
func waitOnlineReads(t *testing.T, online *subscriptionOnline, count int32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		synctest.Wait()
		if online.calls.Load() >= count {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("OnlineStore reads=%d, want at least %d", online.calls.Load(), count)
		}
		time.Sleep(onlineRefreshMinInterval)
	}
}

func TestOnlineSubscriptionsReplaceAndCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			return map[string][]int{"u___2": {1, 3}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		reply := submitSubscriptions(t, c, 1, []string{"u___3", "u___2", "u___2"})
		if reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		var ack struct {
			Revision           int64 `json:"revision"`
			SnapshotIntervalMs int64 `json:"snapshot_interval_ms"`
		}
		if err := json.Unmarshal(reply.Data, &ack); err != nil {
			t.Fatal(err)
		}
		if ack.Revision != 1 || ack.SnapshotIntervalMs != onlineSnapshotInterval.Milliseconds() {
			t.Fatalf("subscription ACK: %+v", ack)
		}
		want := []user.OnlineStatus{
			{UserId: "u___2", Online: true, Platforms: []int{1, 3}},
			{UserId: "u___3", Platforms: []int{}},
		}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, want)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2", "u___3"}); reply.Code != 0 {
			t.Fatalf("same set in another order is not an idempotent retry: %+v", reply)
		}
		if refs := g.Stats().OnlineSubscriptionRefs; refs != 2 {
			t.Fatalf("deduplication or retry double-counted subscription references: %d", refs)
		}
		synctest.Wait()
		for len(f.out) > 0 {
			assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, want)
		}
		if reply := submitSubscriptions(t, c, 2, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("replacement subscription: %+v", reply)
		}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 2, want[:1])
		synctest.Wait()
		before := online.calls.Load()
		if reply := submitSubscriptions(t, c, 3, []string{}); reply.Code != 0 {
			t.Fatalf("cancel subscription: %+v", reply)
		}
		if refs := g.Stats().OnlineSubscriptionRefs; refs != 0 {
			t.Fatalf("cancellation retained subscription references: %d", refs)
		}
		g.markOnlineChanged([]string{"u___2"})
		time.Sleep(2 * onlineSnapshotInterval)
		synctest.Wait()
		if got := online.calls.Load(); got != before {
			t.Fatalf("cancelled subscription still queried OnlineStore: before=%d after=%d", before, got)
		}
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsRejectWithoutReplacing(t *testing.T) {
	tooMany := make([]string, maxOnlineSubscriptions+1)
	for i := range tooMany {
		tooMany[i] = "u___2"
	}
	for _, tc := range []struct {
		name     string
		revision int64
		ids      []string
	}{
		{name: "zero revision", ids: []string{"u___3"}},
		{name: "old revision", revision: 1, ids: []string{"u___3"}},
		{name: "same revision different set", revision: 2, ids: []string{"u___3"}},
		{name: "invalid user", revision: 3, ids: []string{"u___02"}},
		{name: "raw count exceeds limit", revision: 3, ids: tooMany},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				online := &subscriptionOnline{}
				g := newSubscriptionGateway(t, online, nil)
				f := newFakeConn()
				c := serve(t, g, "u___1", f)
				if reply := submitSubscriptions(t, c, 2, []string{"u___2"}); reply.Code != 0 {
					t.Fatalf("initial subscription: %+v", reply)
				}
				want := []user.OnlineStatus{{UserId: "u___2", Platforms: []int{}}}
				assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 2, want)
				if reply := submitSubscriptions(t, c, tc.revision, tc.ids); reply.Code != errcode.ErrInvalidParam.Code {
					t.Fatalf("invalid subscription: %+v", reply)
				}
				g.markOnlineChanged([]string{"u___2"})
				assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 2, want)
			})
		})
	}
}

func TestOnlineSubscriptionsRequireOnlineDependencies(t *testing.T) {
	for _, missing := range []string{"online", "user"} {
		t.Run(missing, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				online := &subscriptionOnline{}
				g := newSubscriptionGateway(t, online, nil)
				switch missing {
				case "online":
					g.deps.Online = nil
				case "user":
					g.deps.User = nil
				}
				f := newFakeConn()
				c := serve(t, g, "u___1", f)
				if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != errcode.ErrInvalidProtocol.Code {
					t.Fatalf("missing %s dependency: %+v", missing, reply)
				}
				synctest.Wait()
				if online.calls.Load() != 0 {
					t.Fatal("unsupported subscriptions queried OnlineStore")
				}
				assertNoOnlineFrame(t, f)
			})
		})
	}
}

func TestOnlineSubscriptionsDiscardReplacedRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		release := make(chan struct{})
		releaseRead := sync.OnceFunc(func() { close(release) })
		defer releaseRead()
		var reads atomic.Int32
		online := &subscriptionOnline{read: func(ctx context.Context, ids []string) (map[string][]int, error) {
			if reads.Add(1) == 1 {
				started <- ctx
				// Return a successful old result even after cancellation, as a late driver result can.
				<-release
				return map[string][]int{"u___2": {1}}, nil
			}
			return map[string][]int{"u___3": {2}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		<-started
		if reply := submitSubscriptions(t, c, 2, []string{"u___3"}); reply.Code != 0 {
			t.Fatalf("replacement subscription: %+v", reply)
		}
		releaseRead()
		want := []user.OnlineStatus{{UserId: "u___3", Online: true, Platforms: []int{2}}}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 2, want)
		synctest.Wait()
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsReadFailureAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var failing atomic.Bool
		failing.Store(true)
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			if failing.Load() {
				return nil, errors.New("online store unavailable")
			}
			return map[string][]int{"u___2": {1}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("OnlineStore failure rejected the subscription: %+v", reply)
		}
		waitOnlineReads(t, online, 1)
		assertNoOnlineFrame(t, f)
		failing.Store(false)
		time.Sleep(onlineSnapshotInterval)
		want := []user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{1}}}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, want)
		synctest.Wait()
		failing.Store(true)
		time.Sleep(4 * onlineSnapshotInterval)
		synctest.Wait()
		if len(f.out) == 0 {
			t.Fatal("continued read failures never marked the old snapshot stale")
		}
		for len(f.out) > 0 {
			if got := nextOnlineSnapshot(t, f); got.Revision != 1 || !got.Stale {
				t.Fatalf("read failure produced an authoritative snapshot: %+v", got)
			}
		}
		failing.Store(false)
		time.Sleep(onlineSnapshotInterval)
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, want)
	})
}

func TestOnlineSubscriptionsPeriodicReadsRecoverMissingEvents(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := onlinedb.New(storetest.NewMem(), time.Minute)
		g := newSubscriptionGateway(t, online, nil)
		remote := onlinestore.ConnRef{UserId: "u___2", PlatformId: 2, ConnId: "remote-connection"}
		if err := online.Add(t.Context(), "remote", remote); err != nil {
			t.Fatal(err)
		}
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{remote.UserId}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		on := []user.OnlineStatus{{UserId: remote.UserId, Online: true, Platforms: []int{2}}}
		off := []user.OnlineStatus{{UserId: remote.UserId, Platforms: []int{}}}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, on)
		// Mutate the shared store without publishing: the watcher must repair a missed event.
		if err := online.Remove(t.Context(), "remote", remote); err != nil {
			t.Fatal(err)
		}
		time.Sleep(onlineSnapshotInterval)
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, off)
		if err := online.Add(t.Context(), "remote", remote); err != nil {
			t.Fatal(err)
		}
		time.Sleep(onlineSnapshotInterval)
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, on)
		// No Remove or further Renew follows, as when the remote node disappears.
		time.Sleep(onlineSnapshotInterval)
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, off)
	})
}

func TestOnlineSubscriptionsCoalesceEventsBeforeReading(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, 1)
		release := make(chan struct{})
		releaseRead := sync.OnceFunc(func() { close(release) })
		defer releaseRead()
		var block atomic.Bool
		var platform atomic.Int32
		platform.Store(1)
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			observed := int(platform.Load())
			if block.Swap(false) {
				started <- struct{}{}
				<-release
			}
			return map[string][]int{"u___2": {observed}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		g.markOnlineChanged([]string{"u___2"})
		synctest.Wait()
		if online.calls.Load() != 0 {
			t.Fatal("an unwatched user's event caused a query")
		}
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{1}}})
		synctest.Wait()
		block.Store(true)
		g.markOnlineChanged([]string{"u___2"})
		<-started
		platform.Store(2)
		for range 100 {
			g.markOnlineChanged([]string{"u___2"})
		}
		synctest.Wait()
		if got := online.calls.Load(); got != 2 {
			t.Fatalf("events started concurrent reads for one connection: calls=%d", got)
		}
		releaseRead()
		time.Sleep(onlineRefreshMinInterval)
		waitOnlineReads(t, online, 3)
		if got := online.calls.Load(); got != 3 {
			t.Fatalf("event burst was not coalesced into one follow-up read: calls=%d", got)
		}
		var last onlineSnapshot
		for len(f.out) > 0 {
			last = nextOnlineSnapshot(t, f)
		}
		assertOnlineSnapshot(t, last, 1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{2}}})
	})
}

func TestOnlineSubscriptionsBudgetDoesNotResync(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.Limits.WsSendBytesTotal = 1
		online := &subscriptionOnline{}
		g := newSubscriptionGateway(t, online, cfg)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("subscription ACK failed under push budget pressure: %+v", reply)
		}
		waitOnlineReads(t, online, 1)
		if stats := g.Stats(); stats.Dropped != 1 || stats.OnlinePushDropped != 1 {
			t.Fatalf("presence budget drop was not counted as a presence drop: %+v", stats)
		}
		assertNoOnlineFrame(t, f)
		if f.isClosed() {
			t.Fatal("a presence budget drop closed the connection")
		}
		time.Sleep(onlineSnapshotInterval)
		waitOnlineReads(t, online, 2)
		if online.calls.Load() != 2 {
			t.Fatal("budget pressure removed the subscription instead of retrying next period")
		}
		// The next period repairs the gap on its own; a dropped snapshot never asks for a resync.
		if stats := g.Stats(); stats.OnlinePushDropped != 2 {
			t.Fatalf("the repeat drop was not counted: %+v", stats)
		}
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsDisconnectStopsRefresh(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := &subscriptionOnline{}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		nextOnlineSnapshot(t, f)
		synctest.Wait()
		before := online.calls.Load()
		c.Close("test")
		if refs := g.Stats().OnlineSubscriptionRefs; refs != 0 {
			t.Fatalf("disconnect retained subscription references: %d", refs)
		}
		g.markOnlineChanged([]string{"u___2"})
		time.Sleep(2 * onlineSnapshotInterval)
		synctest.Wait()
		if got := online.calls.Load(); got != before {
			t.Fatalf("closed connection kept its subscription: before=%d after=%d", before, got)
		}
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsPublishLocallyWithoutBus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var platform atomic.Int32
		platform.Store(1)
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			return map[string][]int{"u___2": {int(platform.Load())}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		nextOnlineSnapshot(t, f)
		platform.Store(2)
		g.publishOnlineChanged(t.Context(), []string{"u___2"})
		assertOnlineSnapshot(
			t,
			nextOnlineSnapshot(t, f),
			1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{2}}},
		)
	})
}

func TestOnlineSubscriptionsDisconnectCancelsRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
			started <- ctx
			<-ctx.Done()
			return nil, ctx.Err()
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		reading := <-started
		c.Close("test")
		synctest.Wait()
		if !errors.Is(reading.Err(), context.Canceled) {
			t.Fatalf("disconnect did not cancel the active read: %v", reading.Err())
		}
		g.markOnlineChanged([]string{"u___2"})
		time.Sleep(onlineSnapshotInterval)
		synctest.Wait()
		if online.calls.Load() != 1 || g.Stats().OnlineSubscriptionRefs != 0 {
			t.Fatal("disconnect left active subscription work")
		}
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsNodeLimitPreservesAndReleasesSets(t *testing.T) {
	online := &subscriptionOnline{}
	users := user.New(storetest.NewMem(), nil, online)
	g := New(testConfig(), Deps{Online: online, User: users})
	t.Cleanup(func() { g.cancelRun(); g.cancel(); g.work.cancelOps(); g.work.wait() })
	ids := make([]string, maxOnlineSubscriptions)
	for i := range ids {
		ids[i] = "u___" + strconv.Itoa(i+100)
	}
	newClient := func(id string) *Client {
		return g.newClient(auth.Identity{UserId: "u___1"}, id, "", newFakeConn())
	}
	// Leave 100 slots for a 99-user existing set and one subscription that can be cancelled.
	for i := range maxOnlineSubscriptionRefs/maxOnlineSubscriptions - 1 {
		c := newClient("capacity-" + strconv.Itoa(i))
		if reply := submitSubscriptions(t, c, 1, ids); reply.Code != 0 {
			t.Fatalf("filling node capacity at connection %d: %+v", i, reply)
		}
	}
	existing, filler, waiting := newClient("existing"), newClient("filler"), newClient("waiting")
	if reply := submitSubscriptions(t, existing, 1, ids[:len(ids)-1]); reply.Code != 0 {
		t.Fatalf("existing set: %+v", reply)
	}
	if reply := submitSubscriptions(t, filler, 1, ids[:1]); reply.Code != 0 {
		t.Fatalf("filling last reference: %+v", reply)
	}
	if got := g.Stats().OnlineSubscriptionRefs; got != maxOnlineSubscriptionRefs {
		t.Fatalf("subscription reference accounting=%d", got)
	}
	if reply := submitSubscriptions(t, waiting, 1, ids[:1]); reply.Code != errcode.ErrTooManyRequests.Code {
		t.Fatalf("over-capacity first subscription: %+v", reply)
	}
	if reply := submitSubscriptions(t, existing, 2, ids); reply.Code != errcode.ErrTooManyRequests.Code {
		t.Fatalf("over-capacity replacement: %+v", reply)
	}
	if reply := submitSubscriptions(t, existing, 1, ids[:len(ids)-1]); reply.Code != 0 {
		t.Fatalf("rejected replacement changed the confirmed set or revision: %+v", reply)
	}
	if reply := submitSubscriptions(t, filler, 2, []string{}); reply.Code != 0 {
		t.Fatalf("cancel last reference: %+v", reply)
	}
	if reply := submitSubscriptions(t, waiting, 1, ids[:1]); reply.Code != 0 {
		t.Fatalf("rejected first subscription could not retry after cancellation: %+v", reply)
	}
	waiting.Close("test")
	if reply := submitSubscriptions(t, existing, 2, ids); reply.Code != 0 {
		t.Fatalf("rejected replacement could not retry after disconnect: %+v", reply)
	}
	if got := g.Stats().OnlineSubscriptionRefs; got != maxOnlineSubscriptionRefs {
		t.Fatalf("capacity recovery lost or leaked references: %d", got)
	}
}

func TestOnlineSubscriptionsCrossNodesAndPlatforms(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := onlinedb.New(storetest.NewMem(), time.Minute)
		users := user.New(storetest.NewMem(), nil, online)
		b := buslocal.New()
		node := func(id string) *Gateway {
			cfg := testConfig()
			cfg.NodeId = id
			cfg.Ws.PingInterval = time.Hour
			cfg.OnlineStore.RenewInterval = time.Hour
			g := New(cfg, Deps{Online: online, User: users, Bus: b})
			startSubscriptionGateway(t, g)
			return g
		}
		n1, n2 := node("n1"), node("n2")
		f := newFakeConn()
		watcher := serve(t, n1, "u___1", f)
		if reply := submitSubscriptions(t, watcher, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		off := []user.OnlineStatus{{UserId: "u___2", Platforms: []int{}}}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, off)
		phone, _ := serveAs(t, n2, auth.Identity{UserId: "u___2", PlatformId: 1}, "phone@n2")
		n2.onlineAdd(phone)
		assertOnlineSnapshot(
			t,
			nextOnlineSnapshot(t, f),
			1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{1}}},
		)
		desktop, _ := serveAs(t, n1, auth.Identity{UserId: "u___2", PlatformId: 2}, "desktop@n1")
		n1.onlineAdd(desktop)
		assertOnlineSnapshot(
			t,
			nextOnlineSnapshot(t, f),
			1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{1, 2}}},
		)
		phone.Close("test")
		assertOnlineSnapshot(
			t,
			nextOnlineSnapshot(t, f),
			1,
			[]user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{2}}},
		)
		desktop.Close("test")
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, off)
	})
}
