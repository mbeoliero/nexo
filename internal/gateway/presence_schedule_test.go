package gateway

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/service/user"
)

func watchedUser(i int) string { return "u___" + strconv.Itoa(1000+i) }

// newSubscribers builds one subscribing connection per watched user without a reader or a writer:
// these tests count reads and queue moves, and the node connection limits are not what is under
// test, so the clients stay out of the user map and out of the goroutine bookkeeping.
func newSubscribers(t *testing.T, g *Gateway, n int) []*Client {
	t.Helper()
	clients := make([]*Client, n)
	for i := range clients {
		clients[i] = g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1}, "sub-"+strconv.Itoa(i), "", newFakeConn())
		if reply := submitSubscriptions(t, clients[i], 1, []string{watchedUser(i)}); reply.Code != 0 {
			t.Fatalf("subscriber %d rejected: %+v", i, reply)
		}
	}
	return clients
}

// checkOnlineInvariants asserts the refresh queue's structural rules under its own lock. They are
// the ones the scheduler relies on instead of the per-subscription flags it used to carry: a
// subscription is queued or reading but never both, holds at most one pending mark, and appears in
// the reverse index exactly as often as it is counted in the node's reference total (design §7.4).
func checkOnlineInvariants(t *testing.T, p *onlineSubscriptions) {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.reads < 0 || p.reads > onlineRefreshWorkers {
		t.Errorf("refresh slots in use=%d, want 0..%d", p.reads, onlineRefreshWorkers)
	}
	refs, reading := 0, 0
	for c, s := range p.active {
		refs += len(s.userIds)
		if s.client != c || c.onlineSub != s {
			t.Errorf("connection %s is not the owner of its own subscription", c.Id)
		}
		if s.index < 0 && s.cancel == nil {
			t.Errorf("connection %s is subscribed but neither queued nor reading", c.Id)
		}
		if s.index >= 0 && s.cancel != nil {
			t.Errorf("connection %s is queued while it reads", c.Id)
		}
		if s.remark && s.cancel == nil {
			t.Errorf("connection %s holds a pending mark without a read to carry it", c.Id)
		}
		if s.cancel != nil {
			reading++
		}
		for _, id := range s.userIds {
			if _, ok := p.byUser[id][c]; !ok {
				t.Errorf("connection %s watches %s without a reverse index entry", c.Id, id)
			}
		}
	}
	if refs != p.refs {
		t.Errorf("reference count=%d, want %d", p.refs, refs)
	}
	if reading > p.reads {
		t.Errorf("%d reads in flight occupy %d refresh slots", reading, p.reads)
	}
	for id, clients := range p.byUser {
		if len(clients) == 0 {
			t.Errorf("reverse index kept an empty entry for %s", id)
		}
		for c := range clients {
			if s, ok := p.active[c]; !ok || !slices.Contains(s.userIds, id) {
				t.Errorf("reverse index maps %s to a connection that does not watch it", id)
			}
		}
	}
	for i, s := range p.due {
		if s.index != i {
			t.Errorf("queue slot %d holds a subscription that believes it sits at %d", i, s.index)
		}
		if _, ok := p.active[s.client]; !ok {
			t.Errorf("queue slot %d holds a subscription that is no longer active", i)
		}
		if parent := (i - 1) / 2; i > 0 && p.due.Less(i, parent) {
			t.Errorf("queue slot %d is due before its parent %d", i, parent)
		}
	}
}

// The write rate is admission control, not a filter on finished work: a presence storm has to slow
// the OnlineStore reads down rather than pay for them and discard the results (design §7.4).
func TestOnlineSubscriptionsRateLimitDefersReadsInsteadOfDroppingFrames(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const subscribers = 3 * onlineFramesPerSecond
		var mu sync.Mutex
		reads := make(map[string]int, subscribers)
		online := &subscriptionOnline{read: func(_ context.Context, ids []string) (map[string][]int, error) {
			mu.Lock()
			reads[ids[0]]++
			mu.Unlock()
			return map[string][]int{}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		newSubscribers(t, g, subscribers)
		synctest.Wait()
		// No clock time has passed, so the limiter can only have released its initial burst.
		if got := online.calls.Load(); got != onlineFramesPerSecond {
			t.Fatalf("reads before the clock moved=%d, want the %d token burst", got, onlineFramesPerSecond)
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := online.calls.Load(); got != subscribers {
			t.Fatalf("reads=%d, want every one of the %d subscribers refreshed", got, subscribers)
		}
		if stats := g.Stats(); stats.OnlinePushDropped != 0 || stats.Dropped != 0 {
			t.Fatalf("throttling discarded finished reads instead of deferring them: %+v", stats)
		}
		mu.Lock()
		defer mu.Unlock()
		for i := range subscribers {
			if n := reads[watchedUser(i)]; n != 1 {
				t.Fatalf("subscriber %d was read %d times, want one refresh each", i, n)
			}
		}
	})
}

// Refresh slots are handed out in mark order, so no connection can be refreshed twice while another
// still waits for its first snapshot, and an event storm cannot move a connection forward (§7.4).
func TestOnlineSubscriptionsRefreshAdmissionIsFairAcrossConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const batches = 3
		const subscribers = batches * onlineRefreshWorkers
		started := make(chan struct{}, subscribers)
		release := make(chan struct{})
		releaseReads := sync.OnceFunc(func() { close(release) })
		defer releaseReads()
		var mu sync.Mutex
		var admitted []string
		online := &subscriptionOnline{read: func(ctx context.Context, ids []string) (map[string][]int, error) {
			mu.Lock()
			admitted = append(admitted, ids[0])
			mu.Unlock()
			started <- struct{}{}
			select {
			case <-release:
				return map[string][]int{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		order := func() []string {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(admitted)
		}
		g := newSubscriptionGateway(t, online, nil)
		newSubscribers(t, g, subscribers)
		watching := make([]string, subscribers)
		for i := range watching {
			watching[i] = watchedUser(i)
		}
		for batch := range batches {
			for range onlineRefreshWorkers {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatalf("batch %d did not fill the refresh slots", batch)
				}
			}
			synctest.Wait()
			if batch == 0 {
				// Marks arriving while these reads run must not put them back at the head of the
				// queue ahead of the connections still waiting for their first refresh.
				for range 50 {
					g.markOnlineChanged(watching)
				}
				synctest.Wait()
			}
			want := slices.Sorted(slices.Values(watching[batch*onlineRefreshWorkers : (batch+1)*onlineRefreshWorkers]))
			got := slices.Sorted(slices.Values(order()[batch*onlineRefreshWorkers:]))
			if !slices.Equal(got, want) {
				t.Fatalf("batch %d refreshed %v, want the connections marked first %v", batch, got, want)
			}
			if batch < batches-1 {
				for range onlineRefreshWorkers {
					release <- struct{}{}
				}
			}
		}
		releaseReads()
		time.Sleep(onlineRefreshMinInterval)
		synctest.Wait()
		// The storm is worth exactly one extra read for each connection that was reading when it
		// arrived, and none at all for the connections that were still queued.
		if got := online.calls.Load(); got != subscribers+onlineRefreshWorkers {
			t.Fatalf("event storm produced %d reads, want %d", got, subscribers+onlineRefreshWorkers)
		}
		want := slices.Sorted(slices.Values(watching[:onlineRefreshWorkers]))
		if got := slices.Sorted(slices.Values(order()[subscribers:])); !slices.Equal(got, want) {
			t.Fatalf("the storm refreshed %v, want the connections it interrupted %v", got, want)
		}
	})
}

// A node with nothing subscribed must do no work at all, and one subscription must cost a constant
// number of dispatcher passes per period rather than one per tick of a fixed scheduler interval.
func TestOnlineSubscriptionsIdleSchedulerDoesNotWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := &subscriptionOnline{}
		g := newSubscriptionGateway(t, online, nil)
		p := g.onlineSubs
		time.Sleep(30 * time.Minute)
		synctest.Wait()
		if got := p.wakes.Load(); got != 0 {
			t.Fatalf("the scheduler woke %d times with nothing subscribed", got)
		}
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		nextOnlineSnapshot(t, f)
		synctest.Wait()
		const periods = 10
		before := p.wakes.Load()
		time.Sleep(periods * onlineSnapshotInterval)
		synctest.Wait()
		// One refresh needs a pass to admit the read and a pass to queue the next one.
		if got := p.wakes.Load() - before; got > 3*periods {
			t.Fatalf("%d dispatcher passes over %d snapshot periods, want a constant number each", got, periods)
		}
		if got := online.calls.Load(); got < periods {
			t.Fatalf("reads=%d over %d periods, want one refresh per period", got, periods)
		}
	})
}

// Marking every connection on the node is one push per connection, not a walk of the whole queue
// under the lock: the dispatcher only ever inspects the head, so a bulk mark neither multiplies
// dispatcher passes nor blocks the connections that subscribe or disconnect while it runs.
func TestOnlineSubscriptionsBulkMarkDispatchesWithoutScanning(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const subscribers = 10000
		const crowded = "u___99"
		started := make(chan struct{}, onlineRefreshWorkers)
		release := make(chan struct{})
		releaseReads := sync.OnceFunc(func() { close(release) })
		defer releaseReads()
		var active atomic.Int32
		online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
			if n := active.Add(1); n > onlineRefreshWorkers {
				t.Errorf("%d concurrent reads, want at most %d", n, onlineRefreshWorkers)
			}
			defer active.Add(-1)
			select { // only the reads that fill the slots are awaited; the drain must not block here
			case started <- struct{}{}:
			default:
			}
			select {
			case <-release:
				return map[string][]int{}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}}
		g := newSubscriptionGateway(t, online, nil)
		p := g.onlineSubs
		clients := make([]*Client, subscribers)
		for i := range clients {
			clients[i] = g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1}, "sub-"+strconv.Itoa(i), "", newFakeConn())
			if reply := submitSubscriptions(t, clients[i], 1, []string{crowded}); reply.Code != 0 {
				t.Fatalf("subscriber %d rejected: %+v", i, reply)
			}
		}
		for range onlineRefreshWorkers {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("the refresh slots did not start reading")
			}
		}
		synctest.Wait()
		const marks = 10
		before := p.wakes.Load()
		for range marks {
			g.markOnlineChanged([]string{crowded})
		}
		synctest.Wait()
		if got := p.wakes.Load() - before; got > marks {
			t.Fatalf("%d marks of %d subscriptions cost %d dispatcher passes", marks, subscribers, got)
		}
		if got := online.calls.Load(); got != onlineRefreshWorkers {
			t.Fatalf("reads=%d while every refresh slot was busy, want %d", got, onlineRefreshWorkers)
		}
		// Subscribing and disconnecting only ever wait for other memory work, never for a queue scan.
		joining := g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1}, "joining", "", newFakeConn())
		if reply := submitSubscriptions(t, joining, 1, []string{crowded}); reply.Code != 0 {
			t.Fatalf("subscribing under a bulk mark: %+v", reply)
		}
		g.removeOnlineSubscriptions(clients[0])
		if reply := submitSubscriptions(t, clients[1], 2, []string{}); reply.Code != 0 {
			t.Fatalf("cancelling under a bulk mark: %+v", reply)
		}
		if refs := g.Stats().OnlineSubscriptionRefs; refs != subscribers-1 {
			t.Fatalf("subscription references=%d, want %d", refs, subscribers-1)
		}
		checkOnlineInvariants(t, p)
		releaseReads()
		synctest.Wait()
		if got := online.calls.Load(); got <= onlineRefreshWorkers {
			t.Fatalf("reads=%d after the slots were freed, want the queue to drain", got)
		}
	})
}

// The refresh timer replaced a fixed ticker, so every period must still produce exactly one read
// per connection, including the period whose read fails.
func TestOnlineSubscriptionsRefreshesWithinOnePeriod(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const failing = 5
		var reads atomic.Int32
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			if reads.Add(1) == failing {
				return nil, errors.New("online store unavailable")
			}
			return map[string][]int{"u___2": {1}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		want := []user.OnlineStatus{{UserId: "u___2", Online: true, Platforms: []int{1}}}
		assertOnlineSnapshot(t, nextOnlineSnapshot(t, f), 1, want)
		const periods = 10
		for period := range periods {
			time.Sleep(onlineSnapshotInterval)
			synctest.Wait()
			if got, want := online.calls.Load(), int32(period)+2; got != want {
				t.Fatalf("reads=%d after %d periods, want %d", got, period+1, want)
			}
		}
		if stats := g.Stats(); stats.OnlineReadFails != 1 || stats.OnlineRefreshes != periods+1 {
			t.Fatalf("a failed period disturbed the schedule: %+v", stats)
		}
		var last onlineSnapshot
		for len(f.out) > 0 {
			last = nextOnlineSnapshot(t, f)
		}
		assertOnlineSnapshot(t, last, 1, want)
	})
}

// Events cannot outpace the per-connection minimum read interval, however many arrive (§7.4).
func TestOnlineSubscriptionsMinIntervalSurvivesEventStorm(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var mu sync.Mutex
		var at []time.Time
		online := &subscriptionOnline{read: func(context.Context, []string) (map[string][]int, error) {
			mu.Lock()
			at = append(at, time.Now())
			mu.Unlock()
			return map[string][]int{"u___2": {1}}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(t, g, "u___1", f)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("initial subscription: %+v", reply)
		}
		nextOnlineSnapshot(t, f)
		synctest.Wait()
		const events = 50
		for range events {
			g.markOnlineChanged([]string{"u___2"})
			time.Sleep(10 * time.Millisecond)
		}
		synctest.Wait()
		if got := online.calls.Load(); got != 1 {
			t.Fatalf("%d events inside one minimum read interval produced %d reads", events, got)
		}
		time.Sleep(onlineRefreshMinInterval)
		synctest.Wait()
		if got := online.calls.Load(); got != 2 {
			t.Fatalf("the coalesced storm produced %d reads, want exactly one follow-up", got)
		}
		mu.Lock()
		defer mu.Unlock()
		if gap := at[1].Sub(at[0]); gap < onlineRefreshMinInterval {
			t.Fatalf("the follow-up read came %v after the previous one, want at least %v", gap, onlineRefreshMinInterval)
		}
	})
}

// Every transition the queue has, driven while reads are in flight: the structure has to hold after
// a replaced revision, a cancelled set, a disconnect and a bulk mark.
func TestOnlineSubscriptionsQueueInvariants(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const subscribers = 3 * onlineRefreshWorkers
		release := make(chan struct{})
		releaseReads := sync.OnceFunc(func() { close(release) })
		defer releaseReads()
		var blocking atomic.Bool
		blocking.Store(true)
		online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
			if blocking.Load() {
				select {
				case <-release:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}
			return map[string][]int{}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		p := g.onlineSubs
		clients := newSubscribers(t, g, subscribers)
		synctest.Wait()
		checkOnlineInvariants(t, p)
		for i, c := range clients {
			switch i % 4 {
			case 0:
				if reply := submitSubscriptions(t, c, 2, []string{watchedUser(i), watchedUser(i + 1)}); reply.Code != 0 {
					t.Fatalf("replacing the set of subscriber %d: %+v", i, reply)
				}
			case 1:
				if reply := submitSubscriptions(t, c, 2, []string{}); reply.Code != 0 {
					t.Fatalf("cancelling subscriber %d: %+v", i, reply)
				}
			case 2:
				g.removeOnlineSubscriptions(c)
			}
			g.markOnlineChanged([]string{watchedUser(i)})
			checkOnlineInvariants(t, p)
		}
		synctest.Wait()
		checkOnlineInvariants(t, p)
		blocking.Store(false)
		releaseReads()
		time.Sleep(2 * onlineSnapshotInterval)
		synctest.Wait()
		checkOnlineInvariants(t, p)
		// Three of every four connections kept a set: two watched users after a replacement and one
		// for the connection left alone.
		if refs, want := g.Stats().OnlineSubscriptionRefs, int64(subscribers/4*3); refs != want {
			t.Fatalf("subscription references=%d, want %d", refs, want)
		}
	})
}

// Refresh workers and the dispatcher belong to the gateway work group, so Shutdown waits for them
// and reports its own deadline rather than returning while a read is still running.
func TestOnlineSubscriptionsWorkersJoinShutdown(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan struct{}, onlineRefreshWorkers)
		release := make(chan struct{})
		releaseReads := sync.OnceFunc(func() { close(release) })
		defer releaseReads()
		online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
			started <- struct{}{}
			<-ctx.Done()
			<-release // a driver that keeps running after its context is cancelled
			return map[string][]int{}, nil
		}}
		g := newSubscriptionGateway(t, online, nil)
		p := g.onlineSubs
		newSubscribers(t, g, onlineRefreshWorkers+1)
		for range onlineRefreshWorkers {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("the refresh slots did not start reading")
			}
		}
		synctest.Wait()
		const budget = 200 * time.Millisecond
		ctx, cancel := context.WithTimeout(t.Context(), budget)
		defer cancel()
		start := time.Now()
		if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != budget {
			t.Fatalf("shutdown returned %v after %v, want it to wait for the refresh workers", err, time.Since(start))
		}
		releaseReads()
		done := make(chan struct{})
		go func() { g.work.wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("presence goroutines outlived shutdown")
		}
		synctest.Wait()
		p.mu.Lock()
		defer p.mu.Unlock()
		if !p.stopped || p.reads != 0 || p.refs != 0 || len(p.due) != 0 || len(p.active) != 0 {
			t.Fatalf("shutdown left scheduler state: stopped=%v reads=%d refs=%d queued=%d active=%d",
				p.stopped, p.reads, p.refs, len(p.due), len(p.active))
		}
	})
}
