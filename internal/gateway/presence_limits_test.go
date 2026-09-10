package gateway

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/internal/auth"
)

func TestOnlineSubscriptionsFullSendQueueReleasesResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.Ws.SendQueue = 1
		g := newSubscriptionGateway(t, &subscriptionOnline{}, cfg)
		f := newFakeConn()
		c := g.newClient(
			auth.Identity{UserId: "u___1", PlatformId: 1},
			"slow-subscriber",
			"",
			f,
		)
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close("test") })
		// Do not run a writer: the existing frame must still occupy the queue when presence arrives.
		if err := c.Send([]byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("subscription rejected before its snapshot: %+v", reply)
		}
		select {
		case <-c.closed:
		case <-time.After(time.Second):
			t.Fatal("a full presence send queue did not close the subscriber")
		}
		synctest.Wait()
		if !f.isClosed() {
			t.Fatal("a full presence send queue left the socket open")
		}
		stats := g.Stats()
		if stats.SlowConsumers != 1 || stats.Conns != 0 || stats.OnlineSubscriptionRefs != 0 {
			t.Fatalf("slow subscriber was not fully released: %+v", stats)
		}
		if queued := g.budget.queued.Load(); queued != 0 {
			t.Fatalf("closed subscriber retained %d queued bytes", queued)
		}
	})
}

func TestOnlineSubscriptionsBoundConcurrentReads(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const subscribers = onlineRefreshWorkers + 2
		// One watched user per subscriber, so a read identifies the connection that was admitted.
		watched := func(i int) string { return fmt.Sprintf("u___%d", 200+i) }
		started := make(chan struct{}, subscribers)
		release := make(chan struct{})
		releaseReads := sync.OnceFunc(func() { close(release) })
		defer releaseReads()
		var active atomic.Int32
		var mu sync.Mutex
		var admitted []string
		online := &subscriptionOnline{read: func(ctx context.Context, ids []string) (map[string][]int, error) {
			mu.Lock()
			admitted = append(admitted, ids[0])
			mu.Unlock()
			active.Add(1)
			defer active.Add(-1)
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
		sockets := make([]*fakeConn, subscribers)
		for i := range subscribers {
			sockets[i] = newFakeConn()
			c := serve(
				t,
				g,
				fmt.Sprintf("u___%d", i+1),
				sockets[i],
			)
			if reply := submitSubscriptions(t, c, 1, []string{watched(i)}); reply.Code != 0 {
				t.Fatalf("subscriber %d rejected: %+v", i, reply)
			}
		}
		for range onlineRefreshWorkers {
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("the available refresh slots did not start reading")
			}
		}
		synctest.Wait()
		if got := active.Load(); got != onlineRefreshWorkers {
			t.Fatalf("concurrent reads=%d, want %d", got, onlineRefreshWorkers)
		}
		if got := online.calls.Load(); got != onlineRefreshWorkers {
			t.Fatalf("queued subscribers started extra reads: calls=%d", got)
		}
		// Connections marked at the same instant are admitted in mark order, so the slots go to the
		// first onlineRefreshWorkers subscribers and never to one that asked later (design §7.4).
		want := make([]string, onlineRefreshWorkers)
		for i := range want {
			want[i] = watched(i)
		}
		slices.Sort(want)
		if got := slices.Sorted(slices.Values(order())); !slices.Equal(got, want) {
			t.Fatalf("refresh slots went to %v, want the connections marked first %v", got, want)
		}
		// Free exactly one slot. One queued subscriber should take it while the others remain blocked.
		release <- struct{}{}
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("a pending subscriber did not use the released refresh slot")
		}
		synctest.Wait()
		if got := active.Load(); got != onlineRefreshWorkers {
			t.Fatalf("slot replacement changed concurrency to %d", got)
		}
		if got := online.calls.Load(); got != onlineRefreshWorkers+1 {
			t.Fatalf("one released slot admitted %d total reads", got)
		}
		releaseReads()
		for _, socket := range sockets {
			got := nextOnlineSnapshot(t, socket)
			if got.Revision != 1 || got.Stale || len(got.Items) != 1 {
				t.Fatalf("queued subscriber did not eventually receive a snapshot: %+v", got)
			}
		}
		synctest.Wait()
		if active.Load() != 0 || online.calls.Load() != subscribers {
			t.Fatalf("read slots were not released: active=%d calls=%d", active.Load(), online.calls.Load())
		}
		if got := order()[onlineRefreshWorkers:]; !slices.Equal(got, []string{watched(onlineRefreshWorkers), watched(onlineRefreshWorkers + 1)}) {
			t.Fatalf("waiting subscribers were admitted as %v, want mark order", got)
		}
	})
}

func TestOnlineSubscriptionsReadHonorsTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		started := make(chan context.Context, 1)
		finished := make(chan error, 1)
		online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
			started <- ctx
			<-ctx.Done()
			finished <- ctx.Err()
			return nil, ctx.Err()
		}}
		g := newSubscriptionGateway(t, online, nil)
		f := newFakeConn()
		c := serve(
			t,
			g,
			"u___1",
			f,
		)
		if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
			t.Fatalf("subscription rejected before its first read: %+v", reply)
		}
		ctx := <-started
		deadline, ok := ctx.Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > connOpTimeout {
			t.Fatalf("read deadline=%v, present=%v, want an operation-bounded deadline", deadline, ok)
		}
		time.Sleep(time.Until(deadline) - time.Nanosecond)
		synctest.Wait()
		if err := ctx.Err(); err != nil {
			t.Fatalf("read was cancelled before its timeout: %v", err)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if err := <-finished; !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("read ended with %v, want deadline exceeded", err)
		}
		if stats := g.Stats(); stats.OnlineReadFails != 1 || stats.OnlineSubscriptionRefs != 1 {
			t.Fatalf("read timeout was not counted while retaining the subscription: %+v", stats)
		}
		if f.isClosed() {
			t.Fatal("an OnlineStore timeout closed the otherwise healthy WS connection")
		}
		assertNoOnlineFrame(t, f)
	})
}

func TestOnlineSubscriptionsShutdownBoundsRead(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lateReturn bool
	}{
		{name: "cooperative read"},
		{name: "read returns after shutdown deadline", lateReturn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				started := make(chan context.Context, 1)
				release := make(chan struct{})
				releaseRead := sync.OnceFunc(func() { close(release) })
				defer releaseRead()
				online := &subscriptionOnline{read: func(ctx context.Context, _ []string) (map[string][]int, error) {
					started <- ctx
					<-ctx.Done()
					if tc.lateReturn {
						<-release
						return map[string][]int{"u___2": {1}}, nil
					}
					return nil, ctx.Err()
				}}
				g := newSubscriptionGateway(t, online, nil)
				f := newFakeConn()
				c := serve(
					t,
					g,
					"u___1",
					f,
				)
				if reply := submitSubscriptions(t, c, 1, []string{"u___2"}); reply.Code != 0 {
					t.Fatalf("subscription rejected: %+v", reply)
				}
				readCtx := <-started
				const budget = 200 * time.Millisecond
				ctx, cancel := context.WithTimeout(t.Context(), budget)
				defer cancel()
				start := time.Now()
				err := g.Shutdown(ctx)
				if tc.lateReturn {
					if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != budget {
						t.Fatalf("shutdown did not honor its deadline: elapsed=%v err=%v", time.Since(start), err)
					}
				} else if err != nil || time.Since(start) >= budget {
					t.Fatalf("cooperative read delayed shutdown: elapsed=%v err=%v", time.Since(start), err)
				}
				if readCtx.Err() == nil || !f.isClosed() {
					t.Fatalf("shutdown left live work: read error=%v socket closed=%v", readCtx.Err(), f.isClosed())
				}
				releaseRead()
				synctest.Wait()
				if stats := g.Stats(); stats.OnlineSubscriptionRefs != 0 || stats.OnlineReadFails != 0 {
					t.Fatalf("shutdown cancellation leaked references or counted a dependency failure: %+v", stats)
				}
				assertNoOnlineFrame(t, f)
			})
		})
	}
}
