package gateway

import (
	"context"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	hertzerrs "github.com/cloudwego/hertz/pkg/common/errors"
	"github.com/cloudwego/hertz/pkg/common/test/mock"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/cloudwego/hertz/pkg/protocol/http1"
	gws "github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
)

var errUpgradeWrite = errors.New("injected upgrade write failure")

type upgradeTestConn struct {
	*mock.Conn
	failAt  string
	entered chan struct{}
	release <-chan struct{}
	closed  atomic.Int32
}

func (c *upgradeTestConn) Writer() network.Writer { return c }

func (c *upgradeTestConn) WriteBinary(raw []byte) (int, error) {
	if c.failAt == "header" {
		return 0, errUpgradeWrite
	}
	return c.Conn.WriteBinary(raw)
}

func (c *upgradeTestConn) Flush() error {
	if c.entered != nil {
		close(c.entered)
		<-c.release
	}
	if c.failAt == "flush" {
		return errUpgradeWrite
	}
	return c.Conn.Flush()
}

func (c *upgradeTestConn) SetReadTimeout(d time.Duration) error {
	if c.failAt == "hijack-timeout" && d == 0 {
		return errUpgradeWrite
	}
	return c.Conn.SetReadTimeout(d)
}

func (c *upgradeTestConn) Close() error {
	c.closed.Add(1)
	return c.Conn.Close()
}

func newUpgradeHttpServer(t *testing.T, g *Gateway) *http1.Server {
	t.Helper()
	h := server.New()
	h.GET("/ws", g.Handle)
	return &http1.Server{
		Core:             h.Engine,
		NoDefaultDate:    true, // no process-wide date updater inside a synctest bubble
		HijackConnHandle: h.Engine.HijackConnHandle,
	}
}

func upgradeRequest(jwt, connection string) string {
	return fmt.Sprintf("GET /ws?platform_id=1&token=%s HTTP/1.1\r\nHost: localhost\r\nConnection: %s\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n", jwt, connection)
}

func assertNoUpgradeSlots(t *testing.T, g *Gateway) {
	t.Helper()
	m := g.users
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.total != 0 || len(m.byUser) != 0 || len(m.byUserN) != 0 || len(m.byToken) != 0 || len(m.byIp) != 0 {
		t.Fatalf("leaked connection slots: total=%d adopted=%d user=%v token=%v ip=%v", m.total, len(m.byUser), m.byUserN, m.byToken, m.byIp)
	}
}

// Upgrade returns nil before Hertz writes HTTP 101. All failures before its callback still
// belong to HTTP, so they must return the reservation even though no Client was created.
func TestUpgradeFailureReleasesSlots(t *testing.T) {
	for _, tc := range []struct {
		name       string
		connection string
		failAt     string
	}{
		{name: "protocol-rejected", connection: "close"},
		{name: "header", connection: "Upgrade", failAt: "header"},
		{name: "flush", connection: "Upgrade", failAt: "flush"},
		{name: "hijack-timeout", connection: "Upgrade", failAt: "hijack-timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := newGateway(t, testConfig())
			t.Cleanup(g.cancel)
			t.Cleanup(g.cancelRun)
			s := newUpgradeHttpServer(t, g)
			for range 3 {
				conn := &upgradeTestConn{Conn: mock.NewConn(upgradeRequest(token(1), tc.connection)), failAt: tc.failAt}
				err := s.Serve(t.Context(), conn)
				if tc.failAt != "" && !errors.Is(err, errUpgradeWrite) {
					t.Fatalf("Serve error = %v, want injected upgrade failure", err)
				}
				if tc.failAt == "" && !errors.Is(err, hertzerrs.ErrShortConnection) {
					t.Fatalf("rejected HTTP upgrade: %v", err)
				}
				// Request completion normally returns the slot; the write budget also covers
				// hosts that disable RequestContext resets. Synchronous rejection is immediate.
				g.work.wait()
				assertNoUpgradeSlots(t, g)
			}
		})
	}
}

func TestUpgradeShutdownBeforeHijack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		s := newUpgradeHttpServer(t, g)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		conn := &upgradeTestConn{
			Conn:    mock.NewConn(upgradeRequest(token(1), "Upgrade")),
			entered: make(chan struct{}), release: release,
		}
		served := make(chan error, 1)
		go func() { served <- s.Serve(t.Context(), conn) }()
		<-conn.entered
		if g.users.Count() != 1 || len(g.users.All()) != 0 {
			t.Fatalf("handshake did not pause before adoption: %+v", g.Stats())
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		if err := g.Shutdown(ctx); err != nil {
			t.Fatalf("shutdown waited for the unadopted HTTP connection: %v", err)
		}
		assertNoUpgradeSlots(t, g)
		// HTTP may finish writing after the gateway stopped. Its delayed callback must close
		// the socket without adopting or returning the already-released slot a second time.
		unblock()
		if err := <-served; !errors.Is(err, hertzerrs.ErrHijacked) {
			t.Fatalf("late HTTP completion: %v", err)
		}
		if conn.closed.Load() == 0 {
			t.Fatal("late upgrade callback left its socket open")
		}
		assertNoUpgradeSlots(t, g)
	})
}

func TestUpgradeClaimPreservesAdoptedSlot(t *testing.T) {
	g := newGateway(t, testConfig())
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	c := g.newClient(auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "current"}, "adopted", "127.0.0.1", newFakeConn())
	t.Cleanup(func() { c.Close("test") })
	if err := g.users.Reserve(c.slot()); err != nil {
		t.Fatal(err)
	}
	finished := make(chan struct{})
	reservation := g.watchUpgrade(finished, c.slot())
	if !reservation.claim() {
		t.Fatal("live callback could not claim the reservation")
	}
	if err := g.users.Adopt(c); err != nil {
		t.Fatal(err)
	}
	g.work.wait() // successful handoff stops its watcher while the socket remains live
	close(finished)
	reservation.release()
	if g.users.Count() != 1 || len(g.users.All()) != 1 {
		t.Fatalf("HTTP completion released an adopted connection: %+v", g.Stats())
	}
	c.Close("test")
	assertNoUpgradeSlots(t, g)
}

func TestUpgradeClaimRacesRelease(t *testing.T) {
	g := newGateway(t, testConfig())
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	for range 100 {
		slot := Slot{UserId: "u___1", TokenId: "current", Ip: "127.0.0.1"}
		if err := g.users.Reserve(slot); err != nil {
			t.Fatal(err)
		}
		finished := make(chan struct{})
		reservation := g.watchUpgrade(finished, slot)
		start := make(chan struct{})
		var workers sync.WaitGroup
		workers.Go(func() {
			<-start
			if reservation.claim() {
				g.users.Release(slot)
			}
		})
		workers.Go(func() { <-start; close(finished) })
		workers.Go(func() { <-start; reservation.release() })
		close(start)
		workers.Wait()
		g.work.wait()
		assertNoUpgradeSlots(t, g)
	}
}

func TestUpgradeSealedWorkReleasesSlot(t *testing.T) {
	g := newGateway(t, testConfig())
	t.Cleanup(g.cancel)
	t.Cleanup(g.cancelRun)
	slot := Slot{UserId: "u___1", TokenId: "current", Ip: "127.0.0.1"}
	if err := g.users.Reserve(slot); err != nil {
		t.Fatal(err)
	}
	g.work.seal()
	r := g.watchUpgrade(make(chan struct{}), slot)
	if r.claim() {
		t.Fatal("untracked upgrade could still claim its slot")
	}
	g.work.wait()
	assertNoUpgradeSlots(t, g)
}

func TestUpgradeReservationExpiresWithoutFinished(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		defer g.cancel()
		defer g.cancelRun()
		slot := Slot{UserId: "u___1", TokenId: "current", Ip: "127.0.0.1"}
		if err := g.users.Reserve(slot); err != nil {
			t.Fatal(err)
		}
		r := g.watchUpgrade(make(chan struct{}), slot)
		synctest.Wait()
		time.Sleep(writeWait - time.Nanosecond)
		if got := g.users.Count(); got != 1 {
			t.Fatalf("reservation released before its budget: %d", got)
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		assertNoUpgradeSlots(t, g)
		if r.claim() {
			t.Fatal("late callback claimed an expired reservation")
		}
		g.work.wait()
	})
}

func TestUpgradeExiledWriteFailureReleasesSlot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		defer g.cancel()
		defer g.cancelRun()
		h := server.New()
		var finished <-chan struct{}
		h.GET("/ws", func(ctx context.Context, c *app.RequestContext) {
			c.Exile()
			finished = c.Finished()
			g.Handle(ctx, c)
		})
		s := &http1.Server{
			Core:             h.Engine,
			NoDefaultDate:    true, // no process-wide date updater inside the synctest bubble
			HijackConnHandle: h.Engine.HijackConnHandle,
		}
		conn := &upgradeTestConn{Conn: mock.NewConn(upgradeRequest(token(1), "Upgrade")), failAt: "header"}
		if err := s.Serve(t.Context(), conn); !errors.Is(err, errUpgradeWrite) {
			t.Fatalf("Serve error = %v, want injected write failure", err)
		}
		select {
		case <-finished:
			t.Fatal("exiled context unexpectedly signaled completion")
		default:
		}
		time.Sleep(writeWait)
		g.work.wait()
		assertNoUpgradeSlots(t, g)
	})
}

func TestUpgradeCallbackAfterBudgetClosesSocket(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		defer g.cancel()
		defer g.cancelRun()
		s := newUpgradeHttpServer(t, g)
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		conn := &upgradeTestConn{
			Conn:    mock.NewConn(upgradeRequest(token(1), "Upgrade")),
			entered: make(chan struct{}), release: release,
		}
		served := make(chan error, 1)
		go func() { served <- s.Serve(t.Context(), conn) }()
		<-conn.entered
		time.Sleep(writeWait)
		g.work.wait()
		assertNoUpgradeSlots(t, g)
		unblock()
		if err := <-served; !errors.Is(err, hertzerrs.ErrHijacked) {
			t.Fatalf("late HTTP completion: %v", err)
		}
		if conn.closed.Load() == 0 {
			t.Fatal("late callback left its socket open")
		}
		assertNoUpgradeSlots(t, g)
	})
}

func TestUpgradeClaimEndsBudgetWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := newGateway(t, testConfig())
		defer g.cancel()
		defer g.cancelRun()
		c := g.newClient(
			auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "current"},
			"adopted",
			"127.0.0.1",
			newFakeConn(),
		)
		defer c.Close("test")
		if err := g.users.Reserve(c.slot()); err != nil {
			t.Fatal(err)
		}
		r := g.watchUpgrade(make(chan struct{}), c.slot())
		if !r.claim() {
			t.Fatal("live callback could not claim the reservation")
		}
		if err := g.users.Adopt(c); err != nil {
			t.Fatal(err)
		}
		claimedAt := time.Now()
		g.work.wait()
		if time.Since(claimedAt) != 0 {
			t.Fatal("claimed reservation waited for its write budget")
		}
		time.Sleep(2 * writeWait)
		if g.users.Count() != 1 || len(g.users.All()) != 1 {
			t.Fatalf("expired budget released an adopted connection: %+v", g.Stats())
		}
	})
}

// Shutdown cancels runCtx at its very start and seals the work group only after its drain poll, both
// later than handshake's draining check and either side of the UserMap.Close that fails Reserve, so a
// request can still find capacity and yet have no watcher left to hold its reservation. The client
// must get the documented 503 in both windows: HTTP 101 leaves the callback nothing but a close
// without a close frame, which reads as 1006 (design §7.3).
func TestUpgradeDrainWindowAnswersServiceUnavailable(t *testing.T) {
	for name, drain := range map[string]func(*Gateway){
		"sealed work group": func(g *Gateway) { g.work.seal() },
		"cancelled run":     func(g *Gateway) { g.cancelRun() },
	} {
		t.Run(name, func(t *testing.T) {
			g := newGateway(t, testConfig())
			t.Cleanup(g.cancel)
			t.Cleanup(g.cancelRun)
			url := startServer(t, g)
			drain(g)
			conn, resp, err := gws.DefaultDialer.Dial(url+"?platform_id=1&token="+token(1), nil)
			if err == nil {
				conn.Close()
				t.Fatal("a draining node completed the WebSocket handshake")
			}
			if resp.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status %d (%v), want 503 without an upgrade", resp.StatusCode, err)
			}
			defer resp.Body.Close()
			raw, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			var body struct {
				Code int `json:"code"`
			}
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Fatalf("draining response %q: %v", raw, err)
			}
			if body.Code != errcode.ErrNodeDraining.Code {
				t.Fatalf("code %d, want %d so the client can tell draining from an overload", body.Code, errcode.ErrNodeDraining.Code)
			}
			assertNoUpgradeSlots(t, g)
		})
	}
}
