package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	gws "github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	onlineredis "github.com/mbeoliero/nexo/internal/onlinestore/redis"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

// blockingOnline stalls one presence call: it reports entered, optionally waits for the call's
// context, reports cancelled, then waits for release. A nil channel is skipped and an unblocked
// call falls through to fakeOnline.
type blockingOnline struct {
	fakeOnline
	blockRemove bool
	blockRenew  bool
	blockPurge  bool
	waitCtx     bool
	entered     chan struct{}
	cancelled   chan struct{}
	release     chan struct{}
}

func (f *blockingOnline) hold(ctx context.Context) error {
	signal(f.entered)
	if f.waitCtx {
		<-ctx.Done()
	}
	signal(f.cancelled)
	if f.release != nil {
		<-f.release
	}
	return ctx.Err()
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (f *blockingOnline) Remove(ctx context.Context, node string, c onlinestore.ConnRef) error {
	if !f.blockRemove {
		return f.fakeOnline.Remove(ctx, node, c)
	}
	return f.hold(ctx)
}

func (f *blockingOnline) Renew(ctx context.Context, node string, refs []onlinestore.ConnRef) error {
	if !f.blockRenew {
		return f.fakeOnline.Renew(ctx, node, refs)
	}
	return f.hold(ctx)
}

func (f *blockingOnline) PurgeNode(ctx context.Context, node string) error {
	if !f.blockPurge {
		return f.fakeOnline.PurgeNode(ctx, node)
	}
	return f.hold(ctx)
}

func TestShutdownClosesConnectionsAndRefusesNew(t *testing.T) {
	cfg := testConfig()
	cfg.NodeId = "n1"
	g := newGateway(t, cfg)
	fo := &fakeOnline{}
	g.deps.Online = fo
	url := startServer(t, g)
	conn, _, err := dialWs(url + "?platform_id=1&token=" + token(1))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	for range 50 {
		if len(g.users.Get("u___1")) == 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	_ = g.users.Get("u___1")[0].Send([]byte(`{"req_id":2001,"data":{}}`))

	done := make(chan error, 1)
	go func() { done <- g.Shutdown(t.Context()) }()

	// The queued frame arrives, then a 1001 close.
	if _, raw, err := conn.ReadMessage(); err != nil || string(raw) != `{"req_id":2001,"data":{}}` {
		t.Fatalf("queued frame: %s %v", raw, err)
	}
	_, _, err = conn.ReadMessage()
	if ce, ok := err.(*gws.CloseError); !ok || ce.Code != gws.CloseGoingAway {
		t.Fatalf("want close 1001, got %v", err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if g.users.Count() != 0 || len(fo.purged) != 1 {
		t.Fatalf("conns=%d purged=%v", g.users.Count(), fo.purged)
	}
	if _, st, err := dialWs(url + "?platform_id=1&token=" + token(1)); err == nil || st == http.StatusSwitchingProtocols {
		t.Fatalf("handshake during shutdown must fail: st=%d err=%v", st, err)
	}
}

func TestShutdownSharesDeadlineAcrossConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := &blockingOnline{blockRemove: true, waitCtx: true}
		g := New(testConfig(), Deps{Online: online})
		sockets := []*fakeConn{}
		for i := range 3 {
			f := newFakeConn()
			sockets = append(sockets, f)
			c := g.newClient(auth.Identity{UserId: fmt.Sprint(i)}, fmt.Sprint(i), "", f)
			if err := g.users.Register(c); err != nil {
				t.Fatal(err)
			}
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		_ = g.Shutdown(ctx)
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("shutdown took %v, budget 1s", elapsed)
		}
		for _, f := range sockets {
			if !f.isClosed() {
				t.Fatal("socket still open")
			}
		}
		// The budget bounds the drain; the purge still runs on a grace context, because leaving
		// this node's rows behind makes every other node suppress pushes until online_store.ttl.
		if len(online.purged) != 1 {
			t.Fatalf("purge must still run after the budget expired: %v", online.purged)
		}
	})
}

func TestShutdownWaitsForRenewToReturn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &blockingOnline{blockRenew: true, waitCtx: true,
			entered: make(chan struct{}, 1), cancelled: make(chan struct{}, 1), release: make(chan struct{})}
		cfg := testConfig()
		cfg.OnlineStore.RenewInterval = time.Second
		g := New(cfg, Deps{Online: f})
		runDone := make(chan struct{})
		go func() { _ = g.Run(t.Context()); close(runDone) }()
		<-f.entered
		done := make(chan struct{})
		go func() { _ = g.Shutdown(t.Context()); close(done) }()
		<-f.cancelled
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("shutdown returned while Renew still using dependencies")
		default:
		}
		close(f.release)
		<-done
		<-runDone
	})
}

type blockedControl struct {
	*fakeConn
	deadline chan time.Time
}

func (f *blockedControl) CloseControl(deadline time.Time) error {
	f.deadline <- deadline
	<-f.done
	return nil
}

func TestShutdownHardCloseDoesNotWaitForControl(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(testConfig(), Deps{})
		f := &blockedControl{fakeConn: newFakeConn(), deadline: make(chan time.Time, 1)}
		serve(t, g, "u___1", f)
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()
		start := time.Now()
		_ = g.Shutdown(ctx)
		if !f.isClosed() || time.Since(start) != 100*time.Millisecond {
			t.Fatal("control delayed hard close")
		}
		if deadline := <-f.deadline; !deadline.Equal(start.Add(100 * time.Millisecond)) {
			t.Fatalf("control deadline %v exceeds budget", deadline)
		}
	})
}

// A purge that honours its context must see the caller's deadline, or it hangs past shutdown.
func TestShutdownPurgeUsesCallerDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		g := New(testConfig(), Deps{Online: &blockingOnline{blockPurge: true, waitCtx: true}})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		err := g.Shutdown(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) != time.Second {
			t.Fatalf("purge: elapsed=%v err=%v", time.Since(start), err)
		}
	})
}

func TestShutdownBoundsUncooperativePurge(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		f := &blockingOnline{blockPurge: true, entered: make(chan struct{}, 1), release: make(chan struct{})}
		g := New(testConfig(), Deps{Online: f})
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		go func() { <-f.entered; time.Sleep(2 * time.Second); close(f.release) }()
		start := time.Now()
		err := g.Shutdown(ctx)
		if elapsed := time.Since(start); elapsed != time.Second || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("purge elapsed=%v err=%v", elapsed, err)
		}
		<-f.release
		synctest.Wait()
	})
}

// A real go-redis socket read is not interrupted by a cancellation arriving after ZREM.
func TestShutdownBoundsRedisSocketRead(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	entered, release, serverDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	go func() {
		defer close(serverDone)
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
			if err != nil {
				t.Error(err)
				return
			}
			args := make([]string, n)
			for i := range n {
				line, err := reader.ReadString('\n')
				if err != nil {
					return
				}
				size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
				if err != nil {
					t.Error(err)
					return
				}
				data := make([]byte, size+2)
				if _, err := io.ReadFull(reader, data); err != nil {
					return
				}
				args[i] = string(data[:size])
			}
			reply := "+OK\r\n"
			switch strings.ToLower(args[0]) {
			case "hello":
				reply = "-ERR unknown command 'hello'\r\n"
			case "ping":
				reply = "+PONG\r\n"
			case "zrem":
				close(entered)
				<-release
				reply = ":1\r\n"
			}
			if _, err := io.WriteString(conn, reply); err != nil {
				return
			}
		}
	}()
	online, err := onlineredis.New(t.Context(), listener.Addr().String(), "", 0, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	cleanupDone := make(chan struct{})
	defer func() { close(release); <-cleanupDone; online.Close(); <-serverDone }()
	g := New(testConfig(), Deps{Online: online})
	socket := newFakeConn()
	c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", socket)
	if err := g.users.Register(c); err != nil {
		t.Fatal(err)
	}
	go func() { c.Close("test"); close(cleanupDone) }()
	<-entered
	liveSocket := newFakeConn()
	live := g.newClient(auth.Identity{UserId: "u___2"}, "c2", "", liveSocket)
	if err := g.users.Register(live); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = g.Shutdown(ctx)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("shutdown elapsed=%v err=%v, budget=100ms", elapsed, err)
	}
	if !socket.isClosed() || !liveSocket.isClosed() || g.users.Count() != 0 {
		t.Error("shutdown left socket or local slot open")
	}
}

func TestCloseReleasesSlotsBeforeRemove(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		online := &blockingOnline{blockRemove: true, waitCtx: true, entered: make(chan struct{}, 1)}
		cfg := testConfig()
		cfg.Limits.WsConnsPerUser, cfg.Limits.WsConnsPerToken, cfg.Limits.WsConnsPerIp = 1, 1, 1
		g := New(cfg, Deps{Online: online})
		socket := newFakeConn()
		c := g.newClient(auth.Identity{UserId: "u___1", TokenId: "t1"}, "c1", "127.0.0.1", socket)
		if err := g.users.Register(c); err != nil {
			t.Fatal(err)
		}
		done := make(chan struct{})
		go func() { c.Close("test"); close(done) }()
		<-online.entered
		if !socket.isClosed() || g.users.Count() != 0 {
			t.Error("Remove holds local socket/slot")
		}
		if err := g.users.Reserve(c.slot()); err != nil {
			t.Errorf("immediate reconnect: %v", err)
		} else {
			g.users.Release(c.slot())
		}
		<-done
		g.work.Wait()
	})
}

func TestShutdownBoundsCommittedSendPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := storetest.NewMem()
		if err := mem.UpsertUser(t.Context(), &store.User{Id: "u___2"}); err != nil {
			t.Fatal(err)
		}
		entered, finished := make(chan struct{}), make(chan struct{})
		publisher := message.PublisherFunc(func(ctx context.Context, _ message.PushEvent) {
			close(entered)
			<-ctx.Done()
			close(finished)
		})
		g := New(testConfig(), Deps{Message: message.New(message.Adapt(mem), publisher, 8192)})
		socket := newFakeConn()
		serve(t, g, "u___1", socket)
		socket.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)
		<-entered
		committed, err := mem.GetMessageByClientId(t.Context(), "si_u___1:u___2", "u___1", "c1")
		if err != nil || committed.Seq != 1 {
			t.Fatalf("message not committed: %v %v", committed, err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()
		start := time.Now()
		err = g.Shutdown(ctx)
		if elapsed := time.Since(start); elapsed != time.Second || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("post-commit publisher exceeded shutdown budget: %v, %v", elapsed, err)
		}
		if !socket.isClosed() {
			t.Error("socket still open")
		}
		<-finished
		g.work.Wait()
		if elapsed := time.Since(start); elapsed != 5*time.Second {
			t.Errorf("publisher lifetime = %v, want 5s", elapsed)
		}
	})
}

func TestConcurrentDrainDoesNotWaitForKickCleanup(t *testing.T) {
	f := &blockingOnline{blockRemove: true, entered: make(chan struct{}, 1), release: make(chan struct{})}
	cfg := testConfig()
	cfg.Ws.SendQueue = 1
	g := New(cfg, Deps{Online: f})
	c := g.newClient(auth.Identity{UserId: "u___1"}, "c1", "", newFakeConn())
	if err := g.users.Register(c); err != nil {
		t.Fatal(err)
	}
	if err := c.Send([]byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	kicked, drained := make(chan struct{}), make(chan struct{})
	go func() { c.kick(KickNewLogin); close(kicked) }()
	<-f.entered
	// Shutdown may already have snapshotted c before the concurrent kick unregisters it.
	go func() { c.closeAfterFlush(nil); close(drained) }()
	defer func() { close(f.release); <-kicked; <-drained }()
	select {
	case <-drained:
	case <-time.After(time.Second):
		t.Error("drain waited for concurrent kick cleanup")
	}
}
