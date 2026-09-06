package gateway

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
)

func serveAs(t *testing.T, g *Gateway, id auth.Identity, connId string) (*Client, *fakeConn) {
	t.Helper()
	f := newFakeConn()
	return serveConn(t, g, id, connId, f), f
}

func expectKick(t *testing.T, f *fakeConn, reason string) {
	t.Helper()
	r := f.next(t)
	if r.ReqId != KickOnline || fmtData(r) != "{reason:"+reason+"}" {
		t.Fatalf("want 2002 %s, got %+v", reason, r)
	}
	waitClosed(t, f)
}

func TestKickSamePlatformOtherToken(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.WsConnsPerUser = 5
	g := newGateway(t, cfg)
	_, old := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1"}, "c1")
	_, otherPlatform := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 2, TokenId: "t2"}, "c2")
	_, sameToken := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1"}, "c3")

	g.Kick("u___1", 1, "t1") // reconnect with the same token: nobody is kicked
	old.quiet(t)
	sameToken.quiet(t)

	g.Kick("u___1", 1, "t9") // new login on platform 1
	expectKick(t, old, KickNewLogin)
	expectKick(t, sameToken, KickNewLogin)
	otherPlatform.quiet(t)
	if got := g.users.Get("u___1"); len(got) != 1 || got[0].PlatformId != 2 {
		t.Fatalf("survivors: %+v", got)
	}
}

func TestKickFlushesQueuedFramesFirst(t *testing.T) {
	g := newGateway(t, testConfig())
	c, f := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1"}, "c1")
	_ = c.Send([]byte(`{"req_id":2001,"data":{}}`))
	c.kick(KickNewLogin)
	if r := f.next(t); r.ReqId != PushMsg {
		t.Fatalf("queued frame must be flushed before 2002: %+v", r)
	}
	expectKick(t, f, KickNewLogin)
}

// fakeChecker replays errs in order and repeats the last one.
type fakeChecker struct {
	mu   sync.Mutex
	errs []error
}

func (f *fakeChecker) Check(context.Context, auth.Identity) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	err := f.errs[0]
	if len(f.errs) > 1 {
		f.errs = f.errs[1:]
	}
	return err
}

func TestNativeRecheckFailureSequence(t *testing.T) {
	g := newGateway(t, testConfig())
	g.deps.Native = &fakeChecker{errs: []error{
		auth.ErrUnavailable, auth.ErrUnavailable, nil,
		auth.ErrUnavailable, auth.ErrUnavailable, auth.ErrUnavailable,
	}}
	c := g.newClient(auth.Identity{Source: auth.SourceNative}, "sequence", "127.0.0.1", newFakeConn())
	defer c.Close("test")
	for i, fails := range []int{1, 2, 0, 1, 2, 3} {
		dead := c.tokenDead(time.Now())
		if dead != (i == 5) || c.recheckFails != fails {
			t.Fatalf("check %d: dead=%v failures=%d, want dead=%v failures=%d", i+1, dead, c.recheckFails, i == 5, fails)
		}
	}
}

func TestRecheckKicksExpiredExternalToken(t *testing.T) {
	g := newGateway(t, testConfig())
	g.recheck = 20 * time.Millisecond
	_, live := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1", Source: auth.SourceExternal, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}, "c1")
	_, expired := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 2, TokenId: "t2", Source: auth.SourceExternal, ExpiresAt: time.Now().Add(-time.Second).UnixMilli()}, "c2")
	expectKick(t, expired, KickTokenExpired)
	live.quiet(t)
}

func TestRecheckNativeToken(t *testing.T) {
	cases := map[string]struct {
		errs   []error
		kicked bool
	}{
		"revoked": {[]error{auth.ErrTokenInvalid}, true},
		"ok":      {[]error{nil}, false},
		// An outage is tolerated for recheckFailLimit-1 ticks, then the connection is kicked.
		"outage":    {[]error{auth.ErrUnavailable}, true},
		"transient": {[]error{auth.ErrUnavailable, auth.ErrUnavailable, nil}, false},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				g := newGateway(t, testConfig())
				g.recheck = 20 * time.Millisecond
				g.deps.Native = &fakeChecker{errs: tc.errs}
				_, f := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1", Source: auth.SourceNative, ExpiresAt: time.Now().Add(time.Hour).UnixMilli()}, "c1")
				synctest.Wait()
				time.Sleep(4 * g.recheck)
				synctest.Wait()
				if tc.kicked {
					expectKick(t, f, KickTokenExpired)
				} else {
					if f.isClosed() {
						t.Fatal("healthy or recovered token was closed")
					}
					select {
					case raw := <-f.out:
						t.Fatalf("unexpected frame: %s", raw)
					default:
					}
				}
			})
		})
	}
}

func TestHandshakeKicksOlderLogin(t *testing.T) {
	g := newGateway(t, testConfig())
	// An external token id is a hash of the token, so a second token(1) call with a
	// different exp would differ; here we plant the old connection directly.
	_, old := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "stale"}, "c0")
	url := startServer(t, g)
	conn, _, err := dialWs(url + "?platform_id=1&token=" + token(1))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	expectKick(t, old, KickNewLogin)
	for range 50 {
		if got := g.users.Get("u___1"); len(got) == 1 && got[0].TokenId != "stale" {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("survivor: %+v", g.users.Get("u___1"))
}

// A frame arriving after exp fails with its req_id, then 2002 token_expired and close (design §8.1).
func TestExpiredTokenRejectedPerFrame(t *testing.T) {
	g := newGateway(t, testConfig())
	g.recheck = time.Hour
	_, f := serveAs(t, g, auth.Identity{UserId: "u___1", PlatformId: 1, TokenId: "t1", Source: auth.SourceExternal, ExpiresAt: time.Now().Add(-time.Second).UnixMilli()}, "c1")
	f.in <- []byte(`{"req_id":1003,"op_id":"o1","data":{}}`)
	if r := f.next(t); r.OpId != "o1" || r.Code != errcode.ErrTokenExpired.Code {
		t.Fatalf("want token expired error for the frame, got %+v", r)
	}
	expectKick(t, f, KickTokenExpired)
}
