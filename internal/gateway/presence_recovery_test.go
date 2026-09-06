package gateway

import (
	"context"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	onlinedb "github.com/mbeoliero/nexo/internal/onlinestore/db"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

type recoveryOnline struct {
	onlinestore.OnlineStore
	adds         atomic.Int32
	retryStarted chan struct{}
	releaseRetry chan struct{}
}

func (s *recoveryOnline) Add(ctx context.Context, nodeId string, ref onlinestore.ConnRef) error {
	switch s.adds.Add(1) {
	case 1:
		return errors.New("injected initial presence outage")
	case 2:
		if s.retryStarted != nil {
			close(s.retryStarted)
			select {
			case <-s.releaseRetry:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return s.OnlineStore.Add(ctx, nodeId, ref)
}

func TestPresenceRecovery(t *testing.T) {
	backends := []struct {
		name   string
		driver string
		env    string
	}{
		{name: "memory"},
		{name: "gorm-postgres", driver: "postgres", env: "NEXO_TEST_PG_DSN"},
		{name: "gorm-mysql", driver: "mysql", env: "NEXO_TEST_MYSQL_DSN"},
		{name: "sqlc-postgres", env: "NEXO_TEST_PG_DSN"},
	}
	for _, backend := range backends {
		t.Run(backend.name, func(t *testing.T) {
			if backend.env != "" && os.Getenv(backend.env) == "" {
				t.Skip(backend.env + " not set")
			}
			for _, scenario := range []string{
				"renew-recovers", "closed-stays-offline", "draining-stays-offline",
				"retry-before-close", "mixed-recovery",
			} {
				t.Run(scenario, func(t *testing.T) {
					var st store.Store
					var err error
					switch {
					case backend.env == "":
						st = storetest.NewMem()
					case backend.driver == "":
						st, err = pgstore.New(t.Context(), os.Getenv(backend.env), 4)
					default:
						st, err = gormstore.New(backend.driver, os.Getenv(backend.env), 4)
					}
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(st.Close)
					online := &recoveryOnline{OnlineStore: onlinedb.New(st, time.Minute)}
					cfg := testConfig()
					cfg.NodeId = uuid.NewV7().String()
					g := New(cfg, Deps{Online: online})
					c := g.newClient(
						auth.Identity{UserId: identity.NativeUserId(uuid.NewV7().String()), PlatformId: 1},
						uuid.NewV7().String(),
						"127.0.0.1",
						newFakeConn(),
					)
					t.Cleanup(func() {
						c.Close("test")
						g.work.Wait()
						g.cancelRun()
						g.cancelOps()
						g.cancel()
					})
					if err := g.users.Register(c); err != nil {
						t.Fatal(err)
					}
					g.onlineAdd(c)
					assertRecoveryOnline(t, online, c, false)
					if online.adds.Load() != 1 || g.users.Count() != 1 || c.ctx().Err() != nil {
						t.Fatal("initial Add failure must leave the connection active")
					}
					switch scenario {
					case "renew-recovers":
						if n, err := g.renew(t.Context()); err != nil || n != 1 {
							t.Fatalf("renew: count=%d err=%v", n, err)
						}
						assertRecoveryOnline(t, online, c, true)
						if _, err := g.renew(t.Context()); err != nil {
							t.Fatal(err)
						}
						if online.adds.Load() != 2 {
							t.Fatalf("successful Add must not be retried: calls=%d", online.adds.Load())
						}
					case "closed-stays-offline", "draining-stays-offline":
						if scenario == "closed-stays-offline" {
							c.Close("test")
							g.work.Wait()
						} else {
							c.kick(KickNewLogin)
						}
						if _, err := g.renew(t.Context()); err != nil {
							t.Fatal(err)
						}
						assertRecoveryOnline(t, online, c, false)
						if online.adds.Load() != 1 {
							t.Fatalf("inactive client retried Add: calls=%d", online.adds.Load())
						}
					case "retry-before-close":
						testRecoveryCloseOrdering(
							t,
							g,
							c,
							online,
						)
					case "mixed-recovery":
						healthy := g.newClient(
							auth.Identity{UserId: identity.NativeUserId(uuid.NewV7().String()), PlatformId: 1},
							uuid.NewV7().String(),
							"127.0.0.1",
							newFakeConn(),
						)
						t.Cleanup(func() { healthy.Close("test"); g.work.Wait() })
						if err := g.users.Register(healthy); err != nil {
							t.Fatal(err)
						}
						g.onlineAdd(healthy)
						assertRecoveryOnline(t, online, healthy, true)
						if err := st.RenewOnlineConns(
							t.Context(),
							cfg.NodeId,
							[]string{healthy.Id},
							time.Now().Add(-2*time.Minute),
						); err != nil {
							t.Fatal(err)
						}
						online.adds.Store(0) // fail this client's next retry too
						if _, err := g.renew(t.Context()); err == nil {
							t.Error("retry error was lost")
						}
						assertRecoveryOnline(t, online, healthy, true)
						assertRecoveryOnline(t, online, c, false)
					}
				})
			}
		})
	}
}

func assertRecoveryOnline(t *testing.T, online onlinestore.OnlineStore, c *Client, want bool) {
	t.Helper()
	status, err := online.Online(t.Context(), []string{c.UserId})
	if err != nil {
		t.Fatal(err)
	}
	if got := slices.Contains(status[c.UserId], c.PlatformId); got != want {
		t.Fatalf("presence online=%v, want %v after initial Add failure", got, want)
	}
}

func testRecoveryCloseOrdering(t *testing.T, g *Gateway, c *Client, online *recoveryOnline) {
	t.Helper()
	online.retryStarted = make(chan struct{})
	online.releaseRetry = make(chan struct{})
	release := sync.OnceFunc(func() { close(online.releaseRetry) })
	defer release()
	renewed := make(chan error, 1)
	go func() {
		_, err := g.renew(t.Context())
		renewed <- err
	}()
	select {
	case <-online.retryStarted:
	case err := <-renewed:
		t.Fatalf("renew finished without retrying failed Add: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("renew did not retry Add within 2s")
	}
	// Add is blocked while renew owns presence; Close must remove after that Add.
	closed := make(chan struct{})
	go func() {
		c.Close("test")
		close(closed)
	}()
	select {
	case <-c.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not close the client during the pending Add")
	}
	release()
	if err := <-renewed; err != nil {
		t.Fatal(err)
	}
	<-closed
	g.work.Wait()
	assertRecoveryOnline(t, online, c, false)
	if _, err := g.renew(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertRecoveryOnline(t, online, c, false)
	if online.adds.Load() != 2 {
		t.Fatalf("closed client retried Add: calls=%d", online.adds.Load())
	}
}
