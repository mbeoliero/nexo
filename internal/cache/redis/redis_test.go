package redis

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/mbeoliero/nexo/internal/cache/cachetest"
)

func TestSuite(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	c, err := New(t.Context(), addr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	cachetest.Run(t, c)
}

func TestGetContextDeadline(t *testing.T) {
	addr := os.Getenv("NEXO_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("NEXO_TEST_REDIS_ADDR not set")
	}
	proxyAddr, armed, delayed := delayedReplyProxy(t, addr)
	setupCtx, setupCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer setupCancel()
	c, err := New(setupCtx, proxyAddr, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	armed.Store(true)
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, found, err := c.Get(ctx, "nexo:deadline:"+uuid.NewV7().String())
	elapsed := time.Since(start)
	select {
	case <-delayed:
	default:
		t.Fatal("GET reply did not reach the delay gate")
	}
	netErr, timeout := errors.AsType[net.Error](err)
	if found || !(errors.Is(err, context.DeadlineExceeded) || (timeout && netErr.Timeout())) {
		t.Errorf("Get: found=%v, err=%v; want timeout", found, err)
	}
	if elapsed > 400*time.Millisecond {
		t.Errorf("100ms deadline took %s; want under 400ms", elapsed)
	}
	t.Logf("Get returned after %s: %v", elapsed, err)
}

// Forward constructor setup normally, then delay one reply by 600ms.
func delayedReplyProxy(t *testing.T, addr string) (string, *atomic.Bool, <-chan struct{}) {
	t.Helper()
	upstream, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	if err := upstream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	armed := new(atomic.Bool)
	delayed := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		downstream, err := ln.Accept()
		if err != nil {
			return
		}
		stop := context.AfterFunc(ctx, func() { _ = downstream.Close() })
		defer stop()
		copied := make(chan struct{})
		go func() {
			_, _ = io.Copy(upstream, downstream)
			_ = upstream.Close()
			close(copied)
		}()
		buf := make([]byte, 4096)
		for {
			n, readErr := upstream.Read(buf)
			if n > 0 {
				if armed.CompareAndSwap(true, false) {
					close(delayed)
					time.Sleep(600 * time.Millisecond)
				}
				if _, err := downstream.Write(buf[:n]); err != nil {
					break
				}
			}
			if readErr != nil {
				break
			}
		}
		_ = downstream.Close()
		_ = upstream.Close()
		<-copied
	}()
	t.Cleanup(func() {
		cancel()
		_ = ln.Close()
		_ = upstream.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("reply proxy did not stop")
		}
	})
	return ln.Addr().String(), armed, delayed
}
