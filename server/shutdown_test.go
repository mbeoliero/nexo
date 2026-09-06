package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"
	"uuid"

	"github.com/cloudwego/hertz/pkg/app"
	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
)

func TestEmbeddedShutdownSequence(t *testing.T) {
	dsn := os.Getenv("NEXO_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("NEXO_TEST_PG_DSN empty")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cfg := DefaultConfig()
	cfg.NodeId = uuid.NewV7().String()
	cfg.Db.Dsn = dsn
	cfg.Auth.Native.Secret = "0123456789abcdef0123456789abcdef"
	s, err := New(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.Start(ctx)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	h := hzserver.New(
		hzserver.WithListener(ln),
		hzserver.WithTransport(standard.NewTransporter),
		hzserver.WithExitWaitTime(time.Second),
	)
	s.Mount(h.Engine)
	entered := make(chan struct{})
	hostStopping := make(chan struct{})
	drainFinished := make(chan struct{})
	h.OnShutdown = append(h.OnShutdown, func(context.Context) { close(hostStopping) })
	h.GET("/in-flight", func(_ context.Context, c *app.RequestContext) {
		close(entered)
		// Keep this request alive until both shutdown paths have started and Drain has returned.
		for _, done := range []<-chan struct{}{hostStopping, drainFinished} {
			select {
			case <-done:
			case <-ctx.Done():
				c.String(http.StatusGatewayTimeout, "shutdown did not progress")
				return
			}
		}
		if err := s.app.Deps().Ready(ctx); err != nil {
			c.String(http.StatusInternalServerError, "owned database closed: %v", err)
			return
		}
		c.String(http.StatusOK, "database still open")
	})
	runDone := make(chan error, 1)
	go func() { runDone <- h.Run() }()
	defer func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Second)
		defer cleanupCancel()
		_ = h.Shutdown(cleanupCtx)
		_ = s.Drain(cleanupCtx)
	}()
	requestDone := make(chan error, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+ln.Addr().String()+"/in-flight", nil)
		if err != nil {
			requestDone <- err
			return
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			requestDone <- err
			return
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err == nil && (resp.StatusCode != http.StatusOK || string(body) != "database still open") {
			err = fmt.Errorf("in-flight response: %d %s", resp.StatusCode, body)
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case err := <-requestDone:
		t.Fatalf("request ended before handler entered: %v", err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}

	// Execute the embedding guide's sequence; no dependencies may close before HTTP finishes.
	sctx, shutdownCancel := context.WithTimeout(ctx, 5*time.Second)
	defer shutdownCancel()
	drained := make(chan error, 1)
	go func() {
		drained <- s.Drain(sctx)
		close(drainFinished)
	}()
	httpShutdownErr := h.Shutdown(sctx)
	drainErr := <-drained
	s.Close()
	if err := errors.Join(httpShutdownErr, drainErr); err != nil {
		t.Fatal(err)
	}
	if err := <-requestDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil && !errors.Is(err, net.ErrClosed) {
		t.Fatal(err)
	}
	if err := ctx.Err(); err != nil {
		t.Fatal(err)
	}
	if err := s.app.Deps().Ready(ctx); err == nil {
		t.Fatal("Close left the owned database open")
	}
}
