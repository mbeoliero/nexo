package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	hzconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/internal/service"
	"github.com/mbeoliero/nexo/pkg/errcode"
)

func captureMessageHandlerLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(io.Discard)
	})
	return buf
}

type stubSendGate struct {
	err          error
	acquireCount int
	releaseCount int
}

func (s *stubSendGate) AcquireSendLease() (func(), error) {
	s.acquireCount++
	if s.err != nil {
		return nil, s.err
	}
	return func() {
		s.releaseCount++
	}, nil
}

func TestSendMessageRejectsWhenGateClosed(t *testing.T) {
	logBuf := captureMessageHandlerLogs(t)
	engine := route.NewEngine(hzconfig.NewOptions(nil))
	handler := NewMessageHandler(&service.MessageService{}, &stubSendGate{err: errcode.ErrServerShuttingDown})
	engine.POST("/send", func(ctx context.Context, c *app.RequestContext) {
		c.Set(middleware.UserIdKey, "u1")
		handler.SendMessage(ctx, c)
	})

	body, _ := json.Marshal(map[string]any{
		"session_type":  1,
		"client_msg_id": "cm1",
		"recv_id":       "u2",
	})
	resp := ut.PerformRequest(engine, "POST", "/send", &ut.Body{Body: bytes.NewBuffer(body), Len: len(body)}, ut.Header{Key: "Content-Type", Value: "application/json"}).Result()

	if resp.StatusCode() != 503 {
		t.Fatalf("status code mismatch: got %d want 503", resp.StatusCode())
	}
	if !strings.Contains(logBuf.String(), "http send message gate rejected") {
		t.Fatalf("expected gate rejection log, got %q", logBuf.String())
	}
}

func TestSendMessageReleasesLeaseAfterServiceCall(t *testing.T) {
	engine := route.NewEngine(hzconfig.NewOptions(nil))
	gate := &stubSendGate{}
	handler := NewMessageHandler(&service.MessageService{}, gate)
	engine.POST("/send", func(ctx context.Context, c *app.RequestContext) {
		c.Set(middleware.UserIdKey, "u1")
		handler.SendMessage(ctx, c)
	})

	body, _ := json.Marshal(map[string]any{
		"session_type": 1,
	})
	_ = ut.PerformRequest(engine, "POST", "/send", &ut.Body{Body: bytes.NewBuffer(body), Len: len(body)}, ut.Header{Key: "Content-Type", Value: "application/json"}).Result()

	if gate.acquireCount != 1 || gate.releaseCount != 1 {
		t.Fatalf("gate counts mismatch: acquire=%d release=%d", gate.acquireCount, gate.releaseCount)
	}
}
