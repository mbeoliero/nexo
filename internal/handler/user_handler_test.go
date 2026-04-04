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
)

func captureUserHandlerLogs(t *testing.T) *bytes.Buffer {
	t.Helper()
	buf := &bytes.Buffer{}
	log.SetOutput(buf)
	t.Cleanup(func() {
		log.SetOutput(io.Discard)
	})
	return buf
}

type stubPresenceQuery struct {
	result *service.PresenceQueryResult
}

func (s *stubPresenceQuery) GetUsersOnlineStatus(_ context.Context, _ []string) (*service.PresenceQueryResult, error) {
	return s.result, nil
}

func TestGetUsersOnlineStatusRejectsMoreThan100UserIDs(t *testing.T) {
	engine := route.NewEngine(hzconfig.NewOptions(nil))
	handler := NewUserHandler(nil, &stubPresenceQuery{})
	engine.POST("/status", func(ctx context.Context, c *app.RequestContext) {
		c.Set(middleware.UserIdKey, "u1")
		handler.GetUsersOnlineStatus(ctx, c)
	})

	req := GetUsersOnlineStatusReq{UserIds: make([]string, 101)}
	for i := range req.UserIds {
		req.UserIds[i] = "u"
	}
	body, _ := json.Marshal(req)

	resp := ut.PerformRequest(engine, "POST", "/status", &ut.Body{Body: bytes.NewBuffer(body), Len: len(body)}, ut.Header{Key: "Content-Type", Value: "application/json"}).Result()
	if resp.StatusCode() != 200 {
		t.Fatalf("status code mismatch: got %d want 200", resp.StatusCode())
	}

	var payload map[string]any
	if err := json.Unmarshal(resp.Body(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload["code"].(float64) == 0 {
		t.Fatalf("expected validation failure: %+v", payload)
	}
}

func TestGetUsersOnlineStatusReturnsSuccessWithMetaOnPartial(t *testing.T) {
	logBuf := captureUserHandlerLogs(t)
	engine := route.NewEngine(hzconfig.NewOptions(nil))
	handler := NewUserHandler(nil, &stubPresenceQuery{
		result: &service.PresenceQueryResult{
			Users: []*service.OnlineStatusResult{
				{UserId: "u1", Status: 1},
			},
			Partial:    true,
			DataSource: "local_only",
		},
	})
	engine.POST("/status", func(ctx context.Context, c *app.RequestContext) {
		c.Set(middleware.UserIdKey, "u1")
		handler.GetUsersOnlineStatus(ctx, c)
	})

	body, _ := json.Marshal(GetUsersOnlineStatusReq{UserIds: []string{"u1"}})
	resp := ut.PerformRequest(engine, "POST", "/status", &ut.Body{Body: bytes.NewBuffer(body), Len: len(body)}, ut.Header{Key: "Content-Type", Value: "application/json"}).Result()

	var payload map[string]any
	if err := json.Unmarshal(resp.Body(), &payload); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if payload["meta"] == nil {
		t.Fatalf("expected meta in response: %+v", payload)
	}
	if !strings.Contains(logBuf.String(), "http get users online status partial response") {
		t.Fatalf("expected partial response log, got %q", logBuf.String())
	}
}
