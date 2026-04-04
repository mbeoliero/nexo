package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/cluster"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/jwt"
)

func TestHandleConnectionRejectsWhenIngressClosed(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.BeginDrain(cluster.DrainReasonPlanned); err != nil {
		t.Fatalf("begin drain: %v", err)
	}

	server := &WsServer{
		cfg:           &config.Config{},
		maxConnNum:    10,
		lifecycleGate: gate,
	}

	req := httptest.NewRequest(http.MethodGet, "/ws", nil)
	rec := httptest.NewRecorder()
	server.HandleConnection(req.Context(), rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status code mismatch: got %d want 503", rec.Code)
	}
}

func TestHandleConnectionRejectsRevokedToken(t *testing.T) {
	cfg := &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	}
	token, err := jwt.GenerateToken("u1", constant.PlatformIdWeb, cfg.JWT.Secret, 1)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	middleware.SetNativeTokenStateValidator(func(context.Context, *jwt.Claims, string) error {
		return errcode.ErrTokenInvalid
	})
	t.Cleanup(func() {
		middleware.SetNativeTokenStateValidator(nil)
	})

	server := &WsServer{
		cfg:           cfg,
		maxConnNum:    10,
		lifecycleGate: cluster.NewLifecycleGate(),
	}

	req := httptest.NewRequest(http.MethodGet, "/ws?token="+token+"&send_id=u1", nil)
	rec := httptest.NewRecorder()
	server.HandleConnection(req.Context(), rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status code mismatch: got %d want 401", rec.Code)
	}
}

func TestResolvePlatformIDRejectsInvalidOverride(t *testing.T) {
	if _, err := resolvePlatformId(constant.PlatformIdWeb, "999", true); err == nil {
		t.Fatal("expected invalid platform override to fail")
	}
}

func TestResolvePlatformIDRejectsDifferentOverrideWhenDisabled(t *testing.T) {
	if _, err := resolvePlatformId(constant.PlatformIdWeb, "1", false); err == nil {
		t.Fatal("expected different platform override to be rejected when override is disabled")
	}
}

func TestResolvePlatformIDAllowsSameOverrideWhenDisabled(t *testing.T) {
	got, err := resolvePlatformId(constant.PlatformIdWeb, "5", false)
	if err != nil {
		t.Fatalf("resolve platform id: %v", err)
	}
	if got != constant.PlatformIdWeb {
		t.Fatalf("platform id mismatch: got %d want %d", got, constant.PlatformIdWeb)
	}
}

func TestResolvePlatformIDAcceptsKnownOverrideWhenEnabled(t *testing.T) {
	got, err := resolvePlatformId(constant.PlatformIdWeb, "1", true)
	if err != nil {
		t.Fatalf("resolve platform id: %v", err)
	}
	if got != constant.PlatformIdIOS {
		t.Fatalf("platform id mismatch: got %d want %d", got, constant.PlatformIdIOS)
	}
}

func TestRegisterClientOrCloseFailsFastWhenRegisterQueueIsBlocked(t *testing.T) {
	oldTimeout := clientRegisterEnqueueTimeout
	clientRegisterEnqueueTimeout = 20 * time.Millisecond
	defer func() {
		clientRegisterEnqueueTimeout = oldTimeout
	}()

	server := &WsServer{
		cfg:          &config.Config{},
		registerChan: make(chan *Client, 1),
	}
	server.registerChan <- &Client{UserId: "other", ConnId: "other"}

	conn := &stubClientConn{}
	client := NewClient(conn, "u1", 5, SDKTypeGo, "token", "c1", server)

	if ok := server.registerClientOrClose(context.Background(), client); ok {
		t.Fatal("expected registration to fail fast when register queue is blocked")
	}
	if !client.IsClosed() {
		t.Fatal("expected client to be closed after registration enqueue timeout")
	}
	if !conn.closed {
		t.Fatal("expected underlying connection to be closed after registration enqueue timeout")
	}
}

func TestAsyncPushToUsersRejectsWhenSendPathClosed(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.CloseSendPath(); err != nil {
		t.Fatalf("close send path: %v", err)
	}

	server := &WsServer{
		cfg:           &config.Config{},
		pushChan:      make(chan *PushTask, 1),
		lifecycleGate: gate,
	}

	server.AsyncPushToUsers(&entity.Message{ConversationId: "c1", Seq: 1}, []string{"u1"}, "")

	if len(server.pushChan) != 0 {
		t.Fatal("expected push to be rejected when send path is closed")
	}
}

func TestDeliverLocalRejectsWhenSendPathClosed(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	if err := gate.CloseSendPath(); err != nil {
		t.Fatalf("close send path: %v", err)
	}

	server := &WsServer{
		cfg:           &config.Config{},
		userMap:       NewUserMap(nil),
		lifecycleGate: gate,
	}
	conn := &stubClientConn{}
	client := NewClient(conn, "u1", 5, SDKTypeGo, "token", "c1", server)
	server.userMap.Register(context.Background(), client)

	if err := server.DeliverLocal(context.Background(), []cluster.ConnRef{{UserId: "u1", ConnId: "c1", PlatformId: 5}}, &cluster.PushPayload{
		Message: &cluster.PushMessage{ConversationId: "conv1"},
	}); err != nil {
		t.Fatalf("deliver local: %v", err)
	}

	if len(conn.writes) != 0 {
		t.Fatalf("expected no writes after send path closes, got %d", len(conn.writes))
	}
}

type lifecycleClosingStubClientConn struct {
	stubClientConn
	onWrite func()
}

func (s *lifecycleClosingStubClientConn) WriteMessage(data []byte) error {
	if s.onWrite != nil {
		s.onWrite()
		s.onWrite = nil
	}
	return s.stubClientConn.WriteMessage(data)
}

func (s *lifecycleClosingStubClientConn) SetReadDeadline(_ time.Time) error  { return nil }
func (s *lifecycleClosingStubClientConn) SetWriteDeadline(_ time.Time) error { return nil }

func TestDeliverLocalStopsWhenSendPathClosesMidLoop(t *testing.T) {
	gate := cluster.NewLifecycleGate()
	server := &WsServer{
		cfg:           &config.Config{},
		userMap:       NewUserMap(nil),
		lifecycleGate: gate,
	}

	conn1 := &lifecycleClosingStubClientConn{
		onWrite: func() {
			_ = gate.ForceCloseSendPath()
		},
	}
	conn2 := &lifecycleClosingStubClientConn{}
	client1 := NewClient(conn1, "u1", 5, SDKTypeGo, "token", "c1", server)
	client2 := NewClient(conn2, "u1", 1, SDKTypeGo, "token", "c2", server)
	server.userMap.Register(context.Background(), client1)
	server.userMap.Register(context.Background(), client2)

	if err := server.DeliverLocal(context.Background(), []cluster.ConnRef{
		{UserId: "u1", ConnId: "c1", PlatformId: 5},
		{UserId: "u1", ConnId: "c2", PlatformId: 1},
	}, &cluster.PushPayload{
		Message: &cluster.PushMessage{ConversationId: "conv1"},
	}); err != nil {
		t.Fatalf("deliver local: %v", err)
	}

	if len(conn1.writes) != 1 {
		t.Fatalf("first connection write count mismatch: got %d want 1", len(conn1.writes))
	}
	if len(conn2.writes) != 0 {
		t.Fatalf("expected second connection to stop after send path closed, got %d writes", len(conn2.writes))
	}
}
