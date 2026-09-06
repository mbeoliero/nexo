package gateway

import (
	"encoding/json/v2"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/golang-jwt/jwt/v5"
	gws "github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/storetest"
)

func testConfig() *config.Config {
	cfg := &config.Config{}
	cfg.Ws = config.WsConfig{MaxFrameBytes: 65536, SendQueue: 4, PingInterval: 50 * time.Millisecond, PongWait: time.Second, DeliverWorkers: 4, DeliverQueue: 64}
	cfg.Limits = config.LimitsConfig{PullPageMax: 100, ConversationPageMax: 100, MaxSeqsPageMax: 100, WsConnsPerUser: 2, WsConnsPerToken: 3, WsConnsPerIp: 50, WsConnsTotal: 100,
		WsFramesPerSec: 100, WsInflightPerConn: 8, WsSendBytesTotal: 1 << 20}
	return cfg
}

func newGateway(t *testing.T, cfg *config.Config) *Gateway {
	t.Helper()
	m := storetest.NewMem()
	for _, id := range []string{"u___1", "u___2"} {
		_ = m.UpsertUser(t.Context(), &store.User{Id: id, UpdatedAt: time.Now()})
	}
	return New(cfg, Deps{Auth: auth.NewExternal([]string{"ext"}, "user"), Message: message.New(message.Adapt(m), message.NoopPublisher{}, 8192), Conv: conversation.New(m, conversation.NoopNotifier{})})
}

func token(userId int64) string {
	s, _ := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{"user_id": userId, "exp": time.Now().Add(time.Hour).Unix()}).SignedString([]byte("ext"))
	return s
}

// fakeConn feeds frames through in and records writes; Close unblocks ReadMessage.
type fakeConn struct {
	in     chan []byte
	out    chan []byte
	done   chan struct{}
	once   sync.Once
	block  bool // WriteMessage never returns: simulates a stalled peer
	pings  int
	mu     sync.Mutex
	closed bool
}

func newFakeConn() *fakeConn {
	return &fakeConn{in: make(chan []byte), out: make(chan []byte, 64), done: make(chan struct{})}
}

func (f *fakeConn) ReadMessage() ([]byte, error) {
	select {
	case b := <-f.in:
		return b, nil
	case <-f.done:
		return nil, errcode.ErrConnClosed
	}
}

func (f *fakeConn) WriteMessage(b []byte) error {
	if f.block {
		<-f.done
		return errors.New("closed")
	}
	f.out <- b
	return nil
}

func (f *fakeConn) WritePing() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pings++
	return nil
}

func (f *fakeConn) Close() error {
	f.once.Do(func() {
		f.mu.Lock()
		f.closed = true
		f.mu.Unlock()
		close(f.done)
	})
	return nil
}

func (f *fakeConn) isClosed() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.closed
}

func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

func (f *fakeConn) next(t *testing.T) Response {
	t.Helper()
	select {
	case b := <-f.out:
		var r Response
		if err := json.Unmarshal(b, &r); err != nil {
			t.Fatalf("bad frame %s: %v", b, err)
		}
		return r
	case <-time.After(2 * time.Second):
		t.Fatal("no frame within 2s")
	}
	return Response{}
}

// serveConn runs conn as a registered client; the cleanup closes it and waits for its workers.
func serveConn(t *testing.T, g *Gateway, id auth.Identity, connId string, conn ClientConn) *Client {
	t.Helper()
	c := g.newClient(id, connId, "127.0.0.1", conn)
	if err := g.users.Register(c); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { c.Serve(); close(done) }()
	t.Cleanup(func() {
		c.Close("test")
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("client workers did not stop")
		}
	})
	return c
}

func serve(t *testing.T, g *Gateway, userId string, conn ClientConn) *Client {
	t.Helper()
	return serveConn(t, g, auth.Identity{UserId: userId, PlatformId: 1, TokenId: "tok-" + userId}, "conn-"+userId, conn)
}

func TestClientRoundTrip(t *testing.T) {
	g := newGateway(t, testConfig())
	f := newFakeConn()
	serve(t, g, "u___1", f)

	f.in <- []byte(`{"req_id":1003,"op_id":"op1","msg_incr":"c-1","data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)
	r := f.next(t)
	if r.Code != 0 || r.OpId != "op1" || r.MsgIncr != "c-1" || !strings.Contains(fmt.Sprint(r.Data), "seq:1") {
		t.Fatalf("ack: %+v", r)
	}
	conv := r.Data.(map[string]any)
	f.in <- []byte(`{"req_id":1001,"data":{}}`)
	if r = f.next(t); r.Code != 0 || !strings.Contains(fmt.Sprint(r.Data), "max_seq:1") {
		t.Fatalf("max_seqs: %+v", r)
	}
	f.in <- fmt.Appendf(nil, `{"req_id":1002,"data":{"conversation_id":%q,"begin_seq":1,"end_seq":10}}`, conv["conversation_id"])
	if r = f.next(t); r.Code != 0 || !strings.Contains(fmt.Sprint(r.Data), "client_msg_id:c1") {
		t.Fatalf("pull: %+v", r)
	}
	f.in <- fmt.Appendf(nil, `{"req_id":1004,"data":{"conversation_id":%q,"read_seq":1}}`, conv["conversation_id"])
	if r = f.next(t); r.Code != 0 || !strings.Contains(fmt.Sprint(r.Data), "read_seq:1") {
		t.Fatalf("read: %+v", r)
	}
	// Business errors come back as frames; the connection stays open.
	f.in <- []byte(`{"req_id":1003,"data":{"client_msg_id":"c2","session_type":1,"recv_id":"u___9","content_type":1,"content":"{}"}}`)
	if r = f.next(t); r.Code != errcode.ErrUserNotFound.Code {
		t.Fatalf("want 10201, got %+v", r)
	}
	f.in <- []byte(`not json`)
	if r = f.next(t); r.Code != errcode.ErrInvalidProtocol.Code {
		t.Fatalf("want 10603, got %+v", r)
	}
	f.in <- []byte(`{"req_id":9999}`)
	if r = f.next(t); r.Code != errcode.ErrInvalidProtocol.Code || r.ReqId != 9999 {
		t.Fatalf("unknown req: %+v", r)
	}
	if f.isClosed() {
		t.Fatal("errors must not close the connection")
	}
}

func TestClientRateLimitClosesAfterThree(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.WsFramesPerSec, cfg.Ws.SendQueue = 1, 8 // burst 2; queue holds all 5 replies
	g := newGateway(t, cfg)
	f := newFakeConn()
	serve(t, g, "u___1", f)

	for range 5 {
		f.in <- []byte(`{"req_id":1001}`)
	}
	waitClosed(t, f)
	// The third 10005 may be cut off by the close; the first two must arrive.
	var limited int
	for done := false; !done; {
		select {
		case b := <-f.out:
			var r Response
			_ = json.Unmarshal(b, &r)
			if r.Code == errcode.ErrTooManyRequests.Code {
				limited++
			}
		default:
			done = true
		}
	}
	if limited < 2 || !f.isClosed() || g.Stats().RateLimited != 3 {
		t.Fatalf("limited=%d closed=%v stats=%+v", limited, f.isClosed(), g.Stats())
	}
}

func TestSlowConsumerIsClosedAndCounted(t *testing.T) {
	g := newGateway(t, testConfig())
	f := newFakeConn()
	f.block = true
	c := serve(t, g, "u___1", f)

	var err error
	for range testConfig().Ws.SendQueue + 2 {
		if err = c.Send([]byte(`{}`)); err != nil {
			break
		}
	}
	if !errors.Is(err, errcode.ErrConnClosed) || !f.isClosed() || g.Stats().SlowConsumers != 1 {
		t.Fatalf("err=%v closed=%v stats=%+v", err, f.isClosed(), g.Stats())
	}
	if g.users.Count() != 0 || g.sendBytes.Load() != 0 {
		t.Fatalf("leak: conns=%d bytes=%d", g.users.Count(), g.sendBytes.Load())
	}
}

func TestSendBytesCapSendsResync(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.WsSendBytesTotal = 10
	g := newGateway(t, cfg)
	f := newFakeConn()
	serve(t, g, "u___1", f)

	if err := c(g, "u___1").Push([]byte(strings.Repeat("x", 11))); err != nil {
		t.Fatal(err)
	}
	if r := f.next(t); r.ReqId != Resync {
		t.Fatalf("want 2004, got %+v", r)
	}
	if g.Stats().Dropped != 1 {
		t.Fatalf("stats=%+v", g.Stats())
	}
}

func waitClosed(t *testing.T, f *fakeConn) {
	t.Helper()
	for range 100 {
		if f.isClosed() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("connection not closed")
}

func c(g *Gateway, userId string) *Client { return g.users.Get(userId)[0] }

func TestPingLoop(t *testing.T) {
	g := newGateway(t, testConfig())
	f := newFakeConn()
	serve(t, g, "u___1", f)
	time.Sleep(180 * time.Millisecond)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.pings < 2 {
		t.Fatalf("pings=%d", f.pings)
	}
}

func TestUserMapLimits(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.WsConnsPerUser, cfg.Limits.WsConnsPerToken, cfg.Limits.WsConnsPerIp, cfg.Limits.WsConnsTotal = 2, 1, 3, 4
	g := newGateway(t, cfg)
	mk := func(user, tok, ip string) *Client {
		return g.newClient(auth.Identity{UserId: user, TokenId: tok}, "c-"+user+tok+ip, ip, newFakeConn())
	}
	reg := func(c *Client) int {
		if e := errcode.From(g.users.Register(c)); e != nil {
			return e.Code
		}
		return 0
	}
	a := mk("u___1", "t1", "ip1")
	if reg(a) != 0 || reg(mk("u___1", "t1", "ip1")) != errcode.ErrConnOverLimit.Code {
		t.Fatal("per-token")
	}
	if reg(mk("u___1", "t2", "ip1")) != 0 || reg(mk("u___1", "t3", "ip1")) != errcode.ErrConnOverLimit.Code {
		t.Fatal("per-user")
	}
	if reg(mk("u___2", "t4", "ip1")) != 0 || reg(mk("u___3", "t5", "ip1")) != errcode.ErrConnOverLimit.Code {
		t.Fatal("per-ip")
	}
	if reg(mk("u___3", "t5", "ip2")) != 0 || reg(mk("u___4", "t6", "ip2")) != errcode.ErrConnOverLimit.Code {
		t.Fatal("total")
	}
	a.Close("test")
	if len(g.users.Get("u___1")) != 1 || g.users.Count() != 3 {
		t.Fatalf("after close: %d/%d", len(g.users.Get("u___1")), g.users.Count())
	}
	if !errors.Is(g.users.Register(a), errcode.ErrConnClosed) {
		t.Fatal("closed client must not be resurrected")
	}
}

func startServer(t *testing.T, g *Gateway) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	h := server.New(server.WithListener(ln), server.WithTransport(standard.NewTransporter))
	h.GET("/ws", g.Handle)
	go h.Spin()
	t.Cleanup(func() { _ = h.Shutdown(t.Context()) })
	url := "ws://" + ln.Addr().String() + "/ws"
	for range 50 {
		if conn, err := net.Dial("tcp", ln.Addr().String()); err == nil {
			conn.Close()
			return url
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not start")
	return url
}

func dialWs(url string) (*gws.Conn, int, error) {
	conn, resp, err := gws.DefaultDialer.Dial(url, nil)
	if err != nil {
		return nil, 0, err
	}
	return conn, resp.StatusCode, nil
}

func TestHandshake(t *testing.T) {
	g := newGateway(t, testConfig())
	url := startServer(t, g)
	dial := func(q string, hdr http.Header) (*gws.Conn, int) {
		conn, resp, err := gws.DefaultDialer.Dial(url+"?"+q, hdr)
		if err != nil {
			return nil, resp.StatusCode
		}
		return conn, resp.StatusCode
	}
	if _, st := dial("platform_id=1&token="+token(1)+"&encoding=protobuf", nil); st != http.StatusBadRequest {
		t.Fatalf("encoding: %d", st)
	}
	if _, st := dial("platform_id=0&token="+token(1), nil); st != http.StatusBadRequest {
		t.Fatalf("platform: %d", st)
	}
	if _, st := dial("platform_id=1&token=bad", nil); st != http.StatusUnauthorized {
		t.Fatalf("token: %d", st)
	}
	if _, st := dial("platform_id=1", nil); st != http.StatusUnauthorized {
		t.Fatalf("missing token: %d", st)
	}

	conn, st := dial("platform_id=1", http.Header{"Authorization": {"Bearer " + token(1)}})
	if st != http.StatusSwitchingProtocols {
		t.Fatalf("upgrade: %d", st)
	}
	defer conn.Close()
	if err := conn.WriteMessage(gws.TextMessage, []byte(`{"req_id":1003,"op_id":"o","data":{"client_msg_id":"c1","session_type":1,"recv_id":"u___2","content_type":1,"content":"{}"}}`)); err != nil {
		t.Fatal(err)
	}
	_, raw, err := conn.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	var r Response
	if json.Unmarshal(raw, &r) != nil || r.Code != 0 || r.OpId != "o" || !strings.Contains(string(raw), `"seq":1`) {
		t.Fatalf("ack over socket: %s", raw)
	}
	if got := g.users.Get("u___1"); len(got) != 1 || got[0].PlatformId != 1 || got[0].TokenId == "" {
		t.Fatalf("registered: %+v", got)
	}

	// Third connection for the same user exceeds ws_conns_per_user=2 → 429.
	c2, _ := dial("platform_id=2&token="+token(1), nil)
	defer c2.Close()
	time.Sleep(50 * time.Millisecond)
	// Capacity is reserved before the upgrade, so the client sees 429, never 101 + close.
	if _, st := dial("platform_id=3&token="+token(1), nil); st != http.StatusTooManyRequests {
		t.Fatalf("over limit: %d", st)
	}
	if g.users.Count() != 2 {
		t.Fatalf("refused handshake must release its slot: %d", g.users.Count())
	}

	conn.Close()
	for range 50 {
		if len(g.users.Get("u___1")) == 1 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("closed socket not unregistered")
}

// Browsers do not apply the same-origin policy to WebSocket, so ws.allowed_origins is the only
// thing standing between a hostile page and a connection opened with the visitor's token.
func TestOriginChecker(t *testing.T) {
	cases := []struct {
		name    string
		allowed []string
		origin  string
		want    bool
	}{
		{"default accepts any browser origin", nil, "https://evil.example", true},
		{"non-browser clients send no origin", []string{"https://app.example"}, "", true},
		{"allow-listed origin", []string{"https://app.example"}, "https://app.example", true},
		{"other origin", []string{"https://app.example"}, "https://evil.example", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &app.RequestContext{}
			if tc.origin != "" {
				c.Request.Header.Set("Origin", tc.origin)
			}
			if got := originChecker(tc.allowed)(c); got != tc.want {
				t.Fatalf("origin %q against %v = %v", tc.origin, tc.allowed, got)
			}
		})
	}
}
