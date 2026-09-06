package gateway

import (
	"context"
	"encoding/json/v2"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"uuid"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/hertz-contrib/websocket"
	"github.com/mbeoliero/kit/log"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/dto"
	"github.com/mbeoliero/nexo/internal/service/message"
)

// TokenChecker re-validates a native token against the TokenStore; *auth.Native implements it.
type TokenChecker interface {
	Check(ctx context.Context, id auth.Identity) error
}

type Deps struct {
	Auth    auth.Authenticator
	Native  TokenChecker            // nil when the native provider is off
	Bus     bus.Bus                 // nil = single node: kicks apply locally, nothing is subscribed
	Online  onlinestore.OnlineStore // nil = no global presence (tests)
	Message *message.Service
	Conv    *conversation.Service
}

type Gateway struct {
	cfg       *config.Config
	deps      Deps
	users     *UserMap
	upgrader  websocket.HertzUpgrader
	recheck   time.Duration
	closing   atomic.Bool
	ready     chan struct{} // closed once the Bus subscription is live (design §10: serve after subscribe)
	readyOnce sync.Once
	// ponytail: node-wide kick arbitration; shard by user if contention is measured.
	// Held only for lifecycle state, never UserMap access, sends or network I/O.
	kickMu sync.Mutex
	// ponytail: node-wide presence lock caps throughput at store latency; shard by user if needed.
	presence    chan struct{}
	cleanup     chan struct{} // bounds active/waiting Remove tasks; overflow expires by presence TTL
	workMu      sync.Mutex
	work        sync.WaitGroup
	sealed      bool
	shutdownCtx context.Context
	opsCtx      context.Context
	cancelOps   context.CancelFunc
	runCtx      context.Context
	cancelRun   context.CancelFunc
	// Root of every connection's context: cancelled at the end of Shutdown so handler goroutines
	// still running after the drain stop before the dependencies close.
	ctx    context.Context
	cancel context.CancelFunc

	// deliver shards push fan-out by conversation id; see enqueuePush.
	deliver     []chan message.PushPayload
	deliverOnce sync.Once

	sendBytes     atomic.Int64
	slowConsumers atomic.Int64
	rateLimited   atomic.Int64
	dropped       atomic.Int64
	pushDropped   atomic.Int64
	decodeFails   atomic.Int64
}

func New(cfg *config.Config, d Deps) *Gateway {
	ctx, cancel := context.WithCancel(context.Background())
	opsCtx, cancelOps := context.WithCancel(context.Background())
	runCtx, cancelRun := context.WithCancel(context.Background())
	deliver := lo.Times(max(cfg.Ws.DeliverWorkers, 1), func(int) chan message.PushPayload {
		return make(chan message.PushPayload, max(cfg.Ws.DeliverQueue, 1))
	})
	return &Gateway{
		cfg: cfg, deps: d, users: NewUserMap(cfg.Limits), recheck: tokenRecheck,
		ready: make(chan struct{}), presence: make(chan struct{}, 1), cleanup: make(chan struct{}, 64),
		opsCtx: opsCtx, cancelOps: cancelOps, runCtx: runCtx, cancelRun: cancelRun,
		ctx: ctx, cancel: cancel, deliver: deliver,
		upgrader: websocket.HertzUpgrader{CheckOrigin: originChecker(cfg.Ws.AllowedOrigins)},
	}
}

// originChecker enforces ws.allowed_origins. Browsers do not apply the same-origin policy to
// WebSocket, so this is the only defence against a hostile page opening a connection with a token
// the browser holds; an empty list keeps the permissive default, which is safe only while nexo
// never reads the token from a cookie. Non-browser clients send no Origin and always pass.
func originChecker(allowed []string) func(*app.RequestContext) bool {
	return func(c *app.RequestContext) bool {
		origin := string(c.GetHeader("Origin"))
		return origin == "" || len(allowed) == 0 || slices.Contains(allowed, origin)
	}
}

// Ready is closed after the first successful Bus subscription (immediately without a Bus).
func (g *Gateway) Ready() <-chan struct{} { return g.ready }

func (g *Gateway) Users() *UserMap { return g.users }

// Stats are cumulative counters since start, except Conns. Dropped counts frames dropped for one
// connection (slow client); PushDropped counts whole push events dropped because a delivery shard
// was full; DecodeFails counts unreadable bus payloads, which mean a version skew between nodes.
type Stats struct {
	Conns, SlowConsumers, RateLimited, Dropped int64
	PushDropped, DecodeFails                   int64
}

func (g *Gateway) Stats() Stats {
	return Stats{
		Conns: int64(g.users.Count()), SlowConsumers: g.slowConsumers.Load(), RateLimited: g.rateLimited.Load(),
		Dropped: g.dropped.Load(), PushDropped: g.pushDropped.Load(), DecodeFails: g.decodeFails.Load(),
	}
}

// Handle is GET /ws (design §7.1): verify → reserve capacity → upgrade → adopt → serve.
func (g *Gateway) Handle(ctx context.Context, c *app.RequestContext) {
	id, err := g.handshake(ctx, c)
	if err != nil {
		webx.FailStatus(ctx, c, handshakeStatus(err), err)
		return
	}
	// middleware.ClientIP has already applied server.trusted_proxies to this.
	ip := c.ClientIP()
	slot := Slot{UserId: id.UserId, TokenId: id.TokenId, Ip: ip}
	if err := g.users.Reserve(slot); err != nil {
		webx.FailStatus(ctx, c, handshakeStatus(err), err)
		return
	}
	connId := uuid.NewV7().String()
	// The upgrade handler runs after this handler returns (standard transporter hijack) and
	// blocks until the connection ends; it owns the slot from here, Adopt releases it on failure.
	err = g.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		cl := g.newClient(id, connId, ip, newWsConn(conn, g.cfg.Ws.MaxFrameBytes, g.cfg.Ws.PongWait))
		if err := g.users.Adopt(cl); err != nil {
			log.CtxInfo(ctx, "ws adopt rejected user=%s: %v", id.UserId, err)
			cl.Close(closeReasonServer)
			return
		}
		log.CtxInfo(ctx, "ws open conn=%s user=%s platform=%d source=%s", connId, id.UserId, id.PlatformId, id.Source)
		// ctx is only read for its log fields from here on: it is cancelled the moment Handle
		// returns, so every dependency call below takes a connection-scoped context instead.
		g.onlineAdd(cl)
		g.publishKick(cl)
		cl.Serve()
	})
	if err != nil {
		g.users.Release(slot)
		log.CtxInfo(ctx, "ws upgrade user=%s: %v", id.UserId, err)
	}
}

func (g *Gateway) handshake(ctx context.Context, c *app.RequestContext) (auth.Identity, error) {
	if g.closing.Load() {
		return auth.Identity{}, errcode.ErrNodeDraining
	}
	if enc := c.Query("encoding"); enc != "" && enc != "json" {
		return auth.Identity{}, errcode.ErrInvalidParam.WithMessage("encoding must be json")
	}
	if comp := c.Query("compression"); comp != "" && comp != "none" {
		return auth.Identity{}, errcode.ErrInvalidParam.WithMessage("compression must be none")
	}
	platform, err := strconv.Atoi(c.Query("platform_id"))
	if err != nil || platform < 1 || platform > auth.MaxPlatformId {
		return auth.Identity{}, errcode.ErrInvalidParam.WithMessage(fmt.Sprintf("platform_id must be 1..%d", auth.MaxPlatformId))
	}
	token := c.Query("token")
	if t, ok := strings.CutPrefix(string(c.GetHeader("Authorization")), "Bearer "); ok && token == "" {
		token = t
	}
	if token == "" {
		return auth.Identity{}, errcode.ErrTokenMissing
	}
	id, err := g.deps.Auth.Verify(ctx, token)
	if err != nil {
		return auth.Identity{}, webx.AuthErr(err)
	}
	// The platform is self-reported (A9); a native token's pid wins when present.
	if id.PlatformId == 0 {
		id.PlatformId = platform
	}
	return id, nil
}

const connOpTimeout = 5 * time.Second

// Shutdown bounds both existing operations and new ones by its single deadline.
// Only Remove detaches from the connection's cancellation.
func connOp(c *Client, parent context.Context) (context.Context, context.CancelFunc) {
	g := c.gw
	g.workMu.Lock()
	defer g.workMu.Unlock()
	deadline := time.Now().Add(connOpTimeout)
	if g.shutdownCtx != nil {
		if d, ok := g.shutdownCtx.Deadline(); ok {
			deadline = minTime(deadline, d)
		}
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	if g.sealed || g.opsCtx.Err() != nil || (g.shutdownCtx != nil && g.shutdownCtx.Err() != nil) {
		cancel()
		return ctx, cancel
	}
	g.work.Add(1)
	stop := context.AfterFunc(g.opsCtx, cancel)
	return ctx, func() { stop(); cancel(); g.work.Done() }
}

func minTime(a, b time.Time) time.Time { return lo.Ternary(a.Before(b), a, b) }

func (g *Gateway) beginWork() bool {
	g.workMu.Lock()
	defer g.workMu.Unlock()
	if g.sealed {
		return false
	}
	g.work.Add(1)
	return true
}

// Presence writes fail open: a missing row only affects offline push and status.
func (g *Gateway) onlineAdd(c *Client) {
	if g.deps.Online == nil {
		return
	}
	ctx, cancel := connOp(c, c.activeCtx)
	defer cancel()
	if !g.lockPresence(ctx) {
		return
	}
	defer g.unlockPresence()
	if c.activeCtx.Err() != nil {
		return
	}
	if err := g.addOnline(ctx, c); err != nil {
		log.CtxWarn(ctx, "onlinestore add conn=%s: %v", c.Id, err)
	}
}

func (g *Gateway) addOnline(ctx context.Context, c *Client) error {
	if c.onlineAdded || c.activeCtx.Err() != nil {
		return nil
	}
	if err := g.deps.Online.Add(ctx, g.cfg.NodeId, c.ref()); err != nil {
		return err
	}
	c.onlineAdded = true
	return nil
}

func (g *Gateway) onlineRemove(c *Client) {
	if g.deps.Online == nil {
		return
	}
	select {
	case g.cleanup <- struct{}{}:
	default:
		log.CtxWarn(c.ctx(), "onlinestore cleanup full conn=%s; presence expires by TTL", c.Id)
		return
	}
	ctx, cancel := connOp(c, context.WithoutCancel(c.ctx()))
	if ctx.Err() != nil {
		cancel()
		<-g.cleanup
		return
	}
	// connOp registers work before this goroutine starts so Shutdown cannot seal an empty group first.
	go func() {
		defer cancel()
		defer func() { <-g.cleanup }()
		if !g.lockPresence(ctx) {
			return
		}
		defer g.unlockPresence()
		if err := g.deps.Online.Remove(ctx, g.cfg.NodeId, c.ref()); err != nil {
			log.CtxWarn(ctx, "onlinestore remove conn=%s: %v", c.Id, err)
		}
	}()
}

func (g *Gateway) lockPresence(ctx context.Context) bool {
	select {
	case g.presence <- struct{}{}:
		if ctx.Err() == nil {
			return true
		}
		g.unlockPresence()
	case <-ctx.Done():
	}
	return false
}

func (g *Gateway) unlockPresence() { <-g.presence }

func (g *Gateway) renew(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, connOpTimeout)
	defer cancel()
	if !g.lockPresence(ctx) {
		return 0, ctx.Err()
	}
	defer g.unlockPresence()
	clients := g.users.All()
	refs := lo.FilterMap(clients, func(c *Client, _ int) (onlinestore.ConnRef, bool) {
		return c.ref(), c.activeCtx.Err() == nil
	})
	// A failed registration retry must not starve already-registered connections' heartbeats.
	if err := g.deps.Online.Renew(ctx, g.cfg.NodeId, refs); err != nil {
		return len(refs), err
	}
	// One connection that keeps failing to register must not skip every client behind it in the
	// slice; renewLoop already rate-limits the logging, so collect and report instead.
	var failed int
	var lastErr error
	for _, c := range clients {
		if err := g.addOnline(ctx, c); err != nil {
			failed, lastErr = failed+1, err
		}
	}
	if lastErr != nil {
		return len(refs), fmt.Errorf("re-add %d/%d conns: %w", failed, len(clients), lastErr)
	}
	return len(refs), nil
}

func handshakeStatus(err error) int {
	switch e := errcode.From(err); e.Code {
	case errcode.ErrInvalidParam.Code:
		return http.StatusBadRequest
	case errcode.ErrConnOverLimit.Code:
		return http.StatusTooManyRequests
	case errcode.ErrNodeDraining.Code:
		return http.StatusServiceUnavailable
	default:
		return webx.HttpStatus(e)
	}
}

func (g *Gateway) dispatch(c *Client, req Request) []byte {
	ctx := c.ctx()
	data, err := g.handle(ctx, c, req)
	if err != nil {
		if errcode.IsSystem(err) {
			log.CtxError(ctx, "ws req_id=%d: %v", req.ReqId, err)
		} else {
			log.CtxInfo(ctx, "ws req_id=%d: %v", req.ReqId, err)
		}
		return req.fail(err)
	}
	return req.reply(data)
}

func (g *Gateway) handle(ctx context.Context, c *Client, req Request) (any, error) {
	switch req.ReqId {
	case ReqGetMaxSeqs:
		var in struct {
			Cursor string `json:"cursor"`
			Limit  int    `json:"limit"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.MaxSeqs(ctx, c.UserId, in.Cursor, in.Limit, g.cfg.Limits.MaxSeqsPageMax)
	case ReqPullMsgBySeqRange:
		var in struct {
			ConversationId string `json:"conversation_id"`
			BeginSeq       int64  `json:"begin_seq"`
			EndSeq         int64  `json:"end_seq"`
			Limit          int    `json:"limit"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.Pull(ctx, message.PullInput{UserId: c.UserId, ConversationId: in.ConversationId, BeginSeq: in.BeginSeq, EndSeq: in.EndSeq, Limit: in.Limit}, g.cfg.Limits.PullPageMax)
	case ReqSendMsg:
		var in dto.SendRequest
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		return g.deps.Message.Send(ctx, message.SendInput{
			SenderId: c.UserId, SenderConnId: c.Id, ClientMsgId: in.ClientMsgId, SessionType: in.SessionType, RecvId: in.RecvId, GroupId: in.GroupId,
			ContentType: in.ContentType, Content: in.Content, SenderRead: in.SenderReadFor(c.Source),
		})
	case ReqMarkRead:
		var in struct {
			ConversationId string `json:"conversation_id"`
			ReadSeq        int64  `json:"read_seq"`
		}
		if err := bind(req, &in); err != nil {
			return nil, err
		}
		if in.ConversationId == "" {
			return nil, errcode.ErrInvalidParam.WithMessage("conversation_id is required")
		}
		seq, err := g.deps.Conv.MarkRead(ctx, c.UserId, c.Id, in.ConversationId, in.ReadSeq)
		if err != nil {
			return nil, err
		}
		return map[string]int64{"read_seq": seq}, nil
	default:
		return nil, errcode.ErrInvalidProtocol.WithMessage("unknown req_id")
	}
}

func bind(req Request, v any) error {
	if len(req.Data) == 0 {
		return nil
	}
	if err := json.Unmarshal(req.Data, v); err != nil {
		return errcode.ErrInvalidParam.Wrap(err)
	}
	return nil
}
