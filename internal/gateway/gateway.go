package gateway

import (
	"context"
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
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
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
	User    *user.Service
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
	// Node-wide kick arbitration; shard by user if contention is measured.
	// Held only for lifecycle state, never UserMap access, sends or network I/O.
	kickMu sync.Mutex
	// Node-wide presence lock caps throughput at store latency; shard by user if needed.
	presence  chan struct{}
	cleanup   chan struct{} // bounds active/waiting Remove tasks; overflow expires by presence TTL
	work      *workGroup
	budget    *sendBudget
	runCtx    context.Context
	cancelRun context.CancelFunc
	// Root of every connection's context: cancelled at the end of Shutdown so handler goroutines
	// still running after the drain stop before the dependencies close.
	ctx    context.Context
	cancel context.CancelFunc

	// deliver shards push fan-out by conversation id; see enqueuePush.
	deliver     []chan message.PushPayload
	deliverOnce sync.Once

	pushDropped        atomic.Int64
	decodeFails        atomic.Int64
	onlineSubs         *onlineSubscriptions
	onlineRefreshes    atomic.Int64
	onlineReadFails    atomic.Int64
	onlinePushDropped  atomic.Int64
	onlinePublishFails atomic.Int64
}

func New(cfg *config.Config, d Deps) *Gateway {
	ctx, cancel := context.WithCancel(context.Background())
	runCtx, cancelRun := context.WithCancel(context.Background())
	deliver := lo.Times(max(cfg.Ws.DeliverWorkers, 1), func(int) chan message.PushPayload {
		return make(chan message.PushPayload, max(cfg.Ws.DeliverQueue, 1))
	})
	return &Gateway{
		cfg: cfg, deps: d, users: NewUserMap(cfg.Limits), recheck: tokenRecheck,
		ready: make(chan struct{}), presence: make(chan struct{}, 1), cleanup: make(chan struct{}, 64),
		work: newWorkGroup(), budget: newSendBudget(cfg.Limits.WsSendBytesTotal),
		runCtx: runCtx, cancelRun: cancelRun,
		ctx: ctx, cancel: cancel, deliver: deliver,
		onlineSubs: newOnlineSubscriptions(),
		upgrader:   websocket.HertzUpgrader{CheckOrigin: originChecker(cfg.Ws.AllowedOrigins)},
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

// Stats are cumulative counters since start, except Conns and OnlineSubscriptionRefs.
// Dropped counts frames dropped for one connection; PushDropped counts whole push events dropped because a
// delivery shard was full; DecodeFails counts unreadable bus payloads, which mean a version skew between nodes.
type Stats struct {
	Conns, SlowConsumers, RateLimited, Dropped int64
	PushDropped, DecodeFails                   int64

	OnlineSubscriptionRefs int64
	OnlineRefreshes        int64
	OnlineReadFails        int64
	OnlinePushDropped      int64
	OnlinePublishFails     int64

	// App fills these from its message service and its Bus; Gateway.Stats alone leaves them
	// zero, which for the Bus pair reads as "no counter was taken", not as a healthy bus.
	MessageRepublishAttempts int64
	// BusDegradedPublishes counts what the configured Bus driver knew it failed to deliver
	// (design §6.1); its weighting is per driver, so it is a failure signal, not a loss rate.
	// BusDegradedPublishesAvailable is false when the driver has no receiver signal at all
	// (PG NOTIFY): the count is then meaningless and must not be charted as zero degradation.
	BusDegradedPublishes          int64
	BusDegradedPublishesAvailable bool
}

func (g *Gateway) Stats() Stats {
	b := g.budget
	return Stats{
		Conns: int64(g.users.Count()), SlowConsumers: b.slowConsumers.Load(), RateLimited: b.rateLimited.Load(),
		Dropped: b.dropped.Load(), PushDropped: g.pushDropped.Load(), DecodeFails: g.decodeFails.Load(),
		OnlineSubscriptionRefs: g.onlineSubscriptionRefs(), OnlineRefreshes: g.onlineRefreshes.Load(),
		OnlineReadFails: g.onlineReadFails.Load(), OnlinePushDropped: g.onlinePushDropped.Load(),
		OnlinePublishFails: g.onlinePublishFails.Load(),
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
	reservation := g.watchUpgrade(c.Finished(), slot)
	// A reservation returned before the upgrade means the node started draining after handshake's
	// check — Shutdown cancelled the run context or sealed the work group: answer the documented
	// 503 instead of upgrading into a close the client can only read as 1006.
	if reservation.settledEarly() {
		webx.FailStatus(ctx, c, handshakeStatus(errcode.ErrNodeDraining), errcode.ErrNodeDraining)
		return
	}
	connId := uuid.NewV7().String()
	// Hertz calls this only after writing HTTP 101. Until then HTTP completion or shutdown
	// returns the reservation; once claimed, Adopt owns its release, including on failure.
	err = g.upgrader.Upgrade(c, func(conn *websocket.Conn) {
		if !reservation.claim() {
			_ = conn.Close()
			return
		}
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
		reservation.release()
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

// Presence writes fail open: a missing row only affects offline push and status.
func (g *Gateway) onlineAdd(c *Client) {
	if g.deps.Online == nil {
		return
	}
	ctx, cancel := g.work.op(c.activeCtx)
	defer cancel()
	var err error
	users, _ := g.withPresenceLock(ctx, func() []string {
		if c.activeCtx.Err() != nil {
			return nil
		}
		var added bool
		if added, err = g.addOnline(ctx, c); !added {
			return nil
		}
		return []string{c.UserId}
	})
	if err != nil {
		log.CtxWarn(ctx, "onlinestore add conn=%s: %v", c.Id, err)
		return
	}
	if len(users) == 0 {
		return
	}
	// The registration is durably in the store, so announcing it must outlive the socket: a duplicate
	// login kick or a drain cancelling c.activeCtx right after the write would otherwise abort the
	// publish, and peers would never learn the user came online (design §7.4). Remove detaches for the
	// same reason. Only the publish detaches: the checks above still read c.activeCtx, so no write
	// escapes a kick, and work.op keeps the shutdown deadline over both.
	pubCtx, pubCancel := g.work.op(context.WithoutCancel(c.ctx()))
	defer pubCancel()
	g.publishOnlineChanged(pubCtx, users)
}

// addOnline reports whether this call created the registration. A connection that is already
// registered, or that lost its context to a kick between the two checks, wrote nothing, and a write
// that did not happen must not be published as a presence change (design §7.4).
func (g *Gateway) addOnline(ctx context.Context, c *Client) (bool, error) {
	if c.onlineAdded || c.activeCtx.Err() != nil {
		return false, nil
	}
	if err := g.deps.Online.Add(ctx, g.cfg.NodeId, c.ref()); err != nil {
		return false, err
	}
	c.onlineAdded = true
	return true, nil
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
	// Remove alone detaches from the connection's cancellation: the row must go although the socket
	// is gone. op registers work before this goroutine starts so Shutdown cannot seal an empty group first.
	ctx, cancel := g.work.op(context.WithoutCancel(c.ctx()))
	if ctx.Err() != nil {
		cancel()
		<-g.cleanup
		return
	}
	go func() {
		defer cancel()
		defer func() { <-g.cleanup }()
		var err error
		users, _ := g.withPresenceLock(ctx, func() []string {
			// Remove runs and publishes even when this connection never registered locally: Renew ZAdds
			// every live ref, so a connection whose Add failed can still own a row (design §7.4).
			if err = g.deps.Online.Remove(ctx, g.cfg.NodeId, c.ref()); err != nil {
				return nil
			}
			c.onlineAdded = false
			return []string{c.UserId}
		})
		if err != nil {
			log.CtxWarn(ctx, "onlinestore remove conn=%s: %v", c.Id, err)
			return
		}
		g.publishOnlineChanged(ctx, users)
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

// withPresenceLock runs fn under the node-wide presence lock and hands back the users whose presence
// changed, so the caller publishes outside the lock; false means the lock was never taken because ctx
// ended. The unlock is deferred rather than written at each exit because the region does OnlineStore
// I/O and walks client state other goroutines mutate: a panic escaping an explicit unlock, or a later
// early return past it, would leave the cap-1 semaphore full for good, and the node would go on
// serving sockets while never registering or removing presence again.
func (g *Gateway) withPresenceLock(ctx context.Context, fn func() []string) ([]string, bool) {
	if !g.lockPresence(ctx) {
		return nil, false
	}
	defer g.unlockPresence()
	return fn(), true
}

func (g *Gateway) renew(ctx context.Context) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, connOpTimeout)
	defer cancel()
	var (
		clients  []*Client
		refs     []onlinestore.ConnRef
		failed   int
		renewErr error
		lastErr  error
	)
	users, locked := g.withPresenceLock(ctx, func() []string {
		clients = g.users.All()
		refs = lo.FilterMap(clients, func(c *Client, _ int) (onlinestore.ConnRef, bool) {
			return c.ref(), c.activeCtx.Err() == nil
		})
		// A failed registration retry must not starve already-registered connections' heartbeats.
		var revived []onlinestore.ConnRef
		if revived, renewErr = g.deps.Online.Renew(ctx, g.cfg.NodeId, refs); renewErr != nil {
			return nil
		}
		// Only registrations this tick actually created are a presence change. Publishing every live
		// ref instead would mark every matching subscription dirty on this node and on every peer on
		// every heartbeat, dragging refreshes from the snapshot period down to the minimum read
		// interval for nothing; a steady-state node therefore publishes nothing (design §7.4).
		changed := lo.Map(revived, func(ref onlinestore.ConnRef, _ int) string { return ref.UserId })
		// One connection that keeps failing to register must not skip every client behind it in the
		// slice; renewLoop already rate-limits the logging, so collect and report instead.
		for _, c := range clients {
			added, err := g.addOnline(ctx, c)
			switch {
			case err != nil:
				failed, lastErr = failed+1, err
			case added:
				changed = append(changed, c.UserId)
			}
		}
		return changed
	})
	if !locked {
		return 0, ctx.Err()
	}
	if renewErr != nil {
		return len(refs), renewErr
	}
	g.publishOnlineChanged(ctx, users)
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
