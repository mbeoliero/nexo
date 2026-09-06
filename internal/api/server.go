package api

import (
	"context"
	"net"
	"net/http"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/common/utils"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/mbeoliero/kit/log"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/api/middleware"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
)

type Deps struct {
	Ready       func(ctx context.Context) error // /healthz dependency probe (DB ping); nil = always ok
	Auth        auth.Authenticator
	NativeLogin bool           // registers /auth/* only when the native provider is enabled
	Internal    *auth.Internal // nil disables /api/v1/internal
	User        *user.Service
	Group       *group.Service
	Message     *message.Service
	Conv        *conversation.Service
	Ws          app.HandlerFunc // GET /ws; nil disables the gateway
}

// Register mounts everything under prefix ("" or "/im"): /healthz, /api/v1/**, /api/v1/internal/**, /ws.
// Trace and AccessLog are scoped to the group so a host's other routes are untouched.
func Register(e *route.Engine, prefix string, cfg *config.Config, d Deps) {
	root := e.Group(prefix)
	// Credentials and message content are always redacted; log.redact_paths only adds to the set.
	redact := lo.Uniq(lo.Map(append([]string{
		"/api/v1/auth/login", "/api/v1/auth/register", "/api/v1/message/send", "/api/v1/internal/message/send",
	}, cfg.Log.RedactPaths...), func(p string, _ int) string { return prefix + p }))
	skip := lo.Map(cfg.Log.SkipPaths, func(p string, _ int) string { return prefix + p })
	// ClientIP first: Trace, ProcessLogger, IpRateLimit and the WS handshake all read c.ClientIP().
	trusted, _ := cfg.Server.TrustedCIDRs() // Validate rejected malformed CIDRs at startup
	root.Use(middleware.ClientIP(trusted), middleware.Trace(),
		middleware.ProcessLogger(middleware.AccessLog{Redact: redact, Skip: skip, Body: cfg.Log.RequestBody}))
	// Registered after Use so the probe's failure log carries a trace id like every other route;
	// log.skip_paths, not the route order, is what keeps it out of the access log.
	root.GET("/healthz", healthz(cfg.NodeId, d.Ready))
	registerRoutes(root, cfg, d, trusted)
	if d.Ws != nil {
		root.GET("/ws", d.Ws)
	}
}

func registerRoutes(root *route.RouterGroup, cfg *config.Config, d Deps, trusted []*net.IPNet) {
	public := root.Group("/api/v1")
	authed := public.Group("", middleware.Bearer(d.Auth, cfg.Auth.DefaultPlatformId))

	u := userHandler{svc: d.User}
	if d.NativeLogin {
		login := public.Group("", middleware.IpRateLimit(cfg.Limits.AuthPerIpPerMin))
		login.POST("/auth/register", u.register)
		login.POST("/auth/login", u.login)
		authed.POST("/auth/logout", u.logout)
	}
	authed.GET("/user/me", u.me)
	authed.PUT("/user/me", u.updateMe)
	authed.GET("/user/info", u.info)
	authed.GET("/user/online_status", u.onlineStatus)

	g := groupHandler{svc: d.Group}
	authed.POST("/group/create", g.create)
	authed.POST("/group/join", g.join)
	authed.POST("/group/quit", g.quit)
	authed.POST("/group/kick", g.kick)
	authed.GET("/group/info", g.info)
	authed.GET("/group/members", g.members)

	msg := messageHandler{svc: d.Message, conversation: d.Conv, pullPageMax: cfg.Limits.PullPageMax, convPageMax: cfg.Limits.ConversationPageMax, maxSeqsPageMax: cfg.Limits.MaxSeqsPageMax}
	authed.POST("/message/send", msg.send)
	authed.GET("/message/pull", msg.pull)
	authed.GET("/message/max_seqs", msg.maxSeqs)
	authed.GET("/conversation/list", msg.list)
	authed.POST("/conversation/read", msg.read)
	authed.PUT("/conversation/opt", msg.opt)

	if d.Internal != nil {
		m := middleware.InternalAuth{Verifier: d.Internal, RequireTls: cfg.InternalAuth.RequireTls, TrustedProxies: trusted, DefaultPlatformId: cfg.Auth.DefaultPlatformId}
		internal := public.Group("/internal", m.Verify)
		internal.GET("/health", health)
		internal.POST("/user/upsert", u.upsert)
		internal.GET("/user/info", u.info)
		internal.GET("/user/online_status", u.onlineStatus)
		asUser := public.Group("/internal", m.AsUser)
		asUser.POST("/group/create", g.create)
		asUser.POST("/group/join", g.join)
		asUser.POST("/group/quit", g.quit)
		asUser.POST("/group/kick", g.kick)
		asUser.POST("/message/send", msg.send)
		asUser.GET("/conversation/list", msg.list)
	}
}

func health(_ context.Context, c *app.RequestContext) {
	webx.OK(c, utils.H{"status": "ok"})
}

const readyTimeout = 2 * time.Second

// healthz is the LB probe (design §12): 503 while the DB cannot be pinged so traffic drains off this node.
func healthz(nodeId string, ready func(context.Context) error) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		if ready != nil {
			ctx, cancel := context.WithTimeout(ctx, readyTimeout)
			defer cancel()
			if err := ready(ctx); err != nil {
				// The cause (which may carry host / user names from the driver) stays in the log.
				log.CtxWarn(ctx, "healthz: %v", err)
				c.JSON(http.StatusServiceUnavailable, utils.H{"status": "unavailable", "node_id": nodeId})
				return
			}
		}
		c.JSON(http.StatusOK, utils.H{"status": "ok", "node_id": nodeId})
	}
}
