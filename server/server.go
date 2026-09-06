// Package server embeds nexo in a Hertz host: New builds it, Mount registers the routes,
// Start runs the node loops, Shutdown drains. Standalone `nexo serve` is ListenAndServe.
package server

import (
	"context"
	"errors"
	"fmt"
	"os/signal"
	"syscall"
	"time"

	hzserver "github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mbeoliero/kit/log"
	"gorm.io/gorm"

	"github.com/mbeoliero/nexo/internal/app"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/store/migrate"
)

const (
	shutdownTimeout  = 15 * time.Second
	httpDrainTimeout = 10 * time.Second
)

type Option func(*options)

type options struct {
	app     []app.Option
	prefix  string
	dbSet   bool
	authSet bool
}

func WithOfflinePusher(p Pusher) Option {
	return func(o *options) { o.app = append(o.app, app.WithOfflinePusher(p)) }
}

// WithAuthenticator replaces the configured provider chain for Bearer and the WS handshake;
// auth.providers may then be empty (design §15.2).
func WithAuthenticator(a Authenticator) Option {
	return func(o *options) { o.app, o.authSet = append(o.app, app.WithAuthenticator(a)), true }
}

// WithGormDb / WithPgxPool reuse a host-owned connection for the Store; Shutdown leaves it open.
// bus=postgres and cache=pg still need db.dsn (design §15.1 rule 3).
// A MySQL host pool must disable CLIENT_FOUND_ROWS on every connection; otherwise duplicate
// message inserts look successful. The configured DSN cannot validate an injected pool.
func WithGormDb(db *gorm.DB) Option {
	return func(o *options) { o.app, o.dbSet = append(o.app, app.WithGormDb(db)), true }
}

func WithPgxPool(p *pgxpool.Pool) Option {
	return func(o *options) { o.app, o.dbSet = append(o.app, app.WithPgxPool(p)), true }
}

// WithRoutePrefix mounts under e.g. "/im": /im/api/v1/**, /im/ws, /im/healthz. Internal HMAC
// signs the full request path, prefix included.
func WithRoutePrefix(prefix string) Option { return func(o *options) { o.prefix = prefix } }

type Server struct {
	cfg    *Config
	app    *app.App
	prefix string
}

func DefaultConfig() *Config                  { return config.Default() }
func LoadConfig(path string) (*Config, error) { return config.Load(path) }

// LoadDbConfig reads only what Migrate needs (db.* plus NEXO_DB_DSN); the rest of the file is not validated.
func LoadDbConfig(path string) (DbConfig, error) { return config.LoadDb(path) }

func Migrate(ctx context.Context, db DbConfig) error { return migrate.Apply(ctx, db.Driver, db.Dsn) }

// New validates cfg and opens everything except the HTTP listener.
func New(ctx context.Context, cfg *Config, opts ...Option) (*Server, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	cfg.Db.Injected = o.dbSet
	cfg.Auth.Injected = o.authSet
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	a, err := app.Build(ctx, cfg, o.app...)
	if err != nil {
		return nil, fmt.Errorf("server: %w", err)
	}
	return &Server{cfg: cfg, app: a, prefix: o.prefix}, nil
}

func (s *Server) Mount(e *route.Engine) { s.app.Mount(e, s.prefix) }

// Start runs purge/renew/bus loops in the background until ctx is done or Shutdown is called.
// It returns once the Bus subscription is live (bounded wait), so mount and start listening after it.
func (s *Server) Start(ctx context.Context) { s.app.Start(ctx) }

// Err yields once: nil after a clean stop, or the Bus subscription failure. Nil before Start.
func (s *Server) Err() <-chan error { return s.app.Err() }

// Shutdown drains WS connections and closes what New opened; it does not stop the host HTTP server.
// Use Drain concurrently with the host's shutdown, await both, then Close so in-flight HTTP
// handlers retain access to dependencies until the host has stopped.
func (s *Server) Shutdown(ctx context.Context) error { return s.app.Shutdown(ctx) }

// Drain refuses new WS, closes the existing ones with 1001 and purges presence; dependencies stay open.
func (s *Server) Drain(ctx context.Context) error { return s.app.Drain(ctx) }

// Close releases the store, cache, bus and online store opened by New. Call after the host server stopped.
func (s *Server) Close() { s.app.Close() }

func (s *Server) User() *UserService                 { return s.app.Deps().User }
func (s *Server) Group() *GroupService               { return s.app.Deps().Group }
func (s *Server) Message() *MessageService           { return s.app.Deps().Message }
func (s *Server) Conversation() *ConversationService { return s.app.Deps().Conv }
func (s *Server) Stats() GatewayStats                { return s.app.Gateway().Stats() }

// Kick closes this node's connections of userId on platformId except keepTokenId; multi-node
// kicks go through the Bus when a new login happens.
func (s *Server) Kick(userId string, platformId int, keepTokenId string) {
	s.app.Gateway().Kick(userId, platformId, keepTokenId)
}

// newHertz is the standalone server; embedding hosts bring their own engine.
func newHertz(addr string) *hzserver.Hertz {
	return hzserver.Default(
		hzserver.WithHostPorts(addr),
		// WS upgrade needs Hijack; netpoll lacks it.
		hzserver.WithTransport(standard.NewTransporter),
		// In-flight HTTP handlers get up to this long after the listener closes (design §10: ≤10s).
		hzserver.WithExitWaitTime(httpDrainTimeout),
	)
}

// logShutdown reports a failed shutdown on the error paths, where the caller is already returning
// the failure that triggered it and cannot return this one too.
func logShutdown(ctx context.Context, err error) {
	if err != nil {
		log.CtxWarn(ctx, "shutdown after failure: %v", err)
	}
}

// ListenAndServe is the standalone node: own Hertz on cfg.Server.Addr, stop on ctx or SIGINT/SIGTERM.
func ListenAndServe(ctx context.Context, cfg *Config, opts ...Option) error {
	log.WithHertz()
	log.CtxInfo(ctx, "nexo starting node_id=%s\n--- effective config ---\n%s", cfg.NodeId, cfg.EffectiveYAML())
	s, err := New(ctx, cfg, opts...)
	if err != nil {
		return err
	}
	log.CtxInfo(ctx, "db connected driver=%s access=%s", cfg.Db.Driver, cfg.Db.Access)

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	h := newHertz(cfg.Server.Addr)
	s.Mount(h.Engine)
	s.Start(ctx)
	httpErr := make(chan error, 1)
	go func() { httpErr <- h.Run() }()
	log.CtxInfo(ctx, "http listening on %s", cfg.Server.Addr)

	select {
	case err := <-httpErr:
		logShutdown(ctx, s.Shutdown(context.Background()))
		return fmt.Errorf("server: http: %w", err)
	case err := <-s.Err():
		if err != nil {
			logShutdown(ctx, s.Shutdown(context.Background()))
			return err
		}
	case <-ctx.Done():
	}

	log.CtxInfo(ctx, "shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	// Order (design §10): stop accepting (Hertz closes the listener at once, then waits for in-flight
	// handlers) while the gateway sends close(1001) to every WS and purges presence. Hertz would cut
	// hijacked WS sockets itself after a moment, so the drain runs concurrently, not after. Dependencies
	// close only once both are done.
	drained := make(chan error, 1)
	go func() { drained <- s.Drain(sctx) }()
	httpShutdownErr := h.Shutdown(sctx)
	if err := <-drained; err != nil {
		log.CtxWarn(sctx, "ws drain: %v", err)
	}
	s.Close()
	if httpShutdownErr != nil && !errors.Is(httpShutdownErr, context.DeadlineExceeded) {
		return fmt.Errorf("server: shutdown: %w", httpShutdownErr)
	}
	return nil
}
