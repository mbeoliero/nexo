package app

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/cloudwego/hertz/pkg/route"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mbeoliero/kit/log"
	"gorm.io/gorm"

	"github.com/mbeoliero/nexo/internal/api"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/bus"
	buslocal "github.com/mbeoliero/nexo/internal/bus/local"
	buspg "github.com/mbeoliero/nexo/internal/bus/postgres"
	busredis "github.com/mbeoliero/nexo/internal/bus/redis"
	"github.com/mbeoliero/nexo/internal/cache"
	"github.com/mbeoliero/nexo/internal/cache/local"
	"github.com/mbeoliero/nexo/internal/cache/pg"
	"github.com/mbeoliero/nexo/internal/cache/redis"
	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/gateway"
	"github.com/mbeoliero/nexo/internal/offlinepush"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	onlinedb "github.com/mbeoliero/nexo/internal/onlinestore/db"
	onlineredis "github.com/mbeoliero/nexo/internal/onlinestore/redis"
	"github.com/mbeoliero/nexo/internal/service/conversation"
	"github.com/mbeoliero/nexo/internal/service/group"
	"github.com/mbeoliero/nexo/internal/service/message"
	"github.com/mbeoliero/nexo/internal/service/user"
	"github.com/mbeoliero/nexo/internal/store"
	"github.com/mbeoliero/nexo/internal/store/gormstore"
	"github.com/mbeoliero/nexo/internal/store/pgstore"
	"github.com/mbeoliero/nexo/internal/tokenstore"
)

type App struct {
	cfg     *config.Config
	deps    api.Deps
	gw      *gateway.Gateway
	closers []func() // what Build opened, in open order; Shutdown runs them in reverse

	runErr chan error
	cancel context.CancelFunc
	// Retain the host's budget across Drain, HTTP shutdown, and Close.
	drainCtx context.Context
}

type Option func(*options)

type options struct {
	pusher offlinepush.Pusher
	auth   auth.Authenticator
	gormDb *gorm.DB
	pool   *pgxpool.Pool
}

// WithOfflinePusher injects an APNs/FCM implementation in place of the configured driver.
func WithOfflinePusher(p offlinepush.Pusher) Option { return func(o *options) { o.pusher = p } }

// WithAuthenticator replaces the configured provider chain for HTTP Bearer and the WS handshake;
// native login/logout still follow auth.providers.
func WithAuthenticator(a auth.Authenticator) Option { return func(o *options) { o.auth = a } }

// WithGormDb / WithPgxPool use a host-owned connection for the Store (design §15.1 rule 3).
func WithGormDb(db *gorm.DB) Option      { return func(o *options) { o.gormDb = db } }
func WithPgxPool(p *pgxpool.Pool) Option { return func(o *options) { o.pool = p } }

func Build(ctx context.Context, cfg *config.Config, opts ...Option) (*App, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	a := &App{cfg: cfg}
	// Past the first opened resource, every failure must close what is already open.
	fail := func(err error) (*App, error) { a.closeAll(); return nil, err }
	st, err := openStore(ctx, cfg.Db, o)
	if err != nil {
		return nil, err
	}
	a.closers = append(a.closers, func() { st.Close() })
	if err := st.Ping(ctx); err != nil {
		return fail(fmt.Errorf("app: db ping: %w", err))
	}
	c, err := openCache(ctx, cfg)
	if err != nil {
		return fail(err)
	}
	a.closers = append(a.closers, func() { c.Close() })
	tokens := tokenstore.New(c)
	chain, native := buildAuth(cfg.Auth, tokens)
	authn := cmp.Or[auth.Authenticator](o.auth, chain)
	b, closeBus, err := openBus(ctx, cfg)
	if err != nil {
		return fail(err)
	}
	a.closers = append(a.closers, closeBus)
	deps := api.Deps{Ready: st.Ping, Auth: authn, NativeLogin: native != nil, User: user.New(st, native),
		Group:   group.New(group.Adapt(st), group.NewBusNotifier(b, cfg.NodeId), cfg.Limits.GroupMaxMembers),
		Message: message.New(message.Adapt(st), message.NewBusPublisher(b, cfg.NodeId), cfg.Limits.MaxContentBytes),
		Conv:    conversation.New(st, conversation.NewBusNotifier(b, cfg.NodeId))}
	deps.Message.SetMemberCacheTtl(cfg.Limits.GroupMemberCacheTtl)
	deps.Message.SetSendRateLimit(cfg.Limits.MessageSendPerMin)
	online, closeOnline, err := openOnlineStore(ctx, cfg, st)
	if err != nil {
		return fail(err)
	}
	a.closers = append(a.closers, closeOnline)
	deps.User.SetOnlineStore(online)
	deps.Message.SetOfflinePush(online, cmp.Or(o.pusher, openPusher(cfg.OfflinePush)))
	gwDeps := gateway.Deps{Auth: authn, Bus: b, Online: online, Message: deps.Message, Conv: deps.Conv}
	if native != nil {
		gwDeps.Native = native
	}
	gw := gateway.New(cfg, gwDeps)
	deps.Ws = gw.Handle
	if ia := cfg.InternalAuth; ia.Enabled {
		deps.Internal = auth.NewInternal(ia.AllSecrets(), ia.AllowedServices, time.Duration(ia.MaxSkewSeconds)*time.Second, c)
	}
	a.deps, a.gw = deps, gw
	return a, nil
}

func (a *App) closeAll() {
	for _, close := range slices.Backward(a.closers) {
		close()
	}
	a.closers = nil
}

func (a *App) Deps() api.Deps            { return a.deps }
func (a *App) Gateway() *gateway.Gateway { return a.gw }

// Mount registers the routes on a caller-owned engine under prefix.
func (a *App) Mount(e *route.Engine, prefix string) { api.Register(e, prefix, a.cfg, a.deps) }

const (
	busReadyTimeout  = 5 * time.Second
	offlinePushDrain = 5 * time.Second
)

// Start runs the node loops (purge, renew, bus consumer) until ctx is done or Shutdown is called.
// It returns once the Bus subscription is live so nothing published before it is lost (design §10);
// after busReadyTimeout it returns anyway and the Bus keeps reconnecting in the background.
// Err reports a Bus subscription failure.
func (a *App) Start(ctx context.Context) {
	ctx, a.cancel = context.WithCancel(ctx)
	a.runErr = make(chan error, 1)
	go func() {
		err := a.gw.Run(ctx)
		if ctx.Err() != nil {
			err = nil
		}
		if err != nil {
			err = fmt.Errorf("app: bus subscribe: %w", err)
		}
		a.runErr <- err
	}()
	select {
	case <-a.gw.Ready():
	case <-ctx.Done():
	case <-time.After(busReadyTimeout):
		log.CtxWarn(ctx, "bus not subscribed after %s; serving anyway", busReadyTimeout)
	}
}

func (a *App) Err() <-chan error { return a.runErr }

// Shutdown is Drain then Close for hosts that stop their HTTP server afterwards.
func (a *App) Shutdown(ctx context.Context) error {
	drainErr := a.Drain(ctx)
	closeErr := a.close(ctx)
	return errors.Join(drainErr, closeErr, ctx.Err())
}

// Drain stops the loops, refuses new WS, sends close(1001) to every WS and purges presence
// (design §10). Dependencies stay open so in-flight HTTP handlers can finish; run it alongside
// the HTTP server's shutdown, then Close.
func (a *App) Drain(ctx context.Context) error {
	a.drainCtx = ctx
	if a.cancel != nil {
		a.cancel()
	}
	if a.gw != nil {
		return a.gw.Shutdown(ctx)
	}
	return ctx.Err()
}

// Close releases what Build opened. Call it after the HTTP server has stopped.
func (a *App) Close() {
	ctx := cmp.Or(a.drainCtx, context.Background())
	if err := a.close(ctx); err != nil {
		log.CtxWarn(ctx, "offline push drain: %v", err)
	}
}

func (a *App) close(ctx context.Context) error {
	wctx, cancel := context.WithTimeout(ctx, offlinePushDrain)
	defer cancel()
	var err error
	// HTTP may enqueue pushes after Drain; wait here while dependencies are still open.
	if a.deps.Message != nil {
		err = a.deps.Message.Wait(wctx)
	}
	a.closeAll()
	return errors.Join(err, ctx.Err())
}

func buildAuth(cfg config.AuthConfig, tokens *tokenstore.TokenStore) (auth.Chain, *auth.Native) {
	var chain auth.Chain
	var native *auth.Native
	for _, p := range cfg.Providers {
		switch p {
		case "external_jwt":
			chain = append(chain, auth.NewExternal(cfg.ExternalJwt.Secrets, cfg.ExternalJwt.DefaultRole))
		case "native":
			native = auth.NewNative(cfg.Native.Secret, time.Duration(cfg.Native.ExpireHours)*time.Hour, tokens)
			chain = append(chain, native)
		}
	}
	return chain, native
}

// openBus returns the bus and what closes it (no-op for local).
func openBus(ctx context.Context, cfg *config.Config) (bus.Bus, func(), error) {
	switch cfg.Bus.Driver {
	case "redis":
		b, err := busredis.New(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.Db)
		if err != nil {
			return nil, nil, err
		}
		return b, func() { _ = b.Close() }, nil
	case "postgres":
		b, err := buspg.New(ctx, cfg.Db.Dsn)
		if err != nil {
			return nil, nil, err
		}
		return b, b.Close, nil
	default:
		return buslocal.New(), func() {}, nil
	}
}

// openPusher returns nil for driver=noop. A non-nil no-op Pusher would still satisfy the
// `pusher != nil` guard in message.Send and pay the whole recipient/mute/visibility/presence
// fan-out on every message just to throw the result away.
func openPusher(cfg config.OfflinePushConfig) offlinepush.Pusher {
	if cfg.Driver == "webhook" {
		return offlinepush.NewWebhook(cfg.WebhookUrl, cfg.WebhookSecret, cfg.Timeout)
	}
	return nil
}

func openOnlineStore(ctx context.Context, cfg *config.Config, st store.Store) (onlinestore.OnlineStore, func(), error) {
	if cfg.OnlineStore.Driver == "redis" {
		s, err := onlineredis.New(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.Db, cfg.OnlineStore.Ttl)
		if err != nil {
			return nil, nil, err
		}
		return s, func() { _ = s.Close() }, nil
	}
	return onlinedb.New(st, cfg.OnlineStore.Ttl), func() {}, nil
}

func openCache(ctx context.Context, cfg *config.Config) (cache.Cache, error) {
	switch cfg.Cache.Driver {
	case "redis":
		return redis.New(ctx, cfg.Redis.Addr, cfg.Redis.Password, cfg.Redis.Db)
	case "pg":
		return pg.New(ctx, cfg.Db.Dsn, cfg.Cache.CleanerInterval)
	default:
		return local.New(), nil
	}
}

func openStore(ctx context.Context, cfg config.DbConfig, o options) (store.Store, error) {
	switch {
	case o.gormDb != nil && cfg.Access != "gorm":
		return nil, errors.New("app: WithGormDb requires db.access=gorm")
	case o.pool != nil && cfg.Access != "sqlc":
		return nil, errors.New("app: WithPgxPool requires db.access=sqlc")
	case o.gormDb != nil:
		return gormstore.FromDb(o.gormDb), nil
	case o.pool != nil:
		return pgstore.FromPool(o.pool), nil
	case cfg.Access == "sqlc":
		return pgstore.New(ctx, cfg.Dsn, cfg.MaxOpenConns)
	default:
		return gormstore.New(cfg.Driver, cfg.Dsn, cfg.MaxOpenConns)
	}
}
