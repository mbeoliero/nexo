// Every field must be read by code; config.example.yaml mirrors this struct.
package config

import (
	"cmp"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/samber/lo"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v3"
)

const (
	EnvPrefix         = "NEXO"
	placeholderSecret = "change-me"
	mysqlTextMax      = 65535
	// HS256 signing keys shorter than the 256-bit hash are the weak link (RFC 7518 §3.2).
	minSecretLen = 32
)

// devSecrets are the values that ship in this repo's configs, Makefile and docs. A node that
// boots with one of them signs tokens anybody can forge, so Validate rejects them by name in
// addition to the length rule.
var devSecrets = []string{placeholderSecret, "compose-dev-secret", "dev-only-secret-not-for-production"}

// weakSecret reports whether s is unusable as an HMAC key: too short, or a published placeholder.
func weakSecret(s string) bool { return len(s) < minSecretLen || slices.Contains(devSecrets, s) }

type Config struct {
	NodeId       string             `mapstructure:"node_id" yaml:"node_id"`
	Server       ServerConfig       `mapstructure:"server" yaml:"server"`
	Db           DbConfig           `mapstructure:"db" yaml:"db"`
	Redis        RedisConfig        `mapstructure:"redis" yaml:"redis"`
	Bus          BusConfig          `mapstructure:"bus" yaml:"bus"`
	OnlineStore  OnlineStoreConfig  `mapstructure:"online_store" yaml:"online_store"`
	Cache        CacheConfig        `mapstructure:"cache" yaml:"cache"`
	OfflinePush  OfflinePushConfig  `mapstructure:"offline_push" yaml:"offline_push"`
	Auth         AuthConfig         `mapstructure:"auth" yaml:"auth"`
	InternalAuth InternalAuthConfig `mapstructure:"internal_auth" yaml:"internal_auth"`
	Ws           WsConfig           `mapstructure:"ws" yaml:"ws"`
	Limits       LimitsConfig       `mapstructure:"limits" yaml:"limits"`
	Log          LogConfig          `mapstructure:"log" yaml:"log"`
}

type ServerConfig struct {
	Addr string `mapstructure:"addr" yaml:"addr"`
	// TrustedProxies are the CIDRs whose X-Forwarded-For / X-Real-IP this node honours. Empty —
	// the default — trusts nobody: every per-IP limit then keys on the socket peer address, which
	// a client cannot spoof. Set it to the load balancer's network when one sits in front.
	TrustedProxies []string `mapstructure:"trusted_proxies" yaml:"trusted_proxies"`
}

// TrustedCIDRs parses TrustedProxies for hertz's ClientIPOptions. Validate has already rejected
// malformed entries, so callers past startup can ignore the error.
func (c ServerConfig) TrustedCIDRs() ([]*net.IPNet, error) {
	var out []*net.IPNet
	for _, raw := range c.TrustedProxies {
		_, n, err := net.ParseCIDR(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("server.trusted_proxies: %q is not a CIDR: %w", raw, err)
		}
		out = append(out, n)
	}
	return out, nil
}

type DbConfig struct {
	Driver       string `mapstructure:"driver" yaml:"driver"` // postgres | mysql
	Access       string `mapstructure:"access" yaml:"access"` // gorm | sqlc
	Dsn          string `mapstructure:"dsn" yaml:"dsn"`
	MaxOpenConns int    `mapstructure:"max_open_conns" yaml:"max_open_conns"`
	// Injected is set by server.New when the host supplies its own connection (design §15.1);
	// dsn is then only needed by bus=postgres and cache=pg, which open their own connections.
	Injected bool `mapstructure:"-" yaml:"-"`
}

type RedisConfig struct {
	Addr     string `mapstructure:"addr" yaml:"addr"`
	Password string `mapstructure:"password" yaml:"password"`
	Db       int    `mapstructure:"db" yaml:"db"`
}

func (r RedisConfig) Enabled() bool { return r.Addr != "" }

type BusConfig struct {
	Driver string `mapstructure:"driver" yaml:"driver"` // redis | postgres | local
}

type OnlineStoreConfig struct {
	Driver        string        `mapstructure:"driver" yaml:"driver"` // db | redis
	Ttl           time.Duration `mapstructure:"ttl" yaml:"ttl"`
	RenewInterval time.Duration `mapstructure:"renew_interval" yaml:"renew_interval"`
}

type CacheConfig struct {
	Driver          string        `mapstructure:"driver" yaml:"driver"` // redis | pg | local
	CleanerInterval time.Duration `mapstructure:"cleaner_interval" yaml:"cleaner_interval"`
}

type OfflinePushConfig struct {
	Driver        string        `mapstructure:"driver" yaml:"driver"` // noop | webhook
	WebhookUrl    string        `mapstructure:"webhook_url" yaml:"webhook_url"`
	WebhookSecret string        `mapstructure:"webhook_secret" yaml:"webhook_secret"`
	Timeout       time.Duration `mapstructure:"timeout" yaml:"timeout"`
}

type AuthConfig struct {
	Providers []string `mapstructure:"providers" yaml:"providers"` // external_jwt | native
	// Injected is set by server.New when the host passes WithAuthenticator: providers may then be
	// empty (no built-in login); a listed provider is still validated because native login uses it.
	Injected          bool              `mapstructure:"-" yaml:"-"`
	DefaultPlatformId int               `mapstructure:"default_platform_id" yaml:"default_platform_id"`
	ExternalJwt       ExternalJwtConfig `mapstructure:"external_jwt" yaml:"external_jwt"`
	Native            NativeAuthConfig  `mapstructure:"native" yaml:"native"`
}

type ExternalJwtConfig struct {
	Secrets     []string `mapstructure:"secrets" yaml:"secrets"`
	DefaultRole string   `mapstructure:"default_role" yaml:"default_role"`
}

type NativeAuthConfig struct {
	Secret      string `mapstructure:"secret" yaml:"secret"`
	ExpireHours int    `mapstructure:"expire_hours" yaml:"expire_hours"`
}

type InternalAuthConfig struct {
	Enabled bool `mapstructure:"enabled" yaml:"enabled"`
	// Secret is the single-key form; Secrets allows rotation (old + new accepted, design §11).
	// AllSecrets merges them; at least one is required when enabled.
	Secret          string   `mapstructure:"secret" yaml:"secret"`
	Secrets         []string `mapstructure:"secrets" yaml:"secrets"`
	AllowedServices []string `mapstructure:"allowed_services" yaml:"allowed_services"`
	MaxSkewSeconds  int      `mapstructure:"max_skew_seconds" yaml:"max_skew_seconds"`
	RequireTls      bool     `mapstructure:"require_tls" yaml:"require_tls"`
}

type WsConfig struct {
	MaxFrameBytes int           `mapstructure:"max_frame_bytes" yaml:"max_frame_bytes"`
	SendQueue     int           `mapstructure:"send_queue" yaml:"send_queue"`
	PingInterval  time.Duration `mapstructure:"ping_interval" yaml:"ping_interval"`
	PongWait      time.Duration `mapstructure:"pong_wait" yaml:"pong_wait"`
	// AllowedOrigins restricts the WS handshake by Origin. Empty — the default — accepts every
	// origin, which browsers do not police for WebSocket: keep it empty only while the token is
	// never ambient (nexo takes it from the query string or the Authorization header, never a
	// cookie). A request without an Origin header (SDK, mobile) is always accepted.
	AllowedOrigins []string `mapstructure:"allowed_origins" yaml:"allowed_origins"`
	// DeliverWorkers shards push fan-out by conversation id, so one slow recipient lookup cannot
	// stall unrelated conversations, kicks or read receipts. DeliverQueue is the per-shard backlog;
	// a full shard drops the event (at-most-once, design §6.1) and counts it in Stats.
	DeliverWorkers int `mapstructure:"deliver_workers" yaml:"deliver_workers"`
	DeliverQueue   int `mapstructure:"deliver_queue" yaml:"deliver_queue"`
}

type LimitsConfig struct {
	MaxContentBytes     int           `mapstructure:"max_content_bytes" yaml:"max_content_bytes"`
	PullPageMax         int           `mapstructure:"pull_page_max" yaml:"pull_page_max"`
	ConversationPageMax int           `mapstructure:"conversation_page_max" yaml:"conversation_page_max"`
	MaxSeqsPageMax      int           `mapstructure:"max_seqs_page_max" yaml:"max_seqs_page_max"`
	GroupMaxMembers     int           `mapstructure:"group_max_members" yaml:"group_max_members"`
	GroupMemberCacheTtl time.Duration `mapstructure:"group_member_cache_ttl" yaml:"group_member_cache_ttl"`
	WsConnsPerUser      int           `mapstructure:"ws_conns_per_user" yaml:"ws_conns_per_user"`
	WsConnsPerToken     int           `mapstructure:"ws_conns_per_token" yaml:"ws_conns_per_token"`
	WsConnsPerIp        int           `mapstructure:"ws_conns_per_ip" yaml:"ws_conns_per_ip"`
	WsConnsTotal        int           `mapstructure:"ws_conns_total" yaml:"ws_conns_total"`
	WsFramesPerSec      int           `mapstructure:"ws_frames_per_sec" yaml:"ws_frames_per_sec"`
	WsInflightPerConn   int           `mapstructure:"ws_inflight_per_conn" yaml:"ws_inflight_per_conn"`
	MessageSendPerMin   int           `mapstructure:"message_send_per_min" yaml:"message_send_per_min"`
	WsSendBytesTotal    int64         `mapstructure:"ws_send_bytes_total" yaml:"ws_send_bytes_total"`
	// AuthPerIpPerMin throttles /auth/register and /auth/login per client IP (bcrypt is expensive);
	// 0 disables it, which is the default because the LB usually does this (deploy/nginx.conf).
	AuthPerIpPerMin int `mapstructure:"auth_per_ip_per_min" yaml:"auth_per_ip_per_min"`
}

type LogConfig struct {
	RedactPaths []string `mapstructure:"redact_paths" yaml:"redact_paths"`
	// SkipPaths write no access log line at all; the LB health probe would otherwise log once per
	// probe per node.
	SkipPaths []string `mapstructure:"skip_paths" yaml:"skip_paths"`
	// RequestBody logs the first 512 bytes of each request body. Off by default because on an IM
	// server those bodies carry message content; RedactPaths are omitted even when it is on.
	RequestBody bool `mapstructure:"request_body" yaml:"request_body"`
}

// Env overrides: NEXO_DB_DSN, NEXO_AUTH_NATIVE_SECRET.
func Load(path string) (*Config, error) {
	cfg, err := decode(path, true)
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadDb is Load for `nexo migrate`: only the db section is validated, so a file that carries
// nothing but db.* (or env NEXO_DB_DSN alone) is enough.
func LoadDb(path string) (DbConfig, error) {
	cfg, err := decode(path, true)
	if err != nil {
		return DbConfig{}, err
	}
	var errs []error
	cfg.Db.validate(func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) })
	if len(errs) > 0 {
		return DbConfig{}, fmt.Errorf("config: invalid: %w", errors.Join(errs...))
	}
	return cfg.Db, nil
}

// Default returns the defaults (no file, no env) without validating; embedding hosts fill it in and
// server.New validates. NodeId is the hostname.
func Default() *Config {
	cfg, err := decode("", false)
	if err != nil {
		panic("config: defaults: " + err.Error())
	}
	return cfg
}

func decode(path string, env bool) (*Config, error) {
	v := viper.New()
	setDefaults(v)
	if env {
		v.SetEnvPrefix(EnvPrefix)
		v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
		// AutomaticEnv alone leaves keys without a default or file value invisible to Unmarshal
		// (db.dsn, auth.native.secret, node_id); binding every key makes env-only deployments work.
		for _, k := range keys(reflect.TypeFor[Config](), "") {
			_ = v.BindEnv(k)
		}
	}
	if path != "" {
		v.SetConfigFile(path)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("config: read %s: %w", path, err)
		}
	}
	var cfg Config
	if err := v.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("config: decode: %w", err)
	}
	host, err := os.Hostname()
	if err != nil && cfg.NodeId == "" {
		return nil, fmt.Errorf("config: node_id empty and hostname unavailable: %w", err)
	}
	cfg.NodeId = cmp.Or(cfg.NodeId, host)
	return &cfg, nil
}

// keys lists every mapstructure path of t, e.g. "auth.native.secret".
func keys(t reflect.Type, prefix string) []string {
	var out []string
	for i := range t.NumField() {
		f := t.Field(i)
		tag, _, _ := strings.Cut(f.Tag.Get("mapstructure"), ",")
		if tag == "" || tag == "-" {
			continue
		}
		key := lo.Ternary(prefix == "", tag, prefix+"."+tag)
		if f.Type.Kind() == reflect.Struct {
			out = append(out, keys(f.Type, key)...)
			continue
		}
		out = append(out, key)
	}
	return out
}

func setDefaults(v *viper.Viper) {
	v.SetDefault("server.addr", ":8080")
	v.SetDefault("db.driver", "postgres")
	v.SetDefault("db.access", "sqlc")
	v.SetDefault("db.max_open_conns", 30)
	v.SetDefault("bus.driver", "local")
	v.SetDefault("online_store.driver", "db")
	v.SetDefault("online_store.ttl", "60s")
	v.SetDefault("online_store.renew_interval", "20s")
	v.SetDefault("cache.driver", "local")
	v.SetDefault("cache.cleaner_interval", "1m")
	v.SetDefault("offline_push.driver", "noop")
	v.SetDefault("offline_push.timeout", "3s")
	v.SetDefault("auth.providers", []string{"native"})
	v.SetDefault("auth.default_platform_id", 5)
	v.SetDefault("auth.external_jwt.default_role", "user")
	v.SetDefault("auth.native.expire_hours", 168)
	v.SetDefault("internal_auth.enabled", false)
	v.SetDefault("internal_auth.max_skew_seconds", 300)
	v.SetDefault("internal_auth.require_tls", true)
	v.SetDefault("ws.max_frame_bytes", 65536)
	v.SetDefault("ws.send_queue", 256)
	v.SetDefault("ws.ping_interval", "30s")
	v.SetDefault("ws.pong_wait", "75s")
	v.SetDefault("ws.allowed_origins", []string{})
	v.SetDefault("ws.deliver_workers", 8)
	v.SetDefault("ws.deliver_queue", 1024)
	v.SetDefault("limits.max_content_bytes", 8192)
	v.SetDefault("limits.pull_page_max", 100)
	v.SetDefault("limits.conversation_page_max", 100)
	v.SetDefault("limits.max_seqs_page_max", 200)
	v.SetDefault("limits.group_max_members", 500)
	v.SetDefault("limits.group_member_cache_ttl", "10s")
	v.SetDefault("limits.ws_conns_per_user", 10)
	v.SetDefault("limits.ws_conns_per_token", 3)
	v.SetDefault("limits.ws_conns_per_ip", 50)
	v.SetDefault("limits.ws_conns_total", 20000)
	v.SetDefault("limits.ws_frames_per_sec", 20)
	v.SetDefault("limits.ws_inflight_per_conn", 8)
	v.SetDefault("limits.message_send_per_min", 120)
	v.SetDefault("limits.ws_send_bytes_total", int64(512<<20))
	v.SetDefault("log.redact_paths", []string{"/api/v1/auth/login", "/api/v1/auth/register"})
	v.SetDefault("log.skip_paths", []string{"/healthz"})
	v.SetDefault("log.request_body", false)
}

// AllSecrets is secrets plus the single secret, newest-first order as configured, without empties.
func (c InternalAuthConfig) AllSecrets() []string {
	return lo.Filter(append(slices.Clone(c.Secrets), c.Secret), func(s string, _ int) bool { return s != "" })
}

// validate covers what both `serve` and `migrate` need from the db section.
func (d DbConfig) validate(fail func(string, ...any)) {
	if !slices.Contains([]string{"postgres", "mysql"}, d.Driver) {
		fail("db.driver=%q, want one of [postgres mysql]", d.Driver)
	}
	if !slices.Contains([]string{"gorm", "sqlc"}, d.Access) {
		fail("db.access=%q, want one of [gorm sqlc]", d.Access)
	}
	if d.Dsn == "" && !d.Injected {
		fail("db.dsn is required")
	}
	if d.Driver == "mysql" && d.Access != "gorm" {
		fail("db.driver=mysql requires db.access=gorm")
	}
	if d.MaxOpenConns <= 0 {
		fail("db.max_open_conns must be > 0")
	}
	// With CLIENT_FOUND_ROWS the no-op branch of INSERT ... ON DUPLICATE KEY reports a matched row,
	// which InsertMessage would read as a fresh insert. Without parseTime the driver hands back
	// datetime(3) as []byte and every time.Time scan fails at query time, long after boot.
	if d.Driver == "mysql" && d.Dsn != "" {
		parsed, err := mysql.ParseDSN(d.Dsn)
		if err != nil {
			// Driver errors can contain credentials or parameter values.
			fail("db.dsn is not a valid mysql DSN")
		} else if parsed.ClientFoundRows {
			fail("db.dsn must not enable clientFoundRows")
		} else if !parsed.ParseTime {
			fail("db.dsn must set parseTime=true")
		}
	}
}

// Combination rules: docs/design.md §11.
func (c *Config) Validate() error {
	var errs []error
	fail := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }
	oneOf := func(field, val string, allowed ...string) {
		if !slices.Contains(allowed, val) {
			fail("%s=%q, want one of %v", field, val, allowed)
		}
	}

	if c.Server.Addr == "" {
		fail("server.addr is required")
	}
	if _, err := c.Server.TrustedCIDRs(); err != nil {
		fail("%v", err)
	}
	c.Db.validate(fail)
	if c.Db.Dsn == "" && c.Db.Injected && (c.Bus.Driver == "postgres" || c.Cache.Driver == "pg") {
		fail("bus.driver=postgres and cache.driver=pg open their own connections: db.dsn is required even with an injected db")
	}
	oneOf("bus.driver", c.Bus.Driver, "redis", "postgres", "local")
	oneOf("online_store.driver", c.OnlineStore.Driver, "db", "redis")
	oneOf("cache.driver", c.Cache.Driver, "redis", "pg", "local")
	oneOf("offline_push.driver", c.OfflinePush.Driver, "noop", "webhook")

	if c.Bus.Driver == "postgres" && c.Db.Driver != "postgres" {
		fail("bus.driver=postgres requires db.driver=postgres")
	}
	if c.Cache.Driver == "pg" && c.Db.Driver != "postgres" {
		fail("cache.driver=pg requires db.driver=postgres")
	}
	needsRedis := c.Bus.Driver == "redis" || c.OnlineStore.Driver == "redis" || c.Cache.Driver == "redis"
	if needsRedis && !c.Redis.Enabled() {
		fail("bus/online_store/cache=redis requires redis.addr")
	}
	// A non-local bus is the only signal in this file that more than one node is running, and a
	// local cache then means node-local security state: TokenStore.Check only knows the tokens
	// its own process issued, and internal_auth dedupes nonces within one process, so a captured
	// request replays freely against every other node (design §6.3).
	if c.Bus.Driver != "local" && c.Cache.Driver == "local" {
		if slices.Contains(c.Auth.Providers, "native") {
			fail("cache.driver=local with bus.driver=%s (multi-node): native tokens are only valid on the node that issued them; use cache.driver=redis or pg", c.Bus.Driver)
		}
		if c.InternalAuth.Enabled {
			fail("cache.driver=local with bus.driver=%s (multi-node): internal_auth nonce replay protection does not span nodes; use cache.driver=redis or pg", c.Bus.Driver)
		}
	}
	// <= 0 disables the sweeper; anything positive must leave room for the cleaner's jitter window.
	if iv := c.Cache.CleanerInterval; iv > 0 && iv < time.Second {
		fail("cache.cleaner_interval must be 0 (disabled) or >= 1s, got %s", iv)
	}
	if c.OnlineStore.Ttl <= 0 || c.OnlineStore.RenewInterval <= 0 || c.OnlineStore.RenewInterval >= c.OnlineStore.Ttl {
		fail("online_store: need 0 < renew_interval < ttl")
	}
	if c.OfflinePush.Driver == "webhook" {
		if !strings.HasPrefix(c.OfflinePush.WebhookUrl, "https://") {
			fail("offline_push.webhook_url must start with https://")
		}
		if c.OfflinePush.WebhookSecret == "" {
			fail("offline_push.webhook_secret is required for webhook driver")
		}
	}

	if len(c.Auth.Providers) == 0 && !c.Auth.Injected {
		fail("auth.providers must not be empty")
	}
	secrets := map[string]string{}
	for _, p := range c.Auth.Providers {
		switch p {
		case "external_jwt":
			if len(c.Auth.ExternalJwt.Secrets) == 0 {
				fail("auth.external_jwt.secrets is required")
			}
			for _, s := range c.Auth.ExternalJwt.Secrets {
				if weakSecret(s) {
					fail("auth.external_jwt.secrets contains a published placeholder or a secret shorter than %d chars", minSecretLen)
				} else if prev, dup := secrets[s]; dup {
					fail("auth: %s shares a secret with %s", p, prev)
				}
				secrets[s] = p
			}
		case "native":
			switch s := c.Auth.Native.Secret; {
			case s == "":
				fail("auth.native.secret is required")
			case weakSecret(s):
				fail("auth.native.secret is a published placeholder or shorter than %d chars; set a real secret (env NEXO_AUTH_NATIVE_SECRET)", minSecretLen)
			default:
				if prev, dup := secrets[s]; dup {
					fail("auth: %s shares a secret with %s", p, prev)
				}
				secrets[s] = p
			}
			if c.Auth.Native.ExpireHours <= 0 {
				fail("auth.native.expire_hours must be > 0")
			}
		default:
			fail("auth.providers: unknown provider %q", p)
		}
	}
	if c.Auth.DefaultPlatformId <= 0 {
		fail("auth.default_platform_id must be > 0")
	}

	if c.InternalAuth.Enabled {
		internalSecrets := c.InternalAuth.AllSecrets()
		if len(internalSecrets) == 0 {
			fail("internal_auth.secret or internal_auth.secrets is required when enabled")
		}
		if slices.ContainsFunc(internalSecrets, weakSecret) {
			fail("internal_auth: every secret must be >= %d chars and not a published placeholder", minSecretLen)
		}
		if len(c.InternalAuth.AllowedServices) == 0 {
			fail("internal_auth.allowed_services must not be empty when enabled")
		}
		if c.InternalAuth.MaxSkewSeconds <= 0 {
			fail("internal_auth.max_skew_seconds must be > 0")
		}
	}

	if c.Ws.MaxFrameBytes <= 0 || c.Ws.SendQueue <= 0 || c.Ws.PingInterval <= 0 || c.Ws.PongWait <= c.Ws.PingInterval {
		fail("ws: need max_frame_bytes>0, send_queue>0, 0 < ping_interval < pong_wait")
	}
	if c.Ws.DeliverWorkers <= 0 || c.Ws.DeliverQueue <= 0 {
		fail("ws: need deliver_workers>0, deliver_queue>0")
	}
	if c.Limits.MaxContentBytes <= 0 || c.Limits.PullPageMax <= 0 || c.Limits.ConversationPageMax <= 0 || c.Limits.MaxSeqsPageMax <= 0 || c.Limits.GroupMaxMembers <= 0 {
		fail("limits: content/page/group limits must be > 0")
	}
	if c.Db.Driver == "mysql" && c.Limits.MaxContentBytes > mysqlTextMax {
		fail("limits.max_content_bytes must be <= %d on mysql (messages.content is TEXT)", mysqlTextMax)
	}
	if c.Limits.AuthPerIpPerMin < 0 {
		fail("limits.auth_per_ip_per_min must be >= 0")
	}

	if len(errs) > 0 {
		return fmt.Errorf("config: invalid: %w", errors.Join(errs...))
	}
	return nil
}

func (c Config) Redacted() Config {
	mask := func(s string) string { return lo.Ternary(s == "", "", "***") }
	c.Redis.Password = mask(c.Redis.Password)
	c.Db.Dsn = mask(c.Db.Dsn)
	c.OfflinePush.WebhookSecret = mask(c.OfflinePush.WebhookSecret)
	c.Auth.Native.Secret = mask(c.Auth.Native.Secret)
	c.InternalAuth.Secret = mask(c.InternalAuth.Secret)
	c.InternalAuth.Secrets = lo.Map(c.InternalAuth.Secrets, func(s string, _ int) string { return mask(s) })
	c.Auth.ExternalJwt.Secrets = lo.Map(c.Auth.ExternalJwt.Secrets, func(s string, _ int) string { return mask(s) })
	return c
}

func (c Config) EffectiveYAML() string {
	b, err := yaml.Marshal(c.Redacted())
	if err != nil {
		return fmt.Sprintf("<config marshal error: %v>", err)
	}
	return string(b)
}
