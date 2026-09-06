package config

import (
	"strings"
	"testing"
)

// Validate rejects HMAC keys under minSecretLen, so fixtures need real-length ones.
const (
	testSecret  = "0123456789abcdef0123456789abcdef"
	testSecret2 = "fedcba9876543210fedcba9876543210"
)

// valid is the smallest config that passes Validate; each case mutates one thing.
func valid() Config {
	c := Config{}
	c.Server.Addr = ":8080"
	c.Db = DbConfig{Driver: "postgres", Access: "sqlc", Dsn: "postgres://nexo:pw@localhost/nexo", MaxOpenConns: 1}
	c.Bus.Driver = "local"
	c.OnlineStore = OnlineStoreConfig{Driver: "db", Ttl: 60e9, RenewInterval: 20e9}
	c.Cache.Driver = "local"
	c.OfflinePush.Driver = "noop"
	c.Auth = AuthConfig{Providers: []string{"native"}, DefaultPlatformId: 5,
		Native: NativeAuthConfig{Secret: testSecret, ExpireHours: 1}}
	c.Ws = WsConfig{MaxFrameBytes: 1, SendQueue: 1, PingInterval: 1, PongWait: 2, DeliverWorkers: 1, DeliverQueue: 1}
	c.Limits = LimitsConfig{MaxContentBytes: 1, PullPageMax: 1, ConversationPageMax: 1, MaxSeqsPageMax: 1, GroupMaxMembers: 1}
	return c
}

func TestValidateCombinations(t *testing.T) {
	cases := map[string]struct {
		mut  func(*Config)
		want string
	}{
		"ok":             {func(c *Config) {}, ""},
		"mysql+sqlc":     {func(c *Config) { c.Db.Driver = "mysql" }, "db.driver=mysql requires db.access=gorm"},
		"pgbus on mysql": {func(c *Config) { c.Db.Driver, c.Db.Access, c.Bus.Driver = "mysql", "gorm", "postgres" }, "bus.driver=postgres requires"},
		"redis no addr":  {func(c *Config) { c.Cache.Driver = "redis" }, "requires redis.addr"},
		"webhook http": {func(c *Config) {
			c.OfflinePush.Driver, c.OfflinePush.WebhookUrl, c.OfflinePush.WebhookSecret = "webhook", "http://x", "s"
		}, "https://"},
		"shared secret": {func(c *Config) {
			c.Auth.Providers = []string{"external_jwt", "native"}
			c.Auth.ExternalJwt.Secrets = []string{testSecret}
		}, "shares a secret"},
		"internal nosecret": {func(c *Config) { c.InternalAuth.Enabled = true }, "internal_auth.secret or internal_auth.secrets is required"},
		"internal secrets list": {func(c *Config) {
			c.InternalAuth = InternalAuthConfig{Enabled: true, Secrets: []string{testSecret, testSecret2}, AllowedServices: []string{"gw"}, MaxSkewSeconds: 300}
		}, ""},
		"unknown provider":   {func(c *Config) { c.Auth.Providers = []string{"ldap"} }, "unknown provider"},
		"placeholder secret": {func(c *Config) { c.Auth.Native.Secret = "change-me" }, "placeholder"},
		// The value that used to ship in deploy/config.*.yaml: long enough, still forgeable by anyone.
		"published dev secret": {func(c *Config) { c.Auth.Native.Secret = "dev-only-secret-not-for-production" }, "placeholder"},
		"short secret":         {func(c *Config) { c.Auth.Native.Secret = strings.Repeat("a", 31) }, "32"},
		"weak internal secret": {func(c *Config) {
			c.InternalAuth = InternalAuthConfig{Enabled: true, Secret: "short", AllowedServices: []string{"gw"}, MaxSkewSeconds: 300}
		}, "internal_auth"},
		"bad trusted proxy":   {func(c *Config) { c.Server.TrustedProxies = []string{"10.0.0.1"} }, "trusted_proxies"},
		"good trusted proxy":  {func(c *Config) { c.Server.TrustedProxies = []string{"10.0.0.0/8", "::1/128"} }, ""},
		"cleaner too small":   {func(c *Config) { c.Cache.CleanerInterval = 1e6 }, "cleaner_interval"},
		"cleaner disabled":    {func(c *Config) { c.Cache.CleanerInterval = 0 }, ""},
		"max_open_conns zero": {func(c *Config) { c.Db.MaxOpenConns = 0 }, "db.max_open_conns"},
		"mysql clientFoundRows": {func(c *Config) {
			c.Db = DbConfig{Driver: "mysql", Access: "gorm", Dsn: "u:p@tcp(h)/db?clientFoundRows=true", MaxOpenConns: 1}
		}, "clientFoundRows"},
		"mysql unparsable dsn": {func(c *Config) {
			c.Db = DbConfig{Driver: "mysql", Access: "gorm", Dsn: "u:secret-value@tcp(h)/db?loc=%zz", MaxOpenConns: 1}
		}, "not a valid mysql DSN"},
		"mysql ok": {func(c *Config) {
			c.Db = DbConfig{Driver: "mysql", Access: "gorm", Dsn: "u:secret-value@tcp(h)/db?parseTime=true", MaxOpenConns: 1}
		}, ""},
		// Without it the process boots clean and then fails every datetime(3) scan as 20001.
		"mysql no parseTime": {func(c *Config) {
			c.Db = DbConfig{Driver: "mysql", Access: "gorm", Dsn: "u:secret-value@tcp(h)/db", MaxOpenConns: 1}
		}, "parseTime"},
		"mysql content over text": {func(c *Config) {
			c.Db = DbConfig{Driver: "mysql", Access: "gorm", Dsn: "u:p@tcp(h)/db?parseTime=true", MaxOpenConns: 1}
			c.Limits.MaxContentBytes = 70000
		}, "max_content_bytes"},
		"auth limit negative": {func(c *Config) { c.Limits.AuthPerIpPerMin = -1 }, "auth_per_ip_per_min"},
		"no providers":        {func(c *Config) { c.Auth.Providers = nil }, "must not be empty"},
		"injected auth":       {func(c *Config) { c.Auth.Providers, c.Auth.Injected = nil, true }, ""},
		"injected but native listed": {func(c *Config) {
			c.Auth.Injected, c.Auth.Native.Secret = true, ""
		}, "auth.native.secret is required"},
		// A non-local bus means more than one node; node-local token / nonce state then gives
		// random cross-node 401s and defeats replay protection (design §6.3).
		"multinode local cache": {func(c *Config) { c.Bus.Driver = "postgres" }, "native tokens are only valid on the node"},
		"multinode local cache internal": {func(c *Config) {
			c.Bus.Driver = "postgres"
			c.Auth.Providers, c.Auth.ExternalJwt.Secrets = []string{"external_jwt"}, []string{testSecret2}
			c.InternalAuth = InternalAuthConfig{Enabled: true, Secret: testSecret, AllowedServices: []string{"gw"}, MaxSkewSeconds: 300}
		}, "nonce replay protection does not span nodes"},
		"multinode shared cache":  {func(c *Config) { c.Bus.Driver, c.Cache.Driver = "postgres", "pg" }, ""},
		"single node local cache": {func(c *Config) { c.Bus.Driver, c.Cache.Driver = "local", "local" }, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := valid()
			tc.mut(&c)
			err := c.Validate()
			switch {
			case tc.want == "" && err != nil:
				t.Fatalf("unexpected error: %v", err)
			case tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)):
				t.Fatalf("got %v, want containing %q", err, tc.want)
			}
			// A DSN error must report the field, never the driver's message: that one quotes the DSN.
			if err != nil && strings.Contains(err.Error(), "secret-value") {
				t.Fatalf("DSN secret leaked: %v", err)
			}
		})
	}
}

func TestRedacted(t *testing.T) {
	c := valid()
	c.Auth.ExternalJwt.Secrets = []string{"k1"}
	c.InternalAuth.Secret = "k2"
	out := c.EffectiveYAML()
	for _, leak := range []string{"pw@", testSecret, "k1", "k2"} {
		if strings.Contains(out, leak) {
			t.Errorf("secret %q leaked:\n%s", leak, out)
		}
	}
	if !strings.Contains(out, `dsn: '***'`) {
		t.Errorf("dsn not masked:\n%s", out)
	}
	if c.Auth.Native.Secret != testSecret || c.Db.Dsn != valid().Db.Dsn {
		t.Error("Redacted must not mutate original")
	}
	// An unset field must stay unset: '***' would read as "configured".
	c.Db.Dsn = ""
	if got := c.Redacted().Db.Dsn; got != "" {
		t.Errorf("empty dsn masked to %q", got)
	}
}

// Values with no default and no file must still arrive through the environment (AGENTS.md: env overrides).
func TestLoadEnvOnly(t *testing.T) {
	t.Setenv("NEXO_DB_DSN", "postgres://env")
	t.Setenv("NEXO_AUTH_NATIVE_SECRET", "0123456789abcdef0123456789abcdef")
	t.Setenv("NEXO_NODE_ID", "env-node")
	t.Setenv("NEXO_SERVER_ADDR", ":9999")
	cfg, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Db.Dsn != "postgres://env" || cfg.NodeId != "env-node" || cfg.Server.Addr != ":9999" || cfg.Auth.Native.Secret == "" {
		t.Fatalf("env not applied: %+v", cfg.Redacted())
	}
}

// nexo migrate needs nothing but db.* (AGENTS.md); the rest of the file is not validated.
func TestLoadDb(t *testing.T) {
	t.Setenv("NEXO_DB_DSN", "postgres://env")
	t.Setenv("NEXO_AUTH_NATIVE_SECRET", "")
	db, err := LoadDb("")
	if err != nil || db.Dsn != "postgres://env" || db.Driver != "postgres" {
		t.Fatalf("LoadDb: %+v %v", db, err)
	}
	if _, err := Load(""); err == nil {
		t.Fatal("Load must still reject the missing native secret")
	}
}
