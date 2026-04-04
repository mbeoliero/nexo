package config

import (
	"fmt"
	"time"

	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/spf13/viper"
)

// Config holds all configuration
type Config struct {
	Server        ServerConfig        `mapstructure:"server"`
	MySQL         MySQLConfig         `mapstructure:"mysql"`
	Redis         RedisConfig         `mapstructure:"redis"`
	JWT           JWTConfig           `mapstructure:"jwt"`
	ExternalJWT   ExternalJWTConfig   `mapstructure:"external_jwt"`
	WebSocket     WebSocketConfig     `mapstructure:"websocket"`
	CrossInstance CrossInstanceConfig `mapstructure:"cross_instance"`
}

// ServerConfig holds server configuration
type ServerConfig struct {
	HTTPPort           int      `mapstructure:"http_port"`
	WSPort             int      `mapstructure:"ws_port"`
	Mode               string   `mapstructure:"mode"`
	AllowedOrigins     []string `mapstructure:"allowed_origins"`
	ReadyDetailEnabled bool     `mapstructure:"ready_detail_enabled"`
}

// MySQLConfig holds MySQL configuration
type MySQLConfig struct {
	Host         string `mapstructure:"host"`
	Port         int    `mapstructure:"port"`
	User         string `mapstructure:"user"`
	Password     string `mapstructure:"password"`
	Database     string `mapstructure:"database"`
	Charset      string `mapstructure:"charset"`
	MaxOpenConns int    `mapstructure:"max_open_conns"`
	MaxIdleConns int    `mapstructure:"max_idle_conns"`
}

// DSN returns the MySQL data source name
func (c *MySQLConfig) DSN() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=%s&parseTime=True&loc=Local",
		c.User, c.Password, c.Host, c.Port, c.Database, c.Charset)
}

// RedisConfig holds Redis configuration
type RedisConfig struct {
	Host      string `mapstructure:"host"`
	Port      int    `mapstructure:"port"`
	Password  string `mapstructure:"password"`
	DB        int    `mapstructure:"db"`
	KeyPrefix string `mapstructure:"key_prefix"`
}

// Addr returns the Redis address
func (c *RedisConfig) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// JWTConfig holds JWT configuration
type JWTConfig struct {
	Secret      string `mapstructure:"secret"`
	ExpireHours int    `mapstructure:"expire_hours"`
}

// ExternalJWTConfig holds external JWT configuration for integrating with other systems
type ExternalJWTConfig struct {
	Enabled           bool   `mapstructure:"enabled"`
	Secret            string `mapstructure:"secret"`
	DefaultRole       string `mapstructure:"default_role"`        // "user" or "agent", defaults to "user"
	DefaultPlatformId int    `mapstructure:"default_platform_id"` // defaults to PlatformIdWeb(5)
}

// WebSocketConfig holds WebSocket configuration
type WebSocketConfig struct {
	MaxConnNum              int64         `mapstructure:"max_conn_num"`
	MaxMessageSize          int64         `mapstructure:"max_message_size"`
	WriteWait               time.Duration `mapstructure:"write_wait"`
	PongWait                time.Duration `mapstructure:"pong_wait"`
	PingPeriod              time.Duration `mapstructure:"ping_period"`
	PushChannelSize         int           `mapstructure:"push_channel_size"`
	PushWorkerNum           int           `mapstructure:"push_worker_num"`
	WriteChannelSize        int           `mapstructure:"write_channel_size"`
	AllowPlatformIDOverride bool          `mapstructure:"allow_platform_id_override"`
}

type CrossInstanceConfig struct {
	Enabled                    bool          `mapstructure:"enabled"`
	InstanceId                 string        `mapstructure:"instance_id"`
	HeartbeatSecond            int           `mapstructure:"heartbeat_second"`
	RouteTTLSeconds            int           `mapstructure:"route_ttl_seconds"`
	InstanceAliveTTLSeconds    int           `mapstructure:"instance_alive_ttl_seconds"`
	ReconcileIntervalSeconds   int           `mapstructure:"reconcile_interval_seconds"`
	DrainTimeoutSeconds        int           `mapstructure:"drain_timeout_seconds"`
	DrainRouteableGraceSeconds int           `mapstructure:"drain_routeable_grace_seconds"`
	SubscriptionChannelSize    int           `mapstructure:"subscription_channel_size"`
	SubscriptionSendTimeout    time.Duration `mapstructure:"subscription_send_timeout"`
	MaxRefsPerEnvelope         int           `mapstructure:"max_refs_per_envelope"`
}

// Global config instance
var GlobalConfig *Config

// Load loads configuration from file
func Load(configPath string) (*Config, error) {
	viper.SetConfigFile(configPath)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}

	var cfg Config
	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	// Set defaults
	if cfg.Server.HTTPPort == 0 {
		cfg.Server.HTTPPort = 8080
	}
	if cfg.Server.WSPort == 0 {
		cfg.Server.WSPort = cfg.Server.HTTPPort
	}
	if cfg.Server.Mode == "" {
		cfg.Server.Mode = "debug"
	}
	if cfg.MySQL.Charset == "" {
		cfg.MySQL.Charset = "utf8mb4"
	}
	if cfg.MySQL.MaxOpenConns == 0 {
		cfg.MySQL.MaxOpenConns = 100
	}
	if cfg.MySQL.MaxIdleConns == 0 {
		cfg.MySQL.MaxIdleConns = 10
	}
	if cfg.Redis.KeyPrefix == "" {
		cfg.Redis.KeyPrefix = "nexo:"
	}
	if cfg.JWT.ExpireHours == 0 {
		cfg.JWT.ExpireHours = 168 // 7 days
	}
	if cfg.ExternalJWT.DefaultRole == "" {
		cfg.ExternalJWT.DefaultRole = "user"
	}
	if cfg.ExternalJWT.DefaultPlatformId == 0 {
		cfg.ExternalJWT.DefaultPlatformId = constant.PlatformIdWeb
	}
	if cfg.WebSocket.MaxConnNum == 0 {
		cfg.WebSocket.MaxConnNum = 10000
	}
	if cfg.WebSocket.MaxMessageSize == 0 {
		cfg.WebSocket.MaxMessageSize = 51200
	}
	if cfg.WebSocket.WriteWait == 0 {
		cfg.WebSocket.WriteWait = 10 * time.Second
	}
	if cfg.WebSocket.PongWait == 0 {
		cfg.WebSocket.PongWait = 30 * time.Second
	}
	if cfg.WebSocket.PingPeriod == 0 {
		cfg.WebSocket.PingPeriod = 27 * time.Second
	}
	if cfg.WebSocket.PushChannelSize == 0 {
		cfg.WebSocket.PushChannelSize = 10000
	}
	if cfg.WebSocket.PushWorkerNum == 0 {
		cfg.WebSocket.PushWorkerNum = 10
	}
	if cfg.WebSocket.WriteChannelSize == 0 {
		cfg.WebSocket.WriteChannelSize = 256
	}
	// CrossInstance defaults are applied even when Enabled=false.
	// This is intentional: DrainTimeoutSeconds affects single-instance shutdown too.
	if cfg.CrossInstance.HeartbeatSecond == 0 {
		cfg.CrossInstance.HeartbeatSecond = 10
	}
	if cfg.CrossInstance.RouteTTLSeconds == 0 {
		cfg.CrossInstance.RouteTTLSeconds = 90
	}
	if cfg.CrossInstance.InstanceAliveTTLSeconds == 0 {
		cfg.CrossInstance.InstanceAliveTTLSeconds = 30
	}
	if cfg.CrossInstance.ReconcileIntervalSeconds == 0 {
		cfg.CrossInstance.ReconcileIntervalSeconds = max(1, cfg.CrossInstance.RouteTTLSeconds/3)
	}
	if cfg.CrossInstance.DrainTimeoutSeconds == 0 {
		cfg.CrossInstance.DrainTimeoutSeconds = 30
	}
	if cfg.CrossInstance.DrainRouteableGraceSeconds == 0 {
		cfg.CrossInstance.DrainRouteableGraceSeconds = 5
	}
	if cfg.CrossInstance.SubscriptionChannelSize == 0 {
		cfg.CrossInstance.SubscriptionChannelSize = 1024
	}
	if cfg.CrossInstance.SubscriptionSendTimeout == 0 {
		cfg.CrossInstance.SubscriptionSendTimeout = 5 * time.Second
	}
	if cfg.CrossInstance.MaxRefsPerEnvelope == 0 {
		cfg.CrossInstance.MaxRefsPerEnvelope = 500
	}

	GlobalConfig = &cfg
	return &cfg, nil
}
