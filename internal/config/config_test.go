package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/mbeoliero/nexo/pkg/constant"
)

func TestLoadSetsCrossInstanceDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte(`
server:
  http_port: 8080
mysql:
  host: localhost
  port: 3306
  user: root
  database: test
redis:
  host: localhost
  port: 6379
jwt:
  secret: test
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.CrossInstance.RouteTTLSeconds == 0 || cfg.CrossInstance.InstanceAliveTTLSeconds == 0 || cfg.CrossInstance.ReconcileIntervalSeconds == 0 {
		t.Fatalf("cross-instance defaults not set: %+v", cfg.CrossInstance)
	}
	if cfg.CrossInstance.SubscriptionChannelSize == 0 || cfg.CrossInstance.SubscriptionSendTimeout == 0 || cfg.CrossInstance.MaxRefsPerEnvelope == 0 {
		t.Fatalf("cross-instance subscription defaults not set: %+v", cfg.CrossInstance)
	}
	if cfg.WebSocket.AllowPlatformIDOverride {
		t.Fatalf("expected platform override to default to false: %+v", cfg.WebSocket)
	}
	if cfg.Server.ReadyDetailEnabled {
		t.Fatalf("expected ready detail exposure to default to false: %+v", cfg.Server)
	}
}

func TestLoadSetsReconcileIntervalDefaultBelowRouteTTL(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte(`
server:
  http_port: 8080
mysql:
  host: localhost
  port: 3306
  user: root
  database: test
redis:
  host: localhost
  port: 6379
jwt:
  secret: test
cross_instance:
  route_ttl_seconds: 90
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.CrossInstance.ReconcileIntervalSeconds >= cfg.CrossInstance.RouteTTLSeconds {
		t.Fatalf("expected reconcile interval below route ttl: %+v", cfg.CrossInstance)
	}
}

func TestCheckedInConfigDoesNotSetFixedCrossInstanceInstanceID(t *testing.T) {
	path := filepath.Join("..", "..", "config", "config.yaml")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load checked-in config: %v", err)
	}

	if cfg.CrossInstance.InstanceId != "" {
		t.Fatalf("expected checked-in config to leave cross-instance instance_id empty, got %q", cfg.CrossInstance.InstanceId)
	}
}

func TestLoadSetsExternalJWTDefaultPlatformToWeb(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	content := []byte(`
server:
  http_port: 8080
mysql:
  host: localhost
  port: 3306
  user: root
  database: test
redis:
  host: localhost
  port: 6379
jwt:
  secret: test
external_jwt:
  enabled: true
  secret: ext-test
`)
	if err := os.WriteFile(path, content, 0o644); err != nil {
		t.Fatalf("write config: %v", err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	if cfg.ExternalJWT.DefaultPlatformId != constant.PlatformIdWeb {
		t.Fatalf("default platform mismatch: got %d want %d", cfg.ExternalJWT.DefaultPlatformId, constant.PlatformIdWeb)
	}
}

func TestCheckedInConfigUsesWebDefaultPlatformForExternalJWT(t *testing.T) {
	path := filepath.Join("..", "..", "config", "config.yaml")

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("load checked-in config: %v", err)
	}

	if cfg.ExternalJWT.DefaultPlatformId != constant.PlatformIdWeb {
		t.Fatalf("checked-in external jwt default platform mismatch: got %d want %d", cfg.ExternalJWT.DefaultPlatformId, constant.PlatformIdWeb)
	}
}
