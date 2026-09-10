package config

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

const exampleConfig = "../../config/config.example.yaml"

// Deployment profiles specify topology and overrides; the example documents the complete schema.
var deployConfigs = []string{
	"../../deploy/config.yaml",
	"../../deploy/config.pg-only.yaml",
	"../../deploy/config.mysql.yaml",
}

// keyPaths flattens a YAML document to dotted paths ("limits.pull_page_max"). Sequence values are
// leaves: their contents are examples, not structure.
func keyPaths(t *testing.T, path string) []string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("%s: %v", path, err)
	}
	return flatten(doc)
}

func flatten(doc map[string]any) []string {
	var out []string
	var walk func(prefix string, m map[string]any)
	walk = func(prefix string, m map[string]any) {
		for k, v := range m {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			if child, ok := v.(map[string]any); ok && len(child) > 0 {
				walk(p, child)
				continue
			}
			out = append(out, p)
		}
	}
	walk("", doc)
	slices.Sort(out)
	return out
}

// A key in the struct but not in the example is undocumented; a key in the example but not in the
// struct is dead. Either way an operator's expectation and the server's behaviour have parted ways.
func TestExampleConfigMirrorsStruct(t *testing.T) {
	want := keys(reflect.TypeFor[Config](), "")
	assertSameKeys(t, "internal/config.Config", want, filepath.Base(exampleConfig), keyPaths(t, exampleConfig))
}

func TestDeployConfigsUseKnownKeys(t *testing.T) {
	known := keyPaths(t, exampleConfig)
	for _, path := range deployConfigs {
		t.Run(filepath.Base(path), func(t *testing.T) {
			actual := keyPaths(t, path)
			for _, key := range actual {
				if !slices.Contains(known, key) {
					t.Errorf("unknown deployment key %q", key)
				}
			}
			for _, key := range []string{
				"server.trusted_proxies", "db.driver", "db.access", "db.max_open_conns",
				"bus.driver", "online_store.driver", "cache.driver", "auth.providers",
			} {
				if !slices.Contains(actual, key) {
					t.Errorf("deployment must explicitly choose %q", key)
				}
			}
		})
	}
}

func TestDeploymentEffectiveConfig(t *testing.T) {
	for _, tt := range []struct {
		name, driver, access, bus, cache, online, redisAddr string
	}{
		{name: "config.yaml", driver: "postgres", access: "sqlc", bus: "redis", cache: "redis", online: "redis", redisAddr: "redis:6379"},
		{name: "config.pg-only.yaml", driver: "postgres", access: "sqlc", bus: "postgres", cache: "pg", online: "db"},
		{name: "config.mysql.yaml", driver: "mysql", access: "gorm", bus: "redis", cache: "redis", online: "redis", redisAddr: "redis:6379"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decode(filepath.Join("../../deploy", tt.name), false)
			if err != nil {
				t.Fatal(err)
			}
			want := Default()
			want.Server.TrustedProxies = []string{"172.16.0.0/12", "192.168.0.0/16"}
			want.Db.Driver, want.Db.Access, want.Db.MaxOpenConns = tt.driver, tt.access, 10
			want.Bus.Driver, want.Cache.Driver, want.OnlineStore.Driver = tt.bus, tt.cache, tt.online
			want.Redis.Addr = tt.redisAddr
			want.Auth.Providers = []string{"native"}
			want.InternalAuth.AllowedServices = []string{"my-backend"}
			want.InternalAuth.RequireTls = false
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("effective deployment differs from intended overrides:\ngot: %s\nwant: %s", got.EffectiveYAML(), want.EffectiveYAML())
			}
		})
	}
}

// Every shipped config must load and validate with only the secrets supplied from the environment,
// which is how the compose profiles run.
func TestShippedConfigsLoad(t *testing.T) {
	for _, path := range append([]string{exampleConfig}, deployConfigs...) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			t.Setenv("NEXO_AUTH_NATIVE_SECRET", testSecret)
			cfg, err := decode(path, false)
			if err != nil {
				t.Fatal(err)
			}
			t.Setenv("NEXO_DB_DSN", "postgres://nexo:pw@localhost:5432/nexo?sslmode=disable")
			if cfg.Db.Driver == "mysql" {
				t.Setenv("NEXO_DB_DSN", "nexo:pw@tcp(localhost:3306)/nexo?parseTime=true")
			}
			t.Setenv("NEXO_REDIS_ADDR", "localhost:6379")
			if _, err := Load(path); err != nil {
				t.Fatalf("%s: %v", path, err)
			}
		})
	}
}

func TestDeployNodePortsStayLocal(t *testing.T) {
	for _, path := range []string{"../../deploy/docker-compose.yml", "../../deploy/docker-compose.mysql.yml"} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			raw, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			var doc struct {
				Services map[string]struct {
					Ports []string `yaml:"ports"`
				} `yaml:"services"`
			}
			if err := yaml.Unmarshal(raw, &doc); err != nil {
				t.Fatal(err)
			}
			for node, port := range map[string]string{"nexo1": "18081", "nexo2": "18082", "nexo3": "18083"} {
				want := []string{"127.0.0.1:" + port + ":8080"}
				if got := doc.Services[node].Ports; !slices.Equal(got, want) {
					t.Errorf("%s ports = %v, want %v; direct nodes must not bypass nginx throttling remotely", node, got, want)
				}
			}
		})
	}
}

func assertSameKeys(t *testing.T, aName string, a []string, bName string, b []string) {
	t.Helper()
	inA, inB := map[string]bool{}, map[string]bool{}
	for _, k := range a {
		inA[k] = true
	}
	for _, k := range b {
		inB[k] = true
	}
	missing := slices.Sorted(maps.Keys(inA))
	missing = slices.DeleteFunc(missing, func(k string) bool { return inB[k] })
	extra := slices.Sorted(maps.Keys(inB))
	extra = slices.DeleteFunc(extra, func(k string) bool { return inA[k] })
	if len(missing) > 0 {
		t.Errorf("in %s but not %s: %v", aName, bName, missing)
	}
	if len(extra) > 0 {
		t.Errorf("in %s but not %s: %v", bName, aName, extra)
	}
}
