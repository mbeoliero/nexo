package config

import (
	"maps"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"testing"

	"gopkg.in/yaml.v3"
)

const exampleConfig = "../../config/config.example.yaml"

// deployConfigs are the shipped profiles. They differ from the example in values only: a key that
// exists in one and not the others is drift, and drift here means an operator silently runs on a
// default nobody chose.
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

func TestDeployConfigsMatchExample(t *testing.T) {
	want := keyPaths(t, exampleConfig)
	for _, path := range deployConfigs {
		t.Run(filepath.Base(path), func(t *testing.T) {
			assertSameKeys(t, filepath.Base(exampleConfig), want, filepath.Base(path), keyPaths(t, path))
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

// designDoc is the architecture doc; its §11 block is presented as the full config reference, so it
// drifts like any other copy. Checking it here also catches a duplicate top-level key, which YAML
// resolves by silently keeping the last one.
const designDoc = "../../docs/design.md"

var (
	yamlBlock  = regexp.MustCompile("(?s)```yaml\n(.*?)```")
	limitsHead = regexp.MustCompile(`(?m)^limits:`)
)

func TestDesignDocConfigBlockMatchesExample(t *testing.T) {
	raw, err := os.ReadFile(designDoc)
	if err != nil {
		t.Fatal(err)
	}
	var blocks []string
	for _, b := range yamlBlock.FindAllStringSubmatch(string(raw), -1) {
		// The config reference is the one block with a top-level `limits:`; the rest are snippets.
		if limitsHead.MatchString(b[1]) {
			blocks = append(blocks, b[1])
		}
	}
	if len(blocks) != 1 {
		t.Fatalf("want exactly one config block in %s, found %d", designDoc, len(blocks))
	}
	var doc map[string]any
	// yaml.v3 rejects a duplicate mapping key, which is one of the drifts this test exists to catch.
	if err := yaml.Unmarshal([]byte(blocks[0]), &doc); err != nil {
		t.Fatalf("%s config block: %v", designDoc, err)
	}
	assertSameKeys(t, filepath.Base(exampleConfig), keyPaths(t, exampleConfig), filepath.Base(designDoc), flatten(doc))
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
