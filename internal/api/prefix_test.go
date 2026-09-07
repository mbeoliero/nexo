package api

import (
	"context"
	"slices"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
)

// The access log compares c.Path() against the redact list, so the list must spell a mounted path
// exactly the way Hertz's route groups do; a trailing slash on the prefix used to produce
// "/im//api/..." and log the login body.
func TestMountPathsMatchHertzGroups(t *testing.T) {
	for _, prefix := range []string{"", "/", "/im", "/im/"} {
		e := route.NewEngine(hconfig.NewOptions(nil))
		var seen string
		e.Group(prefix).Group("/api/v1").POST("/auth/login", func(_ context.Context, c *app.RequestContext) { seen = string(c.Path()) })
		want := "/api/v1/auth/login"
		if prefix != "" && prefix != "/" {
			want = "/im" + want
		}
		if w := ut.PerformRequest(e, "POST", want, nil); w.Code != 200 {
			t.Fatalf("prefix %q: %s not routed (%d)", prefix, want, w.Code)
		}
		got := mountPaths(prefix, []string{"/api/v1/auth/login", "/api/v1/auth/login"})
		if !slices.Equal(got, []string{seen}) {
			t.Errorf("prefix %q: mountPaths = %q, want [%q]", prefix, got, seen)
		}
	}
}
