package api

import (
	"context"
	"errors"
	"net/http"
	"testing"

	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"

	"github.com/mbeoliero/nexo/internal/config"
)

func TestHealthzProbesDb(t *testing.T) {
	dbErr := error(nil)
	e := route.NewEngine(hconfig.NewOptions(nil))
	Register(e, "", &config.Config{NodeId: "n1"}, Deps{Ready: func(context.Context) error { return dbErr }})
	if w := ut.PerformRequest(e, http.MethodGet, "/healthz", nil); w.Code != http.StatusOK {
		t.Fatalf("healthy: %d %s", w.Code, w.Body.String())
	}
	dbErr = errors.New("connection refused")
	if w := ut.PerformRequest(e, http.MethodGet, "/healthz", nil); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("db down: %d %s", w.Code, w.Body.String())
	}
}
