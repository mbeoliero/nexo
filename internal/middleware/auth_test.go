package middleware

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	hzconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"

	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/pkg/constant"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/jwt"
)

func TestParseTokenWithFallbackRejectsNativeTokenWhenStateValidatorFails(t *testing.T) {
	cfg := &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	}
	token, err := jwt.GenerateToken("u1", constant.PlatformIdWeb, cfg.JWT.Secret, 1)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	SetNativeTokenStateValidator(func(context.Context, *jwt.Claims, string) error {
		return errcode.ErrTokenInvalid
	})
	t.Cleanup(func() {
		SetNativeTokenStateValidator(nil)
	})

	claims, err := ParseTokenWithFallback(context.Background(), token, cfg)
	if err != errcode.ErrTokenInvalid {
		t.Fatalf("expected token invalid error, got claims=%+v err=%v", claims, err)
	}
}

func TestJWTAuthRejectsNativeTokenWhenStateValidatorFails(t *testing.T) {
	cfg := &config.Config{
		JWT: config.JWTConfig{Secret: "test-secret"},
	}
	oldCfg := config.GlobalConfig
	config.GlobalConfig = cfg
	t.Cleanup(func() {
		config.GlobalConfig = oldCfg
	})

	token, err := jwt.GenerateToken("u1", constant.PlatformIdWeb, cfg.JWT.Secret, 1)
	if err != nil {
		t.Fatalf("generate token: %v", err)
	}

	SetNativeTokenStateValidator(func(context.Context, *jwt.Claims, string) error {
		return errcode.ErrTokenInvalid
	})
	t.Cleanup(func() {
		SetNativeTokenStateValidator(nil)
	})

	engine := route.NewEngine(hzconfig.NewOptions(nil))
	engine.GET("/secure", JWTAuth(), func(ctx context.Context, c *app.RequestContext) {
		c.JSON(200, map[string]any{"ok": true})
	})

	resp := ut.PerformRequest(
		engine,
		"GET",
		"/secure",
		nil,
		ut.Header{Key: AuthorizationHeader, Value: BearerPrefix + token},
	).Result()

	var body map[string]any
	if err := json.Unmarshal(resp.Body(), &body); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if got := int(body["code"].(float64)); got != errcode.ErrTokenInvalid.Code {
		t.Fatalf("error code mismatch: got %d want %d", got, errcode.ErrTokenInvalid.Code)
	}
	if _, ok := body["data"]; ok {
		t.Fatalf("unexpected success payload: %+v", body)
	}
}
