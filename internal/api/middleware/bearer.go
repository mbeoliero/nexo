package middleware

import (
	"context"
	"strconv"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
)

// Bearer verifies Authorization and stores the Identity. External tokens carry no
// platform, so X-Platform-Id (default from config) fills it in.
func Bearer(a auth.Authenticator, defaultPlatformId int) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		token, ok := strings.CutPrefix(string(c.GetHeader("Authorization")), "Bearer ")
		if !ok || token == "" {
			webx.Abort(ctx, c, errcode.ErrTokenMissing)
			return
		}
		id, err := a.Verify(ctx, token)
		if err != nil {
			webx.Abort(ctx, c, webx.AuthErr(err))
			return
		}
		if id.PlatformId == 0 {
			id.PlatformId = platformId(c, defaultPlatformId)
		}
		webx.SetIdentity(c, id)
		c.Next(ctx)
	}
}

func platformId(c *app.RequestContext, def int) int {
	if v, err := strconv.Atoi(string(c.GetHeader("X-Platform-Id"))); err == nil && v > 0 {
		return v
	}
	return def
}
