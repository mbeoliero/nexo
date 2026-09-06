package middleware

import (
	"context"
	"slices"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/api/webx"
)

const maxLoggedBody = 512

// Seam for tests: kit binds its writer to os.Stdout at init and ignores SetOutput.
var logAccess = log.CtxInfo

// AccessLog configures ProcessLogger.
type AccessLog struct {
	// Redact omits the body for these paths even when Body is on.
	Redact []string
	// Skip writes no line at all for these paths (the LB health probe).
	Skip []string
	// Body logs the first maxLoggedBody bytes of the request body. Off by default: on an IM server
	// those bodies carry message content, which must not reach the access log. The size is logged
	// either way.
	Body bool
}

// ProcessLogger writes one line per request. Authorization / X-Signature are never read here.
func ProcessLogger(cfg AccessLog) app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		start := time.Now()
		c.Next(ctx)

		path := string(c.Path())
		if slices.Contains(cfg.Skip, path) {
			return
		}
		raw := c.Request.Body()
		n := len(raw)
		body := ""
		if cfg.Body && n > 0 && !slices.Contains(cfg.Redact, path) {
			body = string(raw[:min(n, maxLoggedBody)])
		}
		logAccess(ctx, "nexo http [%s] %s status=%d code=%d user=%s ip=%s cost=%s body_len=%d body=%q",
			c.Method(), path, c.Response.StatusCode(), webx.CodeFrom(c), webx.IdentityFrom(c).UserId, c.ClientIP(), time.Since(start).Round(time.Microsecond), n, body)
	}
}
