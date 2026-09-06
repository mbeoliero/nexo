package middleware

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/ratelimit"
)

// Cap on distinct tracked IPs. Past it new addresses share one bucket instead of growing the
// map without bound, so a wide source set (a botnet, or spoofed X-Forwarded-For if a proxy is
// mistakenly trusted) costs an attacker throughput rather than this node's memory.
const maxTrackedIps = 10000

// IpRateLimit caps requests per client IP per minute and answers 429 above it; it guards the
// bcrypt-heavy native login / register routes (design §7.3). perMin <= 0 disables it.
// Node-local like the other limits; the LB (nginx limit_req) is the production layer.
//
// The IP comes from c.ClientIP(), which middleware.ClientIP has already constrained to
// server.trusted_proxies — without that the key would be client-controlled and the limit moot.
func IpRateLimit(perMin int) app.HandlerFunc {
	if perMin <= 0 {
		return func(ctx context.Context, c *app.RequestContext) { c.Next(ctx) }
	}
	l := ratelimit.NewKeyed(float64(perMin)/60, perMin, maxTrackedIps)
	return func(ctx context.Context, c *app.RequestContext) {
		if !l.Allow(c.ClientIP(), time.Now()) {
			webx.Abort(ctx, c, errcode.ErrTooManyRequests.WithMessage("auth rate limit"))
			return
		}
		c.Next(ctx)
	}
}
