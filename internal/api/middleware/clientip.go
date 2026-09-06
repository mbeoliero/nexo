package middleware

import (
	"context"
	"net"

	"github.com/cloudwego/hertz/pkg/app"
)

// ClientIP fixes what c.ClientIP() means for the rest of the chain.
//
// Hertz's default honours X-Forwarded-For and X-Real-IP from *any* peer (its default TrustedCIDRs
// is 0.0.0.0/0 + ::/0), so without this every per-IP limit keys on a value the client picks: a
// fresh header per request mints a fresh limiter and the limit stops existing. Installing the
// policy per request rather than on the engine keeps it working when a host embeds nexo into its
// own Hertz engine (server.Mount), which never sees our engine options.
//
// trusted lists the CIDRs allowed to set those headers, from server.trusted_proxies. Empty means
// trust nobody and always use the socket peer address, which is the safe default.
func ClientIP(trusted []*net.IPNet) app.HandlerFunc {
	fn := app.ClientIPWithOption(app.ClientIPOptions{
		RemoteIPHeaders: []string{"X-Forwarded-For", "X-Real-IP"},
		TrustedCIDRs:    trusted,
	})
	return func(ctx context.Context, c *app.RequestContext) {
		c.SetClientIPFunc(fn)
		c.Next(ctx)
	}
}
