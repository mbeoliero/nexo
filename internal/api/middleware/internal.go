package middleware

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/network"
	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/identity"
)

type InternalAuth struct {
	Verifier   *auth.Internal
	RequireTls bool
	// TrustedProxies are the CIDRs whose X-Forwarded-Proto this node believes, from
	// server.trusted_proxies. Empty means only a directly negotiated TLS connection counts.
	TrustedProxies    []*net.IPNet
	DefaultPlatformId int
}

func (m InternalAuth) request(c *app.RequestContext) auth.InternalRequest {
	h := func(k string) string { return string(c.GetHeader(k)) }
	// Raw request-target, not the parsed/re-encoded args, so the signature sees exactly what was sent.
	rawPath, rawQuery, _ := strings.Cut(string(c.Request.RequestURI()), "?")
	return auth.InternalRequest{
		Service: h("X-Service-Name"), Timestamp: h("X-Timestamp"), Nonce: h("X-Nonce"),
		Method: string(c.Method()), RawPath: rawPath, RawQuery: rawQuery,
		UserId: h("X-User-Id"), PlatformId: h("X-Platform-Id"),
		Body: c.Request.Body(), Signature: h("X-Signature"),
	}
}

// overTls reports whether this request reached the node over TLS. X-Forwarded-Proto is believed
// only from a configured proxy: honouring it from any peer would let a client satisfy require_tls
// with a header, the same forgery ClientIP already blocks for X-Forwarded-For.
func (m InternalAuth) overTls(c *app.RequestContext) bool {
	if conn, ok := c.GetConn().(network.ConnTLSer); ok && conn.ConnectionState().HandshakeComplete {
		return true
	}
	return string(c.GetHeader("X-Forwarded-Proto")) == "https" && trustedPeer(c, m.TrustedProxies)
}

func trustedPeer(c *app.RequestContext, trusted []*net.IPNet) bool {
	addr := c.RemoteAddr()
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	return ip != nil && slices.ContainsFunc(trusted, func(n *net.IPNet) bool { return n.Contains(ip) })
}

func (m InternalAuth) verify(ctx context.Context, c *app.RequestContext) bool {
	if m.RequireTls && !m.overTls(c) {
		webx.Abort(ctx, c, errcode.ErrForbidden.WithMessage("internal api requires tls"))
		return false
	}
	if err := m.Verifier.Verify(ctx, m.request(c)); err != nil {
		webx.Abort(ctx, c, lo.Ternary(errors.Is(err, auth.ErrUnavailable), errcode.ErrStoreFailed.Wrap(err), errcode.ErrUnauthorized.Wrap(err)))
		return false
	}
	return true
}

// Signature only.
func (m InternalAuth) Verify(ctx context.Context, c *app.RequestContext) {
	if m.verify(ctx, c) {
		c.Next(ctx)
	}
}

// Signature plus impersonation of X-User-Id (platform user ids only).
func (m InternalAuth) AsUser(ctx context.Context, c *app.RequestContext) {
	if !m.verify(ctx, c) {
		return
	}
	userId := string(c.GetHeader("X-User-Id"))
	actor, err := identity.ParseActor(userId)
	if err != nil {
		webx.Abort(ctx, c, errcode.ErrInvalidParam.WithMessage("X-User-Id must be a platform user id"))
		return
	}
	webx.SetIdentity(c, auth.Identity{UserId: userId, Role: string(actor.Role), PlatformId: platformId(c, m.DefaultPlatformId), Source: auth.SourceInternal})
	c.Next(ctx)
}
