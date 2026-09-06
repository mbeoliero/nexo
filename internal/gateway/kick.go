package gateway

import (
	"context"
	"errors"
	"time"

	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/internal/auth"
)

const (
	KickNewLogin     = "new_login"
	KickTokenExpired = "token_expired"
)

// Kick closes this node's connections for user+platform that hold a different token,
// after pushing 2002 (design §8.2). A same-token reconnect keeps the old connection.
// Phase 6 calls this from the Bus subscriber as well.
func (g *Gateway) Kick(userId string, platformId int, keepTokenId string) {
	g.kick(context.Background(), userId, platformId, keepTokenId)
}

func (g *Gateway) kick(ctx context.Context, userId string, platformId int, keepTokenId string) {
	clients := g.users.Get(userId)
	g.kickMu.Lock()
	if ctx.Err() != nil || g.closing.Load() {
		g.kickMu.Unlock()
		return
	}
	targets := clients[:0]
	for _, c := range clients {
		if c.PlatformId == platformId && c.TokenId != keepTokenId && c.beginDrain() {
			targets = append(targets, c)
		}
	}
	g.kickMu.Unlock()
	for _, c := range targets {
		c.finishDrain(pushFrame(KickOnline, map[string]string{"reason": KickNewLogin}))
		log.CtxInfo(c.ctx(), "ws kick conn=%s user=%s platform=%d reason=%s", c.Id, c.UserId, c.PlatformId, KickNewLogin)
	}
}

// kick pushes 2002 and closes once the writer has drained the queue.
func (c *Client) kick(reason string) {
	c.closeAfterFlush(pushFrame(KickOnline, map[string]string{"reason": reason}))
	log.CtxInfo(c.ctx(), "ws kick conn=%s user=%s platform=%d reason=%s", c.Id, c.UserId, c.PlatformId, reason)
}

// closeAfterFlush optionally enqueues a last frame, then lets the writer flush the
// queue and close the socket.
func (c *Client) closeAfterFlush(last []byte) {
	c.gw.kickMu.Lock()
	started := c.beginDrain()
	c.gw.kickMu.Unlock()
	if started {
		c.finishDrain(last)
	}
}

// Caller holds gw.kickMu so source invalidation and target selection have one order.
func (c *Client) beginDrain() bool {
	if c.connCtx.Err() != nil || !c.draining.CompareAndSwap(false, true) {
		return false
	}
	c.cancelActive()
	return true
}

func (c *Client) finishDrain(last []byte) {
	timer := time.AfterFunc(writeWait, func() { c.Close(closeReasonKick) })
	context.AfterFunc(c.ctx(), func() { timer.Stop() })
	if last != nil {
		_ = c.Send(last)
	}
	close(c.drain)
}

// recheckLoop enforces token expiry after the handshake: external tokens by exp,
// native tokens also against the TokenStore (a newer login on the platform revokes them).
func (c *Client) recheckLoop() {
	ticker := time.NewTicker(c.gw.recheck)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case now := <-ticker.C:
			if c.tokenDead(now) {
				c.kick(KickTokenExpired)
				return
			}
		}
	}
}

// recheckFailLimit bounds how long a native connection may outlive a TokenStore outage:
// one skipped tick is tolerated, the third consecutive failure kicks (design §8.1).
const recheckFailLimit = 3

func (c *Client) tokenDead(now time.Time) bool {
	if c.Identity.ExpiresAt > 0 && now.UnixMilli() > c.Identity.ExpiresAt {
		return true
	}
	if c.Identity.Source != auth.SourceNative || c.gw.deps.Native == nil {
		return false
	}
	ctx, cancel := context.WithTimeout(c.ctx(), 5*time.Second)
	defer cancel()
	err := c.gw.deps.Native.Check(ctx, c.Identity)
	if ctx.Err() != nil && c.connCtx.Err() != nil {
		return false // closing anyway
	}
	if errors.Is(err, auth.ErrUnavailable) {
		c.recheckFails++
		log.CtxWarn(ctx, "ws token recheck unavailable (%d/%d): %v", c.recheckFails, recheckFailLimit, err)
		return c.recheckFails >= recheckFailLimit
	}
	c.recheckFails = 0
	return err != nil
}
