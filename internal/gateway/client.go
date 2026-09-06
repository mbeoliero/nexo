package gateway

import (
	"context"
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mbeoliero/kit/log"
	"golang.org/x/time/rate"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/onlinestore"
)

const (
	writeWait          = 10 * time.Second
	overLimitCloseAt   = 3 // consecutive rate-limited frames before the connection is closed
	closeReasonSlow    = "slow_consumer"
	closeReasonRate    = "rate_limited"
	closeReasonRead    = "read"
	closeReasonServer  = "server"
	closeReasonOverCap = "send_bytes_over_cap"
	closeReasonKick    = "kicked"
	tokenRecheck       = 60 * time.Second
)

type Client struct {
	recheckFails  int // consecutive TokenStore failures in recheckLoop; only that goroutine touches it
	Id            string
	Ip            string
	auth.Identity // UserId, PlatformId and TokenId are read through promotion

	gw           *Gateway
	conn         ClientConn
	send         chan []byte
	closed       chan struct{}
	close        func()
	remove       func()
	hardClose    func()
	connCtx      context.Context // gateway ctx + log kv; cancelled by close
	cancelCtx    context.CancelFunc
	activeCtx    context.Context // ends at draining; already-admitted handlers keep connCtx until close
	cancelActive context.CancelFunc
	onlineAdded  bool          // protected by gw.presence
	drain        chan struct{} // closed by kick: writer flushes the queue, then closes
	draining     atomic.Bool
	inflight     chan struct{}
	frames       *rate.Limiter // inbound frames per second, burst 2x; Inf when the limit is 0
	overRun      int

	// Queue accounting: enqueue, the writer's dequeue and close all take sendMu so bytes counted
	// in gw.sendBytes are released exactly once, whichever of them wins the race.
	sendMu  sync.Mutex
	queued  int64 // bytes in send, already counted in gw.sendBytes
	closing bool
}

func (g *Gateway) newClient(id auth.Identity, connId, ip string, conn ClientConn) *Client {
	c := &Client{
		Id: connId, Ip: ip, Identity: id,
		gw: g, conn: conn,
		send:     make(chan []byte, g.cfg.Ws.SendQueue),
		closed:   make(chan struct{}),
		drain:    make(chan struct{}),
		inflight: make(chan struct{}, max(g.cfg.Limits.WsInflightPerConn, 1)),
		frames:   newLimiter(float64(g.cfg.Limits.WsFramesPerSec), 2*g.cfg.Limits.WsFramesPerSec),
	}
	c.connCtx, c.cancelCtx = context.WithCancel(log.AppendLogKv(log.AppendLogKv(g.ctx, "conn_id", connId), "user_id", id.UserId))
	c.activeCtx, c.cancelActive = context.WithCancel(c.connCtx)
	c.hardClose = sync.OnceFunc(func() { _ = conn.Close() })
	c.close = sync.OnceFunc(func() {
		g.kickMu.Lock()
		c.cancelCtx()
		g.kickMu.Unlock()
		c.sendMu.Lock()
		c.closing = true
		leaked := c.queued
		c.queued = 0
		c.sendMu.Unlock()
		close(c.closed)
		g.sendBytes.Add(-leaked) // frames still in the queue will never be written
		g.users.Unregister(c)
	})
	c.remove = sync.OnceFunc(func() { g.onlineRemove(c) })
	return c
}

// Send enqueues an outbound frame for the single writer. A full queue means a slow
// consumer: the connection is closed rather than a frame silently dropped. Replies, error
// frames, Kick and Resync go through here and are never subject to the node byte cap.
func (c *Client) Send(frame []byte) error {
	n := int64(len(frame))
	c.gw.sendBytes.Add(n)
	return c.enqueue(frame, n)
}

// Push is Send for server-initiated pushes (2001 / 2003): under ws_send_bytes_total pressure the
// push is dropped and replaced by 2004 Resync so the client re-pulls by seq (design §7.3).
func (c *Client) Push(frame []byte) error {
	n := int64(len(frame))
	total := c.gw.sendBytes.Add(n)
	if limit := c.gw.cfg.Limits.WsSendBytesTotal; limit > 0 && total > limit {
		c.gw.sendBytes.Add(-n)
		c.gw.dropped.Add(1)
		return c.resync(closeReasonOverCap)
	}
	return c.enqueue(frame, n)
}

func (c *Client) resync(reason string) error {
	return c.Send(pushFrame(Resync, map[string]string{"reason": reason}))
}

// enqueue takes ownership of n bytes already added to gw.sendBytes and gives them back on failure.
func (c *Client) enqueue(frame []byte, n int64) error {
	c.sendMu.Lock()
	if c.closing {
		c.sendMu.Unlock()
		c.gw.sendBytes.Add(-n)
		return errcode.ErrConnClosed
	}
	select {
	case c.send <- frame:
		c.queued += n
		c.sendMu.Unlock()
		return nil
	default:
		c.sendMu.Unlock()
		c.gw.sendBytes.Add(-n)
		c.gw.slowConsumers.Add(1)
		c.Close(closeReasonSlow)
		return errcode.ErrConnClosed.WithMessage("send queue full")
	}
}

func (c *Client) Close(reason string) {
	// Independent of cleanup's Once: even a concurrent blocked Remove cannot delay the socket.
	c.hardClose()
	log.CtxInfo(c.ctx(), "ws close conn=%s user=%s platform=%d reason=%s", c.Id, c.UserId, c.PlatformId, reason)
	c.close()
	c.remove()
}

func (c *Client) slot() Slot { return Slot{UserId: c.UserId, TokenId: c.TokenId, Ip: c.Ip} }

func (c *Client) ref() onlinestore.ConnRef {
	return onlinestore.ConnRef{UserId: c.UserId, PlatformId: c.PlatformId, ConnId: c.Id}
}

// ctx is done once the connection closed or the gateway shut down; handlers stop with it.
func (c *Client) ctx() context.Context { return c.connCtx }

// Serve runs both loops and returns when the connection is gone.
func (c *Client) Serve() {
	if !c.gw.beginWork() {
		c.Close(closeReasonServer)
		return
	}
	defer c.gw.work.Done()
	var wg sync.WaitGroup
	wg.Go(c.writeLoop)
	wg.Go(c.recheckLoop)
	c.readLoop()
	wg.Wait()
}

func (c *Client) writeLoop() {
	ticker := time.NewTicker(c.gw.cfg.Ws.PingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.closed:
			return
		case frame := <-c.send:
			if !c.write(frame) {
				return
			}
		case <-c.drain:
			for {
				select {
				case frame := <-c.send:
					if !c.write(frame) {
						return
					}
				default:
					c.closeControl()
					c.Close(closeReasonKick)
					return
				}
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.conn.WritePing(); err != nil {
				c.Close("ping: " + err.Error())
				return
			}
		}
	}
}

func (c *Client) write(frame []byte) bool {
	n := int64(len(frame))
	c.sendMu.Lock()
	if !c.closing { // after close the accounting was already released in one piece
		c.queued -= n
		c.gw.sendBytes.Add(-n)
	}
	c.sendMu.Unlock()
	_ = c.conn.SetWriteDeadline(time.Now().Add(writeWait))
	if err := c.conn.WriteMessage(frame); err != nil {
		c.Close("write: " + err.Error())
		return false
	}
	return true
}

func (c *Client) readLoop() {
	defer c.Close(closeReasonRead)
	for {
		_ = c.conn.SetReadDeadline(time.Now().Add(c.gw.cfg.Ws.PongWait))
		raw, err := c.conn.ReadMessage()
		if err != nil {
			if !errors.Is(err, errcode.ErrConnClosed) {
				log.CtxDebug(c.ctx(), "ws read: %v", err)
			}
			return
		}
		if !c.admit(raw) {
			if c.draining.Load() {
				<-c.closed // writer or the overall drain deadline owns the close
			}
			return
		}
	}
}

// admit applies per-connection limits and dispatches one inbound frame. It returns
// false when the connection must be closed.
func (c *Client) admit(raw []byte) bool {
	if c.draining.Load() || c.ctx().Err() != nil {
		return false
	}
	if !c.frames.Allow() {
		return c.overLimit(Request{})
	}
	req, err := decodeRequest(raw)
	if err != nil {
		c.overRun = 0
		_ = c.Send(req.fail(err))
		return true
	}
	// Per-frame expiry check (design §8.1): the request fails with its req_id, then 2002 and close after flush.
	if exp := c.Identity.ExpiresAt; exp > 0 && time.Now().UnixMilli() > exp {
		_ = c.Send(req.fail(errcode.ErrTokenExpired))
		c.kick(KickTokenExpired)
		return true
	}
	select {
	case c.inflight <- struct{}{}:
	default:
		return c.overLimit(req)
	}
	c.overRun = 0
	if !c.gw.beginWork() {
		<-c.inflight
		return false
	}
	go func() {
		defer c.gw.work.Done()
		defer func() {
			<-c.inflight
			if r := recover(); r != nil {
				log.CtxError(c.ctx(), "ws handler panic req_id=%d: %v\n%s", req.ReqId, r, debug.Stack())
				_ = c.Send(req.fail(errcode.ErrInternal))
			}
		}()
		// A failed send means the connection is already closing; enqueue logged and metered it.
		_ = c.Send(c.gw.dispatch(c, req))
	}()
	return true
}

func (c *Client) overLimit(req Request) bool {
	c.overRun++
	c.gw.rateLimited.Add(1)
	_ = c.Send(req.fail(errcode.ErrTooManyRequests))
	if c.overRun >= overLimitCloseAt {
		c.Close(closeReasonRate)
		return false
	}
	return true
}

// newLimiter maps the config convention "0 = unlimited" onto x/time/rate, where a zero limit allows nothing.
func newLimiter(perSec float64, burst int) *rate.Limiter {
	if perSec <= 0 {
		return rate.NewLimiter(rate.Inf, 0)
	}
	return rate.NewLimiter(rate.Limit(perSec), burst)
}
