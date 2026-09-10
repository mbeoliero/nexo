package gateway

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mbeoliero/kit/log"
	"golang.org/x/time/rate"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/user"
)

const (
	maxOnlineSubscriptions    = 100
	maxOnlineSubscriptionRefs = 100000
	onlineSnapshotInterval    = 30 * time.Second
	onlineStaleAfter          = 3 * onlineSnapshotInterval
	onlineRefreshMinInterval  = time.Second
	onlineRefreshWorkers      = 4
	// onlineFramesPerSecond is admission control, not a filter on finished reads: the token is taken
	// before the OnlineStore round trip, so a presence storm throttles the reads instead of paying
	// for them and discarding the results. A read that ends without a frame (a failure below the
	// stale deadline, a replaced revision) therefore still spends one; refunding it from the worker
	// is not possible, because rate.Reservation only reverses the most recent reservation.
	onlineFramesPerSecond = 200
)

type onlineSubscriptionRequest struct {
	Revision int64    `json:"revision"`
	UserIds  []string `json:"user_ids"`
}

type onlineSubscriptionAck struct {
	Revision           int64 `json:"revision"`
	SnapshotIntervalMs int64 `json:"snapshot_interval_ms"`
}

type onlineSnapshot struct {
	Revision int64               `json:"revision"`
	Items    []user.OnlineStatus `json:"items"`
	Stale    bool                `json:"stale,omitzero"`
}

type onlineSubscription struct {
	client   *Client
	revision int64
	userIds  []string
	// cancel is non-nil for exactly as long as this connection's one in-flight read (design §7.4).
	// remove cancels it but leaves it set, so cancel → resubscribe still cannot start a second one.
	cancel context.CancelFunc
	// index is the slot in onlineSubscriptions.due, -1 while not queued; container/heap owns every
	// write to it. That one slot is §7.4's one pending refresh marker per connection.
	index int
	// readyAt is the earliest admission time. requeue is its only writer and never moves a queued
	// connection later, so a waiting connection only ever advances toward the head (fair queueing).
	readyAt time.Time
	// seq orders connections that became due at the same instant, which is what a bulk
	// presence_changed produces; without it their relative heap order would be arbitrary.
	seq uint64
	// remark records an event that arrived while the read was in flight: §7.4 allows one marker per
	// connection, so it cannot be queued twice, and that read's completion queues it instead.
	remark      bool
	nextAttempt time.Time
	lastSuccess time.Time
}

// onlineDue is the refresh queue ordered by readyAt, FIFO among connections due at the same instant.
// Only its Swap, Push and Pop write onlineSubscription.index.
type onlineDue []*onlineSubscription

func (d onlineDue) Len() int { return len(d) }

func (d onlineDue) Less(i, j int) bool {
	if c := d[i].readyAt.Compare(d[j].readyAt); c != 0 {
		return c < 0
	}
	return d[i].seq < d[j].seq
}

func (d onlineDue) Swap(i, j int) {
	d[i], d[j] = d[j], d[i]
	d[i].index, d[j].index = i, j
}

func (d *onlineDue) Push(x any) {
	s := x.(*onlineSubscription)
	s.index = len(*d)
	*d = append(*d, s)
}

func (d *onlineDue) Pop() any {
	old := *d
	last := len(old) - 1
	s := old[last]
	old[last], s.index = nil, -1
	*d = old[:last]
	return s
}

// mu covers only memory and nonblocking frame enqueue, never store calls or socket close.
// A single in-flight read per connection removes the need for cached states or read tokens.
type onlineSubscriptions struct {
	mu     sync.Mutex
	active map[*Client]*onlineSubscription
	byUser map[string]map[*Client]struct{}
	// due is a min-heap whose head is the only entry the dispatcher inspects, so a bulk mark costs
	// one push per connection and a wake-up never scans connections that are not due yet.
	due     onlineDue
	seq     uint64
	reads   int // refresh slots in use, i.e. reads in flight
	refs    int
	stopped bool
	once    sync.Once
	// jobs is as deep as the refresh slots, and a job is only ever built against a free slot, so the
	// dispatcher's send cannot block while it holds mu. A finished read frees its slot before it
	// wakes the dispatcher, so the dispatcher never wakes to a full pool and parks with work due.
	jobs   chan onlineRead
	wake   chan struct{}
	frames *rate.Limiter
	// wakes counts the dispatcher passes a wake or a timer caused; an idle node performs none.
	wakes atomic.Int64
}

func newOnlineSubscriptions() *onlineSubscriptions {
	return &onlineSubscriptions{
		active: make(map[*Client]*onlineSubscription), byUser: make(map[string]map[*Client]struct{}),
		jobs: make(chan onlineRead, onlineRefreshWorkers),
		wake: make(chan struct{}, 1), frames: rate.NewLimiter(onlineFramesPerSecond, onlineFramesPerSecond),
	}
}

func (g *Gateway) setOnlineSubscriptions(c *Client, in onlineSubscriptionRequest) (onlineSubscriptionAck, error) {
	ack := onlineSubscriptionAck{Revision: in.Revision, SnapshotIntervalMs: onlineSnapshotInterval.Milliseconds()}
	if g.deps.Online == nil || g.deps.User == nil {
		return ack, errcode.ErrInvalidProtocol.WithMessage("online subscriptions unavailable")
	}
	if in.Revision <= 0 || len(in.UserIds) > maxOnlineSubscriptions {
		return ack, errcode.ErrInvalidParam.WithMessage("positive revision and at most 100 user_ids required")
	}
	for _, id := range in.UserIds {
		if !identity.Valid(id) {
			return ack, errcode.ErrInvalidParam.WithMessage("invalid user id: " + id)
		}
	}
	ids := slices.Clone(in.UserIds)
	slices.Sort(ids)
	ids = slices.Compact(ids)
	p := g.onlineSubs
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || g.closing.Load() {
		return ack, errcode.ErrNodeDraining
	}
	if c.activeCtx.Err() != nil {
		return ack, errcode.ErrConnClosed
	}
	s := c.onlineSub
	if s == nil {
		s = &onlineSubscription{client: c, index: -1}
	}
	if in.Revision < s.revision {
		return ack, errcode.ErrInvalidParam.WithMessage("old subscription revision")
	}
	if in.Revision == s.revision {
		if !slices.Equal(ids, s.userIds) {
			return ack, errcode.ErrInvalidParam.WithMessage("revision already used for another set")
		}
		return ack, nil
	}
	if p.refs-len(s.userIds)+len(ids) > maxOnlineSubscriptionRefs {
		return ack, errcode.ErrTooManyRequests
	}
	p.remove(s)
	s.revision, s.userIds = in.Revision, ids
	s.lastSuccess = time.Time{}
	c.onlineSub = s
	if len(ids) == 0 {
		return ack, nil
	}
	p.active[c] = s
	p.refs += len(ids)
	for _, id := range ids {
		if p.byUser[id] == nil {
			p.byUser[id] = make(map[*Client]struct{})
		}
		p.byUser[id][c] = struct{}{}
	}
	p.mark(s, time.Now())
	return ack, nil
}

// remove releases references and the pending marker, but retains the revision and the in-flight
// read until it exits. Even cancel → resubscribe cannot start a second read for the same connection.
func (p *onlineSubscriptions) remove(s *onlineSubscription) {
	if s.cancel != nil {
		s.cancel()
	}
	if s.index >= 0 {
		heap.Remove(&p.due, s.index)
	}
	s.remark = false
	if _, ok := p.active[s.client]; !ok {
		return
	}
	delete(p.active, s.client)
	p.refs -= len(s.userIds)
	for _, id := range s.userIds {
		delete(p.byUser[id], s.client)
		if len(p.byUser[id]) == 0 {
			delete(p.byUser, id)
		}
	}
}

func (g *Gateway) removeOnlineSubscriptions(c *Client) {
	p := g.onlineSubs
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.onlineSub != nil {
		p.remove(c.onlineSub)
		c.onlineSub = nil
	}
}

func (g *Gateway) onlineSubscriptionRefs() int64 {
	p := g.onlineSubs
	p.mu.Lock()
	defer p.mu.Unlock()
	return int64(p.refs)
}

func (p *onlineSubscriptions) notify() {
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// requeue is the only writer of readyAt. It clamps to the per-connection minimum read interval, so
// no event or revision change can bypass it, and it never moves a queued connection later, so the
// dispatcher's armed deadline stays valid and a waiting connection cannot be pushed back (§7.4).
func (p *onlineSubscriptions) requeue(s *onlineSubscription, at time.Time) {
	if at.Compare(s.nextAttempt) < 0 {
		at = s.nextAttempt
	}
	if s.index >= 0 {
		if at.Compare(s.readyAt) >= 0 {
			return
		}
		s.readyAt = at
		heap.Fix(&p.due, s.index)
	} else {
		s.readyAt, s.seq = at, p.seq
		p.seq++
		heap.Push(&p.due, s)
	}
	// Only the head can change what the dispatcher does next: it inspects nothing else, and the
	// deadline it armed for the old head is still correct for every entry behind it.
	if s.index == 0 {
		p.notify()
	}
}

// mark records the one pending refresh a connection may hold (design §7.4). A connection with a read
// in flight keeps the marker without being queued twice: that read's completion queues it instead.
func (p *onlineSubscriptions) mark(s *onlineSubscription, now time.Time) {
	if s == nil {
		return
	}
	if s.cancel != nil {
		s.remark = true
		return
	}
	p.requeue(s, now)
}

func (g *Gateway) markOnlineChanged(userIds []string) {
	p := g.onlineSubs
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.stopped || g.closing.Load() {
		return
	}
	now := time.Now()
	for _, id := range userIds {
		for c := range p.byUser[id] {
			p.mark(p.active[c], now)
		}
	}
}

type onlineRead struct {
	sub      *onlineSubscription
	revision int64
	userIds  []string
	ctx      context.Context
	cancel   context.CancelFunc
}

func (g *Gateway) startOnlineSubscriptions(ctx context.Context) {
	if g.deps.Online == nil || g.deps.User == nil {
		return
	}
	p := g.onlineSubs
	p.once.Do(func() {
		startPool(g, onlineRefreshWorkers, g.readOnline, p.jobs)
		if !g.work.begin() {
			return
		}
		go func() {
			defer g.work.done()
			g.runOnlineSubscriptions(ctx)
		}()
	})
}

// runOnlineSubscriptions dispatches due refreshes. It holds no ticker: the timer is armed only for
// the head of the queue, so a node with nothing due performs no work at all until something marks a
// connection or frees a refresh slot.
func (g *Gateway) runOnlineSubscriptions(ctx context.Context) {
	p := g.onlineSubs
	defer func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		p.stopped = true
		for c, s := range p.active {
			p.remove(s)
			c.onlineSub = nil
		}
	}()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		p.mu.Lock()
		wait := g.dispatchOnline(time.Now())
		p.mu.Unlock()
		// Go 1.23 timers deliver no stale value after Stop, so the initial fire needs no drain.
		timer.Stop()
		if wait > 0 {
			timer.Reset(wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-p.wake:
		case <-timer.C:
		}
		p.wakes.Add(1)
	}
}

// dispatchOnline admits every due connection it can and reports when the loop must run again: a
// positive duration is the next actionable moment, zero means nothing can change without a wake.
// Called with onlineSubs.mu held, it only touches memory and hands finished jobs to the pool.
func (g *Gateway) dispatchOnline(now time.Time) time.Duration {
	p := g.onlineSubs
	if g.closing.Load() || p.stopped {
		return 0
	}
	for len(p.due) > 0 {
		s := p.due[0]
		if d := s.readyAt.Sub(now); d > 0 {
			return d
		}
		if p.reads == onlineRefreshWorkers {
			return 0 // a completed read frees the slot and wakes this loop
		}
		// Take the write token before the read: throttling has to be back-pressure on OnlineStore,
		// not a discard of a round trip already paid for (§7.4). The dispatcher is the only reserver,
		// so cancelling the reservation it did not use restores the token exactly.
		r := p.frames.ReserveN(now, 1)
		if !r.OK() {
			return 0
		}
		if d := r.DelayFrom(now); d > 0 {
			r.CancelAt(now)
			return d
		}
		readCtx, stopRead := context.WithCancel(s.client.activeCtx)
		ctx, finish := g.work.op(readCtx)
		p.jobs <- onlineRead{
			sub: s, revision: s.revision, userIds: s.userIds, ctx: ctx,
			cancel: func() { stopRead(); finish() },
		}
		heap.Pop(&p.due)
		p.reads++
		s.cancel, s.remark = stopRead, false
		s.nextAttempt = now.Add(onlineRefreshMinInterval)
	}
	return 0
}

func (g *Gateway) readOnline(job onlineRead) {
	var items []user.OnlineStatus
	err := job.ctx.Err()
	if err == nil {
		items, err = g.deps.User.OnlineStatus(job.ctx, job.userIds)
	}
	// Ignore successful results returned by a cancelled operation as well as errors from it.
	if job.ctx.Err() != nil {
		err = job.ctx.Err()
	}
	job.cancel()
	slow, logErr := g.finishOnlineRead(job, items, err)
	if logErr != nil {
		log.CtxError(job.ctx, "online subscriptions read: %v", logErr)
	}
	if slow != nil {
		slow.Close(closeReasonSlow)
	}
}

// finishOnlineRead adopts one result and hands its caller the network work that must happen outside
// the subscription lock: Close removes the subscription, so closing here would deadlock (§7.4).
// A non-nil client is the slow consumer to close; a non-nil error is the read failure to log.
func (g *Gateway) finishOnlineRead(job onlineRead, items []user.OnlineStatus, readErr error) (*Client, error) {
	p := g.onlineSubs
	p.mu.Lock()
	defer func() {
		// Free the slot before waking the dispatcher, so it always finds the pool it is woken for.
		p.reads--
		p.notify()
		p.mu.Unlock()
	}()
	s := job.sub
	s.cancel = nil
	active := !p.stopped && !g.closing.Load() && s.client.activeCtx.Err() == nil
	current := s.client.onlineSub == s && len(s.userIds) > 0
	if !active || !current {
		return nil, nil
	}
	now := time.Now()
	// Requeue before the revision check, so every path that keeps the subscription also keeps it
	// queued: an active subscription is always either queued or reading, and nothing else retries.
	next := now.Add(onlineSnapshotInterval)
	if s.remark {
		next = now
	}
	s.remark = false
	p.requeue(s, next)
	if s.revision != job.revision {
		return nil, nil
	}
	g.onlineRefreshes.Add(1)
	snapshot := onlineSnapshot{Revision: s.revision, Items: items}
	if readErr != nil {
		g.onlineReadFails.Add(1)
		snapshot.Items, snapshot.Stale = []user.OnlineStatus{}, true
		if s.lastSuccess.IsZero() || now.Sub(s.lastSuccess) < onlineStaleAfter {
			return nil, readErr
		}
	} else {
		s.lastSuccess = now
	}
	if full := g.queueOnlineSnapshot(s.client, snapshot); full {
		return s.client, readErr // a full queue is a slow consumer, which the caller closes
	}
	return nil, readErr
}

// Keep assembly and enqueue in the same subscription critical section. Closing a slow socket
// must wait until after it, because Close also removes the subscription. full, not "queued", is
// what the caller acts on: it is the only outcome that has to close the connection.
func (g *Gateway) queueOnlineSnapshot(c *Client, snapshot onlineSnapshot) (full bool) {
	frame := pushFrame(OnlineChanged, snapshot)
	n := int64(len(frame))
	if !g.budget.tryTake(n) {
		g.onlinePushDropped.Add(1)
		return false
	}
	// queueFrame's error carries nothing this caller can use: it is either the queue-full error that
	// full already reports, or ErrConnClosed for a connection on its way out, which needs neither a
	// snapshot nor a close.
	full, _ = c.queueFrame(frame, n)
	return full
}
