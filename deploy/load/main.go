package main

import (
	"context"
	"crypto/rand"
	"encoding/json/v2"
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/gorilla/websocket"

	"github.com/mbeoliero/nexo/sdk"
)

const sendPhaseFraction = 0.9

func sendOffset(period time.Duration, user, active int) time.Duration {
	return time.Duration(float64(period) * sendPhaseFraction * float64(user) / float64(active))
}

type options struct {
	nodes           []string
	users           int
	active          int
	rate            float64
	duration        time.Duration
	ramp            time.Duration
	settle          time.Duration
	timeout         time.Duration
	workers         int
	disposable      bool
	allowMissedPush bool
}

func (o options) validate() error {
	if !o.disposable {
		return errors.New("-disposable is required: this creates users and messages; use a dedicated disposable environment")
	}
	if len(o.nodes) < 2 || o.users < 2 || o.users%2 != 0 || o.active < 1 || o.active > o.users {
		return errors.New("need at least 2 distinct nodes, an even -users >= 2, and 1 <= -active <= -users")
	}
	if o.rate <= 0 || math.IsNaN(o.rate) || math.IsInf(o.rate, 0) || o.rate > 1000 {
		return errors.New("-rate must be finite and in (0, 1000]")
	}
	if o.duration <= 0 || o.ramp < 0 || o.settle < 0 || o.timeout <= 0 || o.workers < 1 {
		return errors.New("duration, timeout and workers must be positive; ramp and settle must be nonnegative")
	}
	if o.duration.Seconds()*o.rate < 1 || o.duration.Seconds()*o.rate > 1e6 {
		return errors.New("duration * rate must yield 1..1000000 messages per active user")
	}
	seen := make(map[string]bool)
	for _, raw := range o.nodes {
		u, err := url.Parse(raw)
		if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return errors.New("nodes must be comma-separated direct http(s) node URLs")
		}
		if u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || seen[u.String()] {
			return errors.New("node URLs must be distinct origins without credentials, paths, query or fragment")
		}
		seen[u.String()] = true
	}
	return nil
}

// Each pair is cross-node; successive pairs cover every directed node offset,
// including node 1 -> node 5 in a ten-node run. Both users can send independently.
func nodeFor(user, nodes int) int {
	pair := user / 2
	left := pair % nodes
	if user%2 == 0 {
		return left
	}
	return (left + 1 + pair/nodes%(nodes-1)) % nodes
}

type participant struct {
	id      string
	token   string
	node    int
	client  *sdk.Client
	ws      *websocket.Conn
	ready   chan error
	records []record
}

type runner struct {
	opts       options
	runId      string
	users      []participant
	pairs      []sync.Mutex
	readers    sync.WaitGroup
	closing    atomic.Bool
	frozen     atomic.Bool
	connected  atomic.Int64
	sent       atomic.Int64
	acked      atomic.Int64
	pushed     atomic.Int64
	disconnect atomic.Int64
	resync     atomic.Int64
	errors     atomic.Int64
	errorMu    sync.Mutex
	samples    []string
	started    time.Time
	elapsed    time.Duration
}

func newRunner(o options) *runner {
	r := &runner{
		opts: o, runId: strings.ToLower(rand.Text()),
		users: make([]participant, o.users), pairs: make([]sync.Mutex, o.users/2),
	}
	for i := range r.users {
		r.users[i].node = nodeFor(i, len(o.nodes))
		r.users[i].ready = make(chan error, 1)
		if i < o.active {
			r.users[i].records = make([]record, int(o.duration.Seconds()*o.rate))
		}
	}
	return r
}

func (r *runner) fail(format string, args ...any) {
	r.errors.Add(1)
	r.errorMu.Lock()
	defer r.errorMu.Unlock()
	if len(r.samples) < 20 {
		r.samples = append(r.samples, fmt.Sprintf(format, args...))
	}
}

func parallelUsers(ctx context.Context, count, workers int, fn func(context.Context, int) error) error {
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	var next atomic.Int64
	var wg sync.WaitGroup
	for range min(count, workers) {
		wg.Go(func() {
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= count {
					return
				}
				if err := fn(ctx, i); err != nil {
					cancel(err)
					return
				}
			}
		})
	}
	wg.Wait()
	return context.Cause(ctx)
}

func waitUntil(ctx context.Context, at time.Time) error {
	timer := time.NewTimer(max(0, time.Until(at)))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (r *runner) run(ctx context.Context) error {
	defer func() {
		r.closing.Store(true)
		for i := range r.users {
			if ws := r.users[i].ws; ws != nil {
				_ = ws.Close()
			}
		}
		r.readers.Wait()
	}()
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = max(100, r.opts.workers*len(r.opts.nodes))
	transport.MaxIdleConnsPerHost = r.opts.workers
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: r.opts.timeout}
	password := rand.Text()
	fmt.Fprintf(os.Stderr, "provision: %d users, %d nodes (outside measured ramp/load)\n", r.opts.users, len(r.opts.nodes))
	if err := parallelUsers(ctx, r.opts.users, r.opts.workers, func(ctx context.Context, i int) error {
		return r.provision(ctx, i, httpClient, password)
	}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "connect: %d users over %s\n", r.opts.users, r.opts.ramp)
	rampStart := time.Now()
	if err := parallelUsers(ctx, r.opts.users, r.opts.workers, func(ctx context.Context, i int) error {
		if err := waitUntil(ctx, rampStart.Add(time.Duration(float64(r.opts.ramp)*float64(i)/float64(r.opts.users)))); err != nil {
			return err
		}
		return r.connect(ctx, i)
	}); err != nil {
		return err
	}
	fmt.Fprintf(os.Stderr, "send: online=%d active=%d target=%.0f messages/s duration=%s\n",
		r.connected.Load(), r.opts.active, float64(r.opts.active)*r.opts.rate, r.opts.duration)
	r.started = time.Now()
	deadline := r.started.Add(r.opts.duration)
	period := time.Duration(float64(time.Second) / r.opts.rate)
	var senders sync.WaitGroup
	for i := range r.opts.active {
		senders.Go(func() {
			offset := sendOffset(period, i, r.opts.active)
			for j := range r.users[i].records {
				at := r.started.Add(offset + time.Duration(j)*period)
				if err := waitUntil(ctx, at); err != nil {
					return
				}
				if err := r.send(i, j, at, deadline); err != nil {
					r.fail("send user=%d node=%d: %v", i, r.users[i].node+1, err)
					return
				}
			}
		})
	}
	senders.Wait()
	if err := waitUntil(ctx, deadline); err != nil {
		return err
	}
	r.elapsed = time.Since(r.started)
	fmt.Fprintf(os.Stderr, "settle: sent=%d ack=%d push=%d; wait %s\n", r.sent.Load(), r.acked.Load(), r.pushed.Load(), r.opts.settle)
	if err := waitUntil(ctx, time.Now().Add(r.opts.settle)); err != nil {
		return err
	}
	r.frozen.Store(true)
	fmt.Fprintln(os.Stderr, "verify: pull every message as BOTH participants; no sampling")
	return parallelUsers(ctx, len(r.pairs), r.opts.workers, r.verifyPair)
}

func main() {
	var o options
	var nodes string
	flag.StringVar(&nodes, "nodes", "", "comma-separated direct node origins; not load balancers")
	flag.IntVar(&o.users, "users", 10000, "online users (even; paired cross-node)")
	flag.IntVar(&o.active, "active", 1000, "users sending throughout the load phase")
	flag.Float64Var(&o.rate, "rate", 1, "messages per second per active user")
	flag.DurationVar(&o.duration, "duration", 5*time.Minute, "steady-state send duration")
	flag.DurationVar(&o.ramp, "ramp", 2*time.Minute, "connection ramp, after account provisioning")
	flag.DurationVar(&o.settle, "settle", 10*time.Second, "ACK/push drain window before full pull verification")
	flag.DurationVar(&o.timeout, "timeout", 10*time.Second, "HTTP, handshake and WS write timeout")
	flag.IntVar(&o.workers, "workers", 16, "parallel setup and final verification workers")
	flag.BoolVar(&o.disposable, "disposable", false, "confirm all node URLs belong to a disposable test environment")
	flag.BoolVar(&o.allowMissedPush, "allow-missed-push", false, "allow realtime gaps ONLY if full pull verifies every message")
	flag.Parse()
	for node := range strings.SplitSeq(nodes, ",") {
		o.nodes = append(o.nodes, strings.TrimRight(strings.TrimSpace(node), "/"))
	}
	argErr := o.validate()
	if flag.NArg() != 0 {
		argErr = errors.Join(argErr, fmt.Errorf("positional arguments are not supported: %q", flag.Args()))
	}
	if argErr != nil {
		fmt.Fprintf(os.Stderr, "invalid arguments: %v\n", argErr)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	r := newRunner(o)
	if err := r.run(ctx); err != nil {
		r.fail("run incomplete: %v", err)
	}
	report := r.report()
	if err := json.MarshalWrite(os.Stdout, report); err != nil {
		fmt.Fprintln(os.Stderr, "write report:", err)
		os.Exit(1)
	}
	fmt.Println()
	if !report.Passed {
		os.Exit(1)
	}
}
