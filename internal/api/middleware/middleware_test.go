package middleware

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cloudwego/hertz/pkg/app"
	hconfig "github.com/cloudwego/hertz/pkg/common/config"
	"github.com/cloudwego/hertz/pkg/common/ut"
	"github.com/cloudwego/hertz/pkg/route"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
)

func TestTraceAndAccessLog(t *testing.T) {
	var lines []string
	logAccess = func(ctx context.Context, format string, v ...any) {
		lines = append(lines, fmt.Sprintf("%v ", log.GetAllCustomFields(ctx))+fmt.Sprintf(format, v...))
	}
	t.Cleanup(func() { logAccess = log.CtxInfo })

	e := route.NewEngine(hconfig.NewOptions(nil))
	e.Use(Trace(), ProcessLogger(AccessLog{Redact: []string{"/secret"}, Body: true}))
	var seenTrace string
	e.POST("/secret", func(ctx context.Context, c *app.RequestContext) { seenTrace = TraceIdFrom(ctx); webx.OK(c, nil) })
	e.POST("/open", func(ctx context.Context, c *app.RequestContext) { webx.Fail(ctx, c, errcode.ErrForbidden) })

	const inbound = "0af7651916cd43dd8448eb211c80319c"
	body := `{"password":"hunter2secret"}`
	w := ut.PerformRequest(e, "POST", "/secret", &ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: "traceparent", Value: "00-" + inbound + "-b7ad6b7169203331-01"})
	if got := w.Header().Get(TraceHeader); got != inbound {
		t.Fatalf("traceparent not adopted: %q", got)
	}
	if seenTrace != inbound {
		t.Fatalf("handler ctx trace = %q", seenTrace)
	}
	w = ut.PerformRequest(e, "POST", "/secret", &ut.Body{Body: strings.NewReader(body), Len: len(body)},
		ut.Header{Key: TraceHeader, Value: "1234567890abcdef1234567890abcdef"})
	if got := w.Header().Get(TraceHeader); got != "1234567890abcdef1234567890abcdef" {
		t.Fatalf("Trace-Id not adopted: %q", got)
	}
	open := `{"nickname":"visible-nick"}`
	w = ut.PerformRequest(e, "POST", "/open", &ut.Body{Body: strings.NewReader(open), Len: len(open)},
		ut.Header{Key: "Authorization", Value: "Bearer junk"})
	if len(w.Header().Get(TraceHeader)) != 32 {
		t.Fatalf("minted trace id: %q", w.Header().Get(TraceHeader))
	}

	out := strings.Join(lines, "\n")
	for _, want := range []string{"trace_id:" + inbound, "status=200 code=0", "visible-nick", "status=403 code=10003"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	for _, leak := range []string{"hunter2secret", "Bearer junk"} {
		if strings.Contains(out, leak) {
			t.Errorf("leaked %q in:\n%s", leak, out)
		}
	}
}

func TestIpRateLimit(t *testing.T) {
	e := route.NewEngine(hconfig.NewOptions(nil))
	e.POST("/login", IpRateLimit(2), func(ctx context.Context, c *app.RequestContext) { webx.OK(c, nil) })
	e.POST("/open", IpRateLimit(0), func(ctx context.Context, c *app.RequestContext) { webx.OK(c, nil) })
	codes := make([]int, 3)
	for i := range codes {
		codes[i] = ut.PerformRequest(e, "POST", "/login", nil).Result().StatusCode()
	}
	if codes[0] != 200 || codes[1] != 200 || codes[2] != 429 {
		t.Fatalf("codes=%v, want 200 200 429", codes)
	}
	for range 5 {
		if got := ut.PerformRequest(e, "POST", "/open", nil).Result().StatusCode(); got != 200 {
			t.Fatalf("disabled limiter returned %d", got)
		}
	}
}

func TestInboundSpanKeepsSampledFlag(t *testing.T) {
	const tid, sid = "0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331"
	for _, tc := range []struct {
		flags   string
		sampled bool
	}{
		{flags: "01", sampled: true},
		{flags: "00"},
		{flags: "zz"}, // malformed flags cost the sampling bit, never the trace id
	} {
		c := app.NewContext(0)
		c.Request.Header.Set("traceparent", "00-"+tid+"-"+sid+"-"+tc.flags)
		sc := inboundSpan(c)
		if !sc.IsValid() || sc.TraceID().String() != tid {
			t.Errorf("flags %q: trace id = %q valid=%v", tc.flags, sc.TraceID(), sc.IsValid())
		}
		if sc.IsSampled() != tc.sampled {
			t.Errorf("flags %q: sampled = %v; want %v", tc.flags, sc.IsSampled(), tc.sampled)
		}
	}
}
