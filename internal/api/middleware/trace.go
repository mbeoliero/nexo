package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/mbeoliero/kit/log"
	"go.opentelemetry.io/otel/trace"
)

const TraceHeader = "Trace-Id"

// Trace adopts an inbound trace id (W3C traceparent, then Trace-Id), or mints one,
// puts it on the context as an OTel span context and log field, and echoes Trace-Id.
func Trace() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		sc := inboundSpan(c)
		if !sc.IsValid() {
			sc = newSpan()
		}
		traceId := sc.TraceID().String()
		c.Header(TraceHeader, traceId)
		ctx = trace.ContextWithSpanContext(ctx, sc)
		ctx = log.AppendLogKv(ctx, log.TraceIDKey, traceId)
		c.Next(ctx)
	}
}

func inboundSpan(c *app.RequestContext) trace.SpanContext {
	// traceparent: 00-<32 hex trace>-<16 hex span>-<2 hex flags>
	if parts := strings.Split(string(c.GetHeader("traceparent")), "-"); len(parts) == 4 && parts[0] == "00" {
		tid, err1 := trace.TraceIDFromHex(parts[1])
		sid, err2 := trace.SpanIDFromHex(parts[2])
		if err1 == nil && err2 == nil {
			// Dropping the sampled bit would make every span rooted here unsampled, so the trace
			// has a hole exactly at this hop. Malformed flags are not worth losing the id over.
			var flags trace.TraceFlags
			if b, err := hex.DecodeString(parts[3]); err == nil && len(b) == 1 {
				flags = trace.TraceFlags(b[0])
			}
			return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: sid, TraceFlags: flags, Remote: true})
		}
	}
	if tid, err := trace.TraceIDFromHex(string(c.GetHeader(TraceHeader))); err == nil {
		return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: randomSpanId(), Remote: true})
	}
	return trace.SpanContext{}
}

func newSpan() trace.SpanContext {
	var tid trace.TraceID
	rand.Read(tid[:])
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: tid, SpanID: randomSpanId()})
}

func randomSpanId() trace.SpanID {
	var sid trace.SpanID
	rand.Read(sid[:])
	return sid
}

// TraceIdFrom reads the trace id set by Trace; empty outside the middleware.
func TraceIdFrom(ctx context.Context) string {
	if sc := trace.SpanContextFromContext(ctx); sc.HasTraceID() {
		return sc.TraceID().String()
	}
	return ""
}
