package webx

import (
	"cmp"
	"context"
	"errors"
	"net/http"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/mbeoliero/kit/log"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
)

const (
	identityKey = "nexo.identity"
	codeKey     = "nexo.code"
)

type Response struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data"`
}

func OK(c *app.RequestContext, data any) {
	c.Set(codeKey, 0)
	c.JSON(http.StatusOK, Response{Code: 0, Data: cmp.Or(data, any(struct{}{}))})
}

// Bind decodes the JSON body into T, answering ErrInvalidParam with the decode error on failure.
func Bind[T any](ctx context.Context, c *app.RequestContext) (T, bool) {
	var req T
	if err := c.BindJSON(&req); err != nil {
		Fail(ctx, c, errcode.ErrInvalidParam.Wrap(err))
		return req, false
	}
	return req, true
}

// BindValid is Bind plus a required-field check. A malformed body and a missing field both answer
// msg, so the client cannot tell them apart — the shape every handler here already used.
func BindValid[T any](ctx context.Context, c *app.RequestContext, msg string, valid func(T) bool) (T, bool) {
	var req T
	if err := c.BindJSON(&req); err != nil || !valid(req) {
		Fail(ctx, c, errcode.ErrInvalidParam.WithMessage(msg))
		return req, false
	}
	return req, true
}

// Respond is the tail every handler shares: the mapped errcode on failure, data otherwise.
func Respond(ctx context.Context, c *app.RequestContext, data any, err error) {
	if err != nil {
		Fail(ctx, c, err)
		return
	}
	OK(c, data)
}

// System errors are logged with their cause; the client only sees the message.
func Fail(ctx context.Context, c *app.RequestContext, err error) {
	e := errcode.From(err)
	FailStatus(ctx, c, HttpStatus(e), err)
}

// FailStatus is Fail with an explicit HTTP status (WS handshake uses 400 / 429).
func FailStatus(ctx context.Context, c *app.RequestContext, status int, err error) {
	e := errcode.From(err)
	c.Set(codeKey, e.Code)
	if e.IsSystem() {
		log.CtxError(ctx, "%s %s: %v", c.Method(), c.Path(), err)
	} else {
		log.CtxInfo(ctx, "%s %s: %v", c.Method(), c.Path(), err)
	}
	c.JSON(status, Response{Code: e.Code, Message: e.Message, Data: struct{}{}})
}

// Abort is Fail for middleware: writes the error and stops the chain.
func Abort(ctx context.Context, c *app.RequestContext, err error) {
	Fail(ctx, c, err)
	c.Abort()
}

// HttpStatus maps an error onto the transport status. Business failures (10xxx) stay 200 and are
// distinguished by the code in the body, which is the contract the SDK and callers already use.
// System failures (20xxx) must not: a node answering 200 while its database is down stays in the
// load balancer's rotation and reports a clean error rate to every monitor above the handler.
func HttpStatus(e *errcode.Error) int {
	switch e.Code {
	case errcode.ErrUnauthorized.Code, errcode.ErrTokenInvalid.Code, errcode.ErrTokenExpired.Code, errcode.ErrTokenMissing.Code:
		return http.StatusUnauthorized
	case errcode.ErrForbidden.Code, errcode.ErrNoPermission.Code:
		return http.StatusForbidden
	case errcode.ErrAuthUnavailable.Code:
		return http.StatusServiceUnavailable
	case errcode.ErrTooManyRequests.Code:
		return http.StatusTooManyRequests
	case errcode.ErrTimeout.Code:
		return http.StatusGatewayTimeout
	}
	if e.IsSystem() {
		return http.StatusInternalServerError
	}
	return http.StatusOK
}

// AuthErr maps an auth verification failure onto the errcode the client sees; the Bearer
// middleware and the WS handshake share it.
func AuthErr(err error) error {
	switch {
	case errors.Is(err, auth.ErrTokenExpired):
		return errcode.ErrTokenExpired.Wrap(err)
	case errors.Is(err, auth.ErrUnavailable):
		return errcode.ErrAuthUnavailable.Wrap(err)
	default:
		return errcode.ErrTokenInvalid.Wrap(err)
	}
}

func SetIdentity(c *app.RequestContext, id auth.Identity) { c.Set(identityKey, id) }

func IdentityFrom(c *app.RequestContext) auth.Identity {
	id, _ := c.Get(identityKey)
	v, _ := id.(auth.Identity)
	return v
}

// CodeFrom is the errcode recorded by OK / Fail, or 0 when neither ran.
func CodeFrom(c *app.RequestContext) int {
	v, _ := c.Get(codeKey)
	code, _ := v.(int)
	return code
}
