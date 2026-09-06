// Code format K MM NN; classify by regex on the number alone:
//
//	K  = 1 business (client handles it, never alerted)
//	     2 system   (our fault or a dependency; log error, alert)   regex ^2\d{4}$
//	MM = module: 00 common 01 auth 02 user 03 group 04 message 05 conversation 06 ws
//	NN = sequence 01..99
package errcode

import (
	"errors"
	"fmt"
)

type Class int

const (
	Business Class = 1
	System   Class = 2
)

func (c Class) String() string {
	if c == System {
		return "system"
	}
	return "business"
}

type Error struct {
	Code    int
	Message string
	cause   error
}

func (e *Error) Error() string {
	if e.cause != nil {
		return fmt.Sprintf("errcode %d: %s: %v", e.Code, e.Message, e.cause)
	}
	return fmt.Sprintf("errcode %d: %s", e.Code, e.Message)
}

func (e *Error) Unwrap() error { return e.cause }

func (e *Error) Is(target error) bool {
	t, ok := target.(*Error)
	return ok && t.Code == e.Code
}

func (e *Error) Class() Class { return Class(e.Code / 10000) }

func (e *Error) IsSystem() bool { return e.Class() == System }

func New(code int, message string) *Error {
	if code < 10000 || code > 29999 || code%100 == 0 {
		panic(fmt.Sprintf("errcode: malformed code %d, want KMMNN with K in 1..2 and NN in 01..99", code))
	}
	return &Error{Code: code, Message: message}
}

func (e *Error) Wrap(cause error) *Error {
	if cause == nil {
		return e
	}
	return &Error{Code: e.Code, Message: e.Message, cause: cause}
}

func (e *Error) WithMessage(message string) *Error {
	return &Error{Code: e.Code, Message: message, cause: e.cause}
}

// Non-errcode errors become ErrInternal so they are never reported as code=0.
func From(err error) *Error {
	if err == nil {
		return nil
	}
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}
	return ErrInternal.Wrap(err)
}

// Or keeps an errcode raised further down and wraps anything else as fallback. Callers use it at a
// transaction boundary, where the closure's error may be either.
func Or(err error, fallback *Error) error {
	if _, ok := errors.AsType[*Error](err); ok {
		return err
	}
	return fallback.Wrap(err)
}

func IsSystem(err error) bool {
	e := From(err)
	return e != nil && e.IsSystem()
}

var (
	ErrInvalidParam    = New(10001, "invalid parameter")
	ErrUnauthorized    = New(10002, "unauthorized")
	ErrForbidden       = New(10003, "forbidden")
	ErrNotFound        = New(10004, "not found")
	ErrTooManyRequests = New(10005, "too many requests")
	ErrNoPermission    = New(10006, "no permission to access this resource")

	ErrInternal    = New(20001, "internal server error")
	ErrStoreFailed = New(20002, "storage unavailable")
	ErrBusFailed   = New(20003, "event bus unavailable")
	ErrTimeout     = New(20004, "request timed out")

	ErrTokenInvalid  = New(10101, "token invalid")
	ErrTokenExpired  = New(10102, "token expired")
	ErrTokenMissing  = New(10103, "token missing")
	ErrTokenMismatch = New(10104, "token user mismatch")
	ErrLoginFailed   = New(10105, "login failed")
	ErrPasswordWrong = New(10106, "password wrong")
	// ErrProviderDisabled: the operation needs an auth provider this node did not enable
	// (native login/logout/register with auth.providers=[external_jwt]).
	ErrProviderDisabled = New(10107, "auth provider disabled")
	// Native verification cannot read TokenStore: HTTP 503 (design §6.3).
	// Token writes use ErrInternal; internal nonce failures use ErrStoreFailed.
	ErrAuthUnavailable = New(20101, "authentication temporarily unavailable")

	ErrUserNotFound = New(10201, "user not found")
	ErrUserExists   = New(10202, "user already exists")

	ErrGroupNotFound      = New(10301, "group not found")
	ErrGroupDismissed     = New(10302, "group has been dismissed")
	ErrNotGroupMember     = New(10303, "not a group member")
	ErrMemberNotActive    = New(10304, "member not active")
	ErrAlreadyGroupMember = New(10305, "already a group member")
	ErrNotGroupOwner      = New(10306, "not group owner")
	ErrNotGroupAdmin      = New(10307, "not group admin")
	ErrCannotKickOwner    = New(10308, "cannot kick group owner")
	ErrGroupFull          = New(10309, "group member limit reached")

	ErrMessageNotFound       = New(10401, "message not found")
	ErrMessageDuplicate      = New(10402, "duplicate message")
	ErrMessageContentTooLong = New(10403, "message content too large")

	ErrSeqAllocFailed    = New(20401, "seq allocation failed")
	ErrMessageSendFailed = New(20402, "message send failed")
	ErrMessagePullFailed = New(20403, "message pull failed")

	ErrConversationNotFound = New(10501, "conversation not found")

	ErrConnOverLimit   = New(10601, "connection over max limit")
	ErrConnClosed      = New(10602, "connection closed")
	ErrInvalidProtocol = New(10603, "invalid protocol")
	// ErrNodeDraining: this node is shutting down. Distinct from ErrConnOverLimit so a client can
	// reconnect at once (another node will take it) instead of backing off, and so the gateway need
	// not match on message text to pick 503 over 429.
	ErrNodeDraining = New(10604, "node is shutting down")

	ErrPushFailed = New(20601, "push message failed")
)
