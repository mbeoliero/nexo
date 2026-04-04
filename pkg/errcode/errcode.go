package errcode

import "fmt"

// Error represents a business error
type Error struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}

func (e *Error) Error() string {
	return fmt.Sprintf("errcode: %d, msg: %s", e.Code, e.Msg)
}

// New creates a new error with code and message
func New(code int, msg string) *Error {
	return &Error{Code: code, Msg: msg}
}

// Wrap wraps an error with additional context
func (e *Error) Wrap(err error) *Error {
	if err == nil {
		return e
	}
	return &Error{
		Code: e.Code,
		Msg:  fmt.Sprintf("%s: %v", e.Msg, err),
	}
}

// Common error codes
var (
	// Success
	ErrSuccess = New(0, "success")

	// Common errors (1xxx)
	ErrInvalidParam       = New(1001, "invalid parameter")
	ErrInternalServer     = New(1002, "internal server error")
	ErrUnauthorized       = New(1003, "unauthorized")
	ErrForbidden          = New(1004, "forbidden")
	ErrNotFound           = New(1005, "not found")
	ErrTooManyRequests    = New(1006, "too many requests")
	ErrNoPermission       = New(1007, "no permission to access this resource")
	ErrServerShuttingDown = New(1008, "server shutting down")

	// Auth errors (2xxx)
	ErrTokenInvalid  = New(2001, "token invalid")
	ErrTokenExpired  = New(2002, "token expired")
	ErrTokenMissing  = New(2003, "token missing")
	ErrTokenMismatch = New(2004, "token user mismatch")
	ErrLoginFailed   = New(2005, "login failed")
	ErrUserNotFound  = New(2006, "user not found")
	ErrUserExists    = New(2007, "user already exists")
	ErrPasswordWrong = New(2008, "password wrong")

	// Group errors (3xxx)
	ErrGroupNotFound      = New(3001, "group not found")
	ErrGroupDismissed     = New(3002, "group has been dismissed")
	ErrNotGroupMember     = New(3003, "not a group member")
	ErrMemberNotActive    = New(3004, "member not active")
	ErrAlreadyGroupMember = New(3005, "already a group member")
	ErrNotGroupOwner      = New(3006, "not group owner")
	ErrNotGroupAdmin      = New(3007, "not group admin")
	ErrCannotKickOwner    = New(3008, "cannot kick group owner")

	// Message errors (4xxx)
	ErrMessageNotFound  = New(4001, "message not found")
	ErrMessageDuplicate = New(4002, "duplicate message")
	ErrConvNotFound     = New(4003, "conversation not found")
	ErrSeqAllocFailed   = New(4004, "seq allocation failed")
	ErrSendFailed       = New(4005, "message send failed")
	ErrPullFailed       = New(4006, "message pull failed")

	// WebSocket errors (5xxx)
	ErrConnOverLimit   = New(5001, "connection over max limit")
	ErrConnClosed      = New(5002, "connection closed")
	ErrInvalidProtocol = New(5003, "invalid protocol")
	ErrPushFailed      = New(5004, "push message failed")
)
