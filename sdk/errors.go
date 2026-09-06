package sdk

import (
	"errors"
	"fmt"
)

// Error carries an envelope code (1xxxx business, 2xxxx system), or an HTTP/protocol failure with Code=0.
type Error struct {
	Code       int
	Message    string
	HttpStatus int
}

func (e *Error) Error() string {
	return fmt.Sprintf("nexo: code=%d http=%d: %s", e.Code, e.HttpStatus, e.Message)
}

func (e *Error) IsBusiness() bool { return e.Code >= 10000 && e.Code < 20000 }
func (e *Error) IsSystem() bool   { return e.Code >= 20000 && e.Code < 30000 }
func (e *Error) IsAuth() bool     { return e.HttpStatus == 401 || e.HttpStatus == 403 }

// CodeOf returns the envelope code of err, or -1 when err is not a nexo Error.
func CodeOf(err error) int {
	if e, ok := errors.AsType[*Error](err); ok {
		return e.Code
	}
	return -1
}
