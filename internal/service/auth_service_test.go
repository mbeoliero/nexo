package service

import (
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/pkg/errcode"
	"gorm.io/gorm"
)

func TestClassifyLoginLookupErrorReturnsUserNotFoundOnRecordNotFound(t *testing.T) {
	if got := classifyLoginLookupError(gorm.ErrRecordNotFound); got != errcode.ErrUserNotFound {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrUserNotFound)
	}
}

func TestClassifyLoginLookupErrorReturnsInternalServerOnStorageFailure(t *testing.T) {
	if got := classifyLoginLookupError(errors.New("db down")); got != errcode.ErrInternalServer {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrInternalServer)
	}
}
