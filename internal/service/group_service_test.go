package service

import (
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/pkg/errcode"
	"gorm.io/gorm"
)

func TestClassifyGroupLookupErrorReturnsGroupNotFoundOnRecordNotFound(t *testing.T) {
	if got := classifyGroupLookupError(gorm.ErrRecordNotFound); got != errcode.ErrGroupNotFound {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrGroupNotFound)
	}
}

func TestClassifyGroupLookupErrorReturnsInternalServerOnStorageFailure(t *testing.T) {
	if got := classifyGroupLookupError(errors.New("db down")); got != errcode.ErrInternalServer {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrInternalServer)
	}
}

func TestClassifyGroupMemberLookupErrorReturnsNotGroupMemberOnRecordNotFound(t *testing.T) {
	if got := classifyGroupMemberLookupError(gorm.ErrRecordNotFound); got != errcode.ErrNotGroupMember {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrNotGroupMember)
	}
}

func TestClassifyGroupMemberLookupErrorReturnsInternalServerOnStorageFailure(t *testing.T) {
	if got := classifyGroupMemberLookupError(errors.New("db down")); got != errcode.ErrInternalServer {
		t.Fatalf("error mismatch: got %v want %v", got, errcode.ErrInternalServer)
	}
}
