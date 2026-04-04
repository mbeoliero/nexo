package service

import (
	"errors"
	"testing"

	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"gorm.io/gorm"
)

func TestResolvePullRangeReturnsInternalServerOnSeqUserReadError(t *testing.T) {
	_, _, err := resolvePullRange(1, 0, 10, nil, errcode.ErrInternalServer)
	if err != errcode.ErrInternalServer {
		t.Fatalf("expected internal server error, got %v", err)
	}
}

func TestResolvePullRangeClampsToVisibleRange(t *testing.T) {
	seqUser := &entity.SeqUser{
		MinSeq:  3,
		MaxSeq:  7,
		ReadSeq: 4,
	}

	beginSeq, endSeq, err := resolvePullRange(1, 0, 10, seqUser, nil)
	if err != nil {
		t.Fatalf("resolve pull range: %v", err)
	}
	if beginSeq != 3 {
		t.Fatalf("begin seq mismatch: got %d want %d", beginSeq, 3)
	}
	if endSeq != 7 {
		t.Fatalf("end seq mismatch: got %d want %d", endSeq, 7)
	}
}

func TestResolveGroupMembershipLookupReturnsMissingOnRecordNotFound(t *testing.T) {
	found, err := resolveGroupMembershipLookup(gorm.ErrRecordNotFound)
	if err != nil {
		t.Fatalf("resolve group membership lookup: %v", err)
	}
	if found {
		t.Fatal("expected member lookup to report missing member")
	}
}

func TestResolveGroupMembershipLookupReturnsStorageError(t *testing.T) {
	found, err := resolveGroupMembershipLookup(errors.New("db down"))
	if err == nil {
		t.Fatal("expected storage error")
	}
	if found {
		t.Fatal("expected found=false on storage error")
	}
}
