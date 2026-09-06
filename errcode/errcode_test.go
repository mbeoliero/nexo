package errcode

import (
	"errors"
	"io"
	"testing"
)

func TestWrapKeepsChainAndCode(t *testing.T) {
	err := ErrStoreFailed.Wrap(io.EOF)
	if !errors.Is(err, ErrStoreFailed) {
		t.Fatal("wrapped error should match sentinel by code")
	}
	if !errors.Is(err, io.EOF) {
		t.Fatal("wrapped error should keep cause in chain")
	}
	if ErrStoreFailed.cause != nil {
		t.Fatal("Wrap must not mutate the sentinel")
	}
}

// The 2xxxx-is-system rule is what logs and metrics classify on (AGENTS.md).
func TestClassFromCode(t *testing.T) {
	cases := []struct {
		err *Error
		cls Class
	}{
		{ErrInvalidParam, Business},
		{ErrNotGroupMember, Business},
		{ErrSeqAllocFailed, System},
		{ErrPushFailed, System},
	}
	for _, c := range cases {
		if c.err.Class() != c.cls {
			t.Errorf("%d: class=%s, want %s", c.err.Code, c.err.Class(), c.cls)
		}
	}
}

func TestFromMapsUnknownToSystem(t *testing.T) {
	got := From(io.EOF)
	if got.Code != ErrInternal.Code || !got.IsSystem() {
		t.Fatalf("got code=%d, want 20001 system", got.Code)
	}
	if got := From(ErrForbidden.Wrap(io.EOF)); got.Code != ErrForbidden.Code {
		t.Fatalf("code = %d, want 10003", got.Code)
	}
	if From(nil) != nil {
		t.Fatal("From(nil) should be nil")
	}
}

func TestIsSystem(t *testing.T) {
	cases := map[error]bool{
		nil:                            false,
		ErrInvalidParam:                false,
		ErrInvalidParam.Wrap(io.EOF):   false,
		ErrInternal:                    true,
		ErrSeqAllocFailed.Wrap(io.EOF): true,
		io.EOF:                         true,
	}
	for err, want := range cases {
		if got := IsSystem(err); got != want {
			t.Errorf("IsSystem(%v) = %v, want %v", err, got, want)
		}
	}
}

func TestNewRejectsMalformedCode(t *testing.T) {
	for _, code := range []int{0, 1001, 30001, 10000, 100001} {
		func() {
			defer func() {
				if recover() == nil {
					t.Errorf("New(%d) should panic", code)
				}
			}()
			New(code, "x")
		}()
	}
}
