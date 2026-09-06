package auth

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/mbeoliero/nexo/internal/cache/local"
)

func TestInternalVerify(t *testing.T) {
	ctx := t.Context()
	c := local.New()
	defer c.Close()
	v := NewInternal([]string{"new-secret", "secret"}, []string{"gateway"}, 300*time.Second, c) // rotation: both accepted

	base := func() InternalRequest {
		return InternalRequest{
			Service: "gateway", Timestamp: strconv.FormatInt(time.Now().Unix(), 10), Nonce: "0123456789abcdef0123",
			Method: "POST", RawPath: "/api/v1/internal/user/upsert", RawQuery: "a=1&b=2", UserId: "u___1", PlatformId: "5",
			Body: []byte(`{"id":"u___1"}`),
		}
	}
	signed := func(r InternalRequest) InternalRequest { r.Signature = Sign("secret", r); return r }

	if err := v.Verify(ctx, signed(base())); err != nil {
		t.Fatalf("valid: %v", err)
	}
	if err := v.Verify(ctx, signed(base())); !errors.Is(err, ErrNonceReplayed) {
		t.Fatalf("replay: %v", err)
	}

	cases := map[string]struct {
		mut  func(*InternalRequest)
		want error
	}{
		"unknown service": {func(r *InternalRequest) { r.Service = "evil" }, ErrServiceNotAllowed},
		"old timestamp":   {func(r *InternalRequest) { r.Timestamp = strconv.FormatInt(time.Now().Add(-10*time.Minute).Unix(), 10) }, ErrClockSkew},
		"bad timestamp":   {func(r *InternalRequest) { r.Timestamp = "x" }, ErrClockSkew},
		"short nonce":     {func(r *InternalRequest) { r.Nonce = "abc" }, ErrBadSignature},
		"wrong secret":    {func(r *InternalRequest) { r.Signature = Sign("other", *r) }, ErrBadSignature},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r := base()
			r.Nonce = name + "-0123456789abcdef"
			tc.mut(&r)
			if r.Signature == "" {
				r.Signature = Sign("secret", r)
			}
			if err := v.Verify(ctx, r); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}

	// Signature covers user id, platform, query and body: any change after signing fails.
	tampered := map[string]func(*InternalRequest){
		"user":     func(r *InternalRequest) { r.UserId = "u___2" },
		"platform": func(r *InternalRequest) { r.PlatformId = "1" },
		"query":    func(r *InternalRequest) { r.RawQuery = "a=2" },
		"body":     func(r *InternalRequest) { r.Body = []byte(`{}`) },
		"method":   func(r *InternalRequest) { r.Method = "GET" },
	}
	for name, mut := range tampered {
		r := base()
		r.Nonce = "tamper-" + name + "-0123456789abcdef"
		r = signed(r)
		mut(&r)
		if err := v.Verify(ctx, r); !errors.Is(err, ErrBadSignature) {
			t.Errorf("tampered %s: got %v", name, err)
		}
	}
}
