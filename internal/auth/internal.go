package auth

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/internal/cache"
)

const SourceInternal = "internal"

var (
	ErrServiceNotAllowed = errors.New("auth: service not allowed")
	ErrClockSkew         = errors.New("auth: timestamp outside window")
	ErrBadSignature      = errors.New("auth: bad signature")
	ErrNonceReplayed     = errors.New("auth: nonce replayed")
)

const minNonceLen = 16

// Signature string, one line per field:
//
//	service \n ts \n nonce \n METHOD \n rawPath \n rawQuery \n userId \n platformId \n hex(sha256(body))
type InternalRequest struct {
	Service    string
	Timestamp  string
	Nonce      string
	Method     string
	RawPath    string
	RawQuery   string
	UserId     string
	PlatformId string
	Body       []byte
	Signature  string
}

func (r InternalRequest) payload() string {
	sum := sha256.Sum256(r.Body)
	return strings.Join([]string{r.Service, r.Timestamp, r.Nonce, r.Method, r.RawPath, r.RawQuery, r.UserId, r.PlatformId, hex.EncodeToString(sum[:])}, "\n")
}

func Sign(secret string, r InternalRequest) string { return signPayload(secret, r.payload()) }

func signPayload(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

type Internal struct {
	secrets  []string // any of them verifies; rotation keeps old + new for a while
	services []string
	skew     time.Duration
	nonces   cache.Cache
	now      func() time.Time
}

func NewInternal(secrets []string, services []string, skew time.Duration, nonces cache.Cache) *Internal {
	return &Internal{secrets: secrets, services: services, skew: skew, nonces: nonces, now: time.Now}
}

// Verify order: service allowed → time window → constant-time signature compare → nonce SetNX.
// A nonce store failure is ErrUnavailable so callers fail closed.
func (i *Internal) Verify(ctx context.Context, r InternalRequest) error {
	if !slices.Contains(i.services, r.Service) {
		return fmt.Errorf("%w: %q", ErrServiceNotAllowed, r.Service)
	}
	ts, err := strconv.ParseInt(r.Timestamp, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: bad timestamp", ErrClockSkew)
	}
	if d := i.now().Sub(time.Unix(ts, 0)).Abs(); d > i.skew {
		return fmt.Errorf("%w: %s", ErrClockSkew, d)
	}
	if len(r.Nonce) < minNonceLen {
		return fmt.Errorf("%w: nonce too short", ErrBadSignature)
	}
	// The payload is the same for every secret; only the HMAC key rotates.
	p := r.payload()
	if !lo.SomeBy(i.secrets, func(s string) bool {
		return subtle.ConstantTimeCompare([]byte(signPayload(s, p)), []byte(r.Signature)) == 1
	}) {
		return ErrBadSignature
	}
	fresh, err := i.nonces.SetNX(ctx, cache.KeyPrefix+"inonce:"+r.Service+":"+r.Nonce, "1", 2*i.skew)
	if err != nil {
		return fmt.Errorf("%w: nonce store: %w", ErrUnavailable, err)
	}
	if !fresh {
		return ErrNonceReplayed
	}
	return nil
}
