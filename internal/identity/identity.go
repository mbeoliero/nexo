// User id format:
//
//	u___{int64}  platform user
//	ag__{int64}  platform agent
//	nx__{uuid}   native (self-hosted) account
//
// Ids contain "_", so conversation ids use ":" as separator.
package identity

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

const PrefixLength = 4

const (
	PrefixUser   = "u___"
	PrefixAgent  = "ag__"
	PrefixNative = "nx__"
)

type Role string

const (
	RoleUser  Role = "user"
	RoleAgent Role = "agent"
)

var ErrInvalidUserId = errors.New("identity: invalid user id")

type Actor struct {
	Id   int64
	Role Role
}

func (a Actor) UserId() (string, error) {
	switch a.Role {
	case RoleUser:
		return PrefixUser + strconv.FormatInt(a.Id, 10), nil
	case RoleAgent:
		return PrefixAgent + strconv.FormatInt(a.Id, 10), nil
	default:
		return "", fmt.Errorf("identity: unknown role %q", a.Role)
	}
}

func ParseActor(userId string) (Actor, error) {
	if len(userId) <= PrefixLength {
		return Actor{}, fmt.Errorf("%w: %q", ErrInvalidUserId, userId)
	}
	var role Role
	switch userId[:PrefixLength] {
	case PrefixUser:
		role = RoleUser
	case PrefixAgent:
		role = RoleAgent
	default:
		return Actor{}, fmt.Errorf("%w: unknown prefix in %q", ErrInvalidUserId, userId)
	}
	digits := userId[PrefixLength:]
	id, err := strconv.ParseInt(digits, 10, 64)
	// Canonical spelling only. ParseInt also accepts u___05, u___+5 and u___-5, which collapse to
	// the same Actor here but reach the users table as separate rows: Upsert and the X-User-Id
	// header store the string they were given, so a padded id would fork that user's history.
	if err != nil || id <= 0 || strconv.FormatInt(id, 10) != digits {
		return Actor{}, fmt.Errorf("%w: %q", ErrInvalidUserId, userId)
	}
	return Actor{Id: id, Role: role}, nil
}

func NativeUserId(uuid string) string { return PrefixNative + uuid }

func IsNative(userId string) bool { return strings.HasPrefix(userId, PrefixNative) }

func Valid(userId string) bool {
	if len(userId) <= PrefixLength || strings.ContainsRune(userId, ':') {
		return false
	}
	switch userId[:PrefixLength] {
	case PrefixUser, PrefixAgent:
		_, err := ParseActor(userId)
		return err == nil
	case PrefixNative:
		return true
	}
	return false
}
