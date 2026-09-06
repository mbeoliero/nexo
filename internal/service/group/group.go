package group

import (
	"context"
	"crypto/rand"
	"encoding/base32"
	"errors"
	"time"

	"github.com/samber/lo"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/conv"
	"github.com/mbeoliero/nexo/internal/store"
)

// Notifier is called after a committed membership change; the bus implements it in phase 6.
type Notifier interface {
	GroupChanged(ctx context.Context, groupId string)
}

type NoopNotifier struct{}

func (NoopNotifier) GroupChanged(context.Context, string) {}

// notifyTimeout bounds the post-commit membership broadcast; see notifyChanged.
const notifyTimeout = 5 * time.Second

// notifyChanged runs the invalidation broadcast on a context that outlives the request. The
// membership change is already committed, so a client that walks away mid-call must not leave
// every other node serving a stale member cache for a whole TTL (conversation.MarkRead and
// message.Send do the same). Bounded so a stuck bus cannot hold the handler open either.
func (s *Service) notifyChanged(ctx context.Context, groupId string) {
	nctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), notifyTimeout)
	defer cancel()
	s.notify.GroupChanged(nctx, groupId)
}

type Service struct {
	store      Store
	notify     Notifier
	maxMembers int
	now        func() time.Time
}

func New(st Store, notify Notifier, maxMembers int) *Service {
	return &Service{store: st, notify: notify, maxMembers: maxMembers, now: store.NowMs}
}

type Info struct {
	Id           string `json:"group_id"`
	Name         string `json:"name"`
	Avatar       string `json:"avatar"`
	Introduction string `json:"introduction"`
	OwnerId      string `json:"owner_id"`
	Status       int32  `json:"status"`
	Extra        string `json:"extra"`
	MemberCount  int64  `json:"member_count"`
	CreatedAt    int64  `json:"created_at"`
	UpdatedAt    int64  `json:"updated_at"`
}

type Member struct {
	UserId        string `json:"user_id"`
	Role          int32  `json:"role"`
	Nickname      string `json:"nickname"`
	InviterUserId string `json:"inviter_user_id"`
	JoinedAt      int64  `json:"joined_at"`
}

type CreateInput struct {
	Name         string
	Avatar       string
	Introduction string
	Extra        string
	MemberIds    []string
}

// Column widths (migrations 00001): name varchar(255), avatar / introduction varchar(1024).
const (
	maxName         = 255
	maxAvatar       = 1024
	maxIntroduction = 1024
)

func (s *Service) Create(ctx context.Context, ownerId string, in CreateInput) (Info, error) {
	switch {
	case in.Name == "":
		return Info{}, errcode.ErrInvalidParam.WithMessage("name is required")
	case len(in.Name) > maxName:
		return Info{}, errcode.ErrInvalidParam.WithMessage("name: at most 255 bytes")
	case len(in.Avatar) > maxAvatar:
		return Info{}, errcode.ErrInvalidParam.WithMessage("avatar: at most 1024 bytes")
	case len(in.Introduction) > maxIntroduction:
		return Info{}, errcode.ErrInvalidParam.WithMessage("introduction: at most 1024 bytes")
	case len(in.Extra) > store.MaxExtraBytes:
		return Info{}, errcode.ErrInvalidParam.WithMessage("extra: at most 65535 bytes")
	}
	ids := lo.Uniq(append([]string{ownerId}, in.MemberIds...))
	if !lo.EveryBy(ids, identity.Valid) {
		return Info{}, errcode.ErrInvalidParam.WithMessage("invalid member id")
	}
	if len(ids) > s.maxMembers {
		return Info{}, errcode.ErrGroupFull
	}
	existing, err := s.store.GetUsers(ctx, ids)
	if err != nil {
		return Info{}, errcode.ErrStoreFailed.Wrap(err)
	}
	if len(existing) != len(ids) {
		return Info{}, errcode.ErrUserNotFound
	}

	now := s.now()
	g := store.Group{Id: shortId(), Name: in.Name, Avatar: in.Avatar, Introduction: in.Introduction, OwnerId: ownerId, Extra: in.Extra, CreatedAt: now, UpdatedAt: now}
	members := lo.Map(ids, func(id string, _ int) store.GroupMember {
		return store.GroupMember{GroupId: g.Id, UserId: id, Role: lo.Ternary[int32](id == ownerId, store.RoleOwner, store.RoleMember), InviterUserId: lo.Ternary(id == ownerId, "", ownerId), JoinedAt: now}
	})
	conv := conv.Group(g.Id)
	err = s.store.WithTx(ctx, func(tx Tx) error {
		if err := tx.CreateGroup(ctx, &g, members); err != nil {
			return err
		}
		if _, err := tx.LockConversation(ctx, conv, store.ConversationGroup, g.Id, now); err != nil {
			return err
		}
		// Initial members see the whole history: min_seq = 1. One bulk insert (design §12).
		return tx.CreateUserConversations(ctx, lo.Map(ids, func(id string, _ int) store.UserConversation {
			return store.UserConversation{OwnerId: id, ConversationId: conv, Type: store.ConversationGroup, GroupId: g.Id, MinSeq: 1, UpdatedAt: now}
		}))
	})
	if err != nil {
		return Info{}, errcode.ErrStoreFailed.Wrap(err)
	}
	s.notifyChanged(ctx, g.Id)
	return toInfo(g, int64(len(ids))), nil
}

func (s *Service) Join(ctx context.Context, groupId, userId string) error {
	g, err := s.activeGroup(ctx, groupId)
	if err != nil {
		return err
	}
	if _, err := s.store.GetUser(ctx, userId); errors.Is(err, store.ErrNotFound) {
		return errcode.ErrUserNotFound
	} else if err != nil {
		return errcode.ErrStoreFailed.Wrap(err)
	}
	now := s.now()
	conv := conv.Group(g.Id)
	err = s.store.WithTx(ctx, func(tx Tx) error {
		c, err := tx.LockConversation(ctx, conv, store.ConversationGroup, g.Id, now)
		if err != nil {
			return err
		}
		if _, err := tx.GetGroupMember(ctx, g.Id, userId); err == nil {
			return errcode.ErrAlreadyGroupMember
		} else if !errors.Is(err, store.ErrNotFound) {
			return err
		}
		n, err := tx.CountGroupMembers(ctx, g.Id)
		if err != nil {
			return err
		}
		if n >= int64(s.maxMembers) {
			return errcode.ErrGroupFull
		}
		if err := tx.AddGroupMember(ctx, &store.GroupMember{GroupId: g.Id, UserId: userId, Role: store.RoleMember, JoinedAt: now}); err != nil {
			return err
		}
		// Joiner sees only messages after this point; read cursor starts at the current max.
		return tx.UpsertUserConversation(ctx, &store.UserConversation{OwnerId: userId, ConversationId: conv, Type: store.ConversationGroup, GroupId: g.Id, MinSeq: c.MaxSeq + 1, ReadSeq: c.MaxSeq, UpdatedAt: now})
	})
	if err != nil {
		return errcode.Or(err, errcode.ErrStoreFailed)
	}
	s.notifyChanged(ctx, g.Id)
	return nil
}

func (s *Service) Quit(ctx context.Context, groupId, userId string) error {
	g, err := s.activeGroup(ctx, groupId)
	if err != nil {
		return err
	}
	if g.OwnerId == userId {
		return errcode.ErrInvalidParam.WithMessage("owner cannot quit the group")
	}
	return s.remove(ctx, g, userId, userId)
}

func (s *Service) Kick(ctx context.Context, groupId, operatorId, targetId string) error {
	g, err := s.activeGroup(ctx, groupId)
	if err != nil {
		return err
	}
	if targetId == g.OwnerId {
		return errcode.ErrCannotKickOwner
	}
	if operatorId == targetId {
		return errcode.ErrInvalidParam.WithMessage("use quit to leave the group")
	}
	return s.remove(ctx, g, operatorId, targetId)
}

// remove deletes membership and freezes the visible upper bound at the current max_seq. A group
// with no message yet has max_seq 0, which would read as "no upper bound", so that row is deleted
// instead (design §4.2).
func (s *Service) remove(ctx context.Context, g *store.Group, operatorId, userId string) error {
	conv := conv.Group(g.Id)
	now := s.now()
	err := s.store.WithTx(ctx, func(tx Tx) error {
		c, err := tx.LockConversation(ctx, conv, store.ConversationGroup, g.Id, now)
		if err != nil {
			return err
		}
		// Kick rejects self-removal; equal IDs identify the Quit path.
		if operatorId != userId {
			op, err := member(ctx, tx, g.Id, operatorId)
			if err != nil {
				return err
			}
			if op.Role < store.RoleAdmin {
				return errcode.ErrNotGroupAdmin
			}
			target, err := member(ctx, tx, g.Id, userId)
			if err != nil {
				return err
			}
			if op.Role == store.RoleAdmin && target.Role >= store.RoleAdmin {
				return errcode.ErrNoPermission
			}
		}
		removed, err := tx.RemoveGroupMember(ctx, g.Id, userId)
		if err != nil {
			return err
		}
		if !removed {
			return errcode.ErrNotGroupMember
		}
		if c.MaxSeq == 0 {
			return tx.DeleteUserConversation(ctx, userId, conv)
		}
		return tx.SetUserConversationMaxSeq(ctx, userId, conv, c.MaxSeq)
	})
	if err != nil {
		return errcode.Or(err, errcode.ErrStoreFailed)
	}
	s.notifyChanged(ctx, g.Id)
	return nil
}

func (s *Service) Get(ctx context.Context, groupId, userId string) (Info, error) {
	g, err := s.group(ctx, groupId)
	if err != nil {
		return Info{}, err
	}
	if _, err := member(ctx, s.store, g.Id, userId); err != nil {
		return Info{}, err
	}
	n, err := s.store.CountGroupMembers(ctx, g.Id)
	if err != nil {
		return Info{}, errcode.ErrStoreFailed.Wrap(err)
	}
	return toInfo(*g, n), nil
}

func (s *Service) Members(ctx context.Context, groupId, userId string) ([]Member, error) {
	g, err := s.group(ctx, groupId)
	if err != nil {
		return nil, err
	}
	if _, err := member(ctx, s.store, g.Id, userId); err != nil {
		return nil, err
	}
	rows, err := s.store.ListGroupMembers(ctx, g.Id)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return lo.Map(rows, func(m store.GroupMember, _ int) Member {
		return Member{UserId: m.UserId, Role: m.Role, Nickname: m.Nickname, InviterUserId: m.InviterUserId, JoinedAt: m.JoinedAt.UnixMilli()}
	}), nil
}

func (s *Service) group(ctx context.Context, groupId string) (*store.Group, error) {
	g, err := s.store.GetGroup(ctx, groupId)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errcode.ErrGroupNotFound
	}
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return g, nil
}

func (s *Service) activeGroup(ctx context.Context, groupId string) (*store.Group, error) {
	g, err := s.group(ctx, groupId)
	if err != nil {
		return nil, err
	}
	if g.Status == store.GroupStatusDismissed {
		return nil, errcode.ErrGroupDismissed
	}
	return g, nil
}

func member(ctx context.Context, st Tx, groupId, userId string) (*store.GroupMember, error) {
	m, err := st.GetGroupMember(ctx, groupId, userId)
	if errors.Is(err, store.ErrNotFound) {
		return nil, errcode.ErrNotGroupMember
	}
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return m, nil
}

func toInfo(g store.Group, n int64) Info {
	return Info{Id: g.Id, Name: g.Name, Avatar: g.Avatar, Introduction: g.Introduction, OwnerId: g.OwnerId, Status: g.Status, Extra: g.Extra, MemberCount: n, CreatedAt: g.CreatedAt.UnixMilli(), UpdatedAt: g.UpdatedAt.UnixMilli()}
}

// shortId is the group id: 16 lowercase base32 chars from 10 random bytes; no node coordination needed.
// The alphabet is RFC 4648 base32 in lowercase, which saves a case-folding pass over the output.
var base32Lower = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func shortId() string {
	var b [10]byte
	// crypto/rand.Read never returns an error as of Go 1.24; panic matches sdk.newConnId, and a
	// silently non-random group id would be worse than a crash.
	if _, err := rand.Read(b[:]); err != nil {
		panic("group: crypto/rand failed: " + err.Error())
	}
	return base32Lower.EncodeToString(b[:])
}
