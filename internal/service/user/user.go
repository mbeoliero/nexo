package user

import (
	"context"
	"errors"
	"fmt"
	"time"
	"uuid"

	"github.com/samber/lo"
	"golang.org/x/crypto/bcrypt"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/auth"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/onlinestore"
	"github.com/mbeoliero/nexo/internal/store"
)

type Service struct {
	store  store.UserStore
	native *auth.Native // nil when the native provider is disabled
	online onlinestore.OnlineStore
	now    func() time.Time
}

type OnlineStatus struct {
	UserId    string `json:"user_id"`
	Online    bool   `json:"online"`
	Platforms []int  `json:"platform_ids"`
}

// OnlineStatus reads global presence (all nodes). Without an OnlineStore everyone is offline.
func (s *Service) OnlineStatus(ctx context.Context, userIds []string) ([]OnlineStatus, error) {
	var byUser map[string][]int
	if s.online != nil {
		var err error
		if byUser, err = s.online.Online(ctx, userIds); err != nil {
			return nil, errcode.ErrStoreFailed.Wrap(err)
		}
	}
	return lo.Map(userIds, func(id string, _ int) OnlineStatus {
		p := byUser[id]
		return OnlineStatus{UserId: id, Online: len(p) > 0, Platforms: lo.Ternary(p == nil, []int{}, p)}
	}), nil
}

func (s *Service) SetOnlineStore(o onlinestore.OnlineStore) { s.online = o }

func New(st store.UserStore, native *auth.Native) *Service {
	return &Service{store: st, native: native, now: store.NowMs}
}

// requireNative guards the password-based operations. The HTTP routes are only mounted when the
// provider is on, but server.Server.User() hands this service to embedding hosts unguarded.
func (s *Service) requireNative() error {
	if s.native == nil {
		return errcode.ErrProviderDisabled.WithMessage("native auth provider is disabled")
	}
	return nil
}

type Profile struct {
	Id        string `json:"user_id"`
	Nickname  string `json:"nickname"`
	Avatar    string `json:"avatar"`
	Extra     string `json:"extra"`
	CreatedAt int64  `json:"created_at"`
	UpdatedAt int64  `json:"updated_at"`
}

func toProfile(u store.User) Profile {
	return Profile{Id: u.Id, Nickname: u.Nickname, Avatar: u.Avatar, Extra: u.Extra, CreatedAt: u.CreatedAt.UnixMilli(), UpdatedAt: u.UpdatedAt.UnixMilli()}
}

// Column widths (migrations 00001): nickname varchar(255), avatar varchar(1024); extra is text.
const (
	maxUsername = 64
	minPassword = 6
	maxPassword = 72 // bcrypt input limit
	maxNickname = 255
	maxAvatar   = 1024
)

func validateProfile(nickname, avatar, extra *string) error {
	if nickname != nil && len(*nickname) > maxNickname {
		return errcode.ErrInvalidParam.WithMessage("nickname: at most 255 bytes")
	}
	if avatar != nil && len(*avatar) > maxAvatar {
		return errcode.ErrInvalidParam.WithMessage("avatar: at most 1024 bytes")
	}
	if extra != nil && len(*extra) > store.MaxExtraBytes {
		return errcode.ErrInvalidParam.WithMessage("extra: at most 65535 bytes")
	}
	return nil
}

func (s *Service) Register(ctx context.Context, username, password, nickname string) (Profile, error) {
	if err := s.requireNative(); err != nil {
		return Profile{}, err
	}
	if len(username) == 0 || len(username) > maxUsername || len(password) < minPassword || len(password) > maxPassword {
		return Profile{}, errcode.ErrInvalidParam.WithMessage("username 1-64 chars, password 6-72 chars")
	}
	if err := validateProfile(&nickname, nil, nil); err != nil {
		return Profile{}, err
	}
	if _, err := s.store.GetUserByUsername(ctx, username); err == nil {
		return Profile{}, errcode.ErrUserExists
	} else if !errors.Is(err, store.ErrNotFound) {
		return Profile{}, errcode.ErrStoreFailed.Wrap(err)
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		return Profile{}, errcode.ErrInternal.Wrap(err)
	}
	now := s.now()
	u := store.User{
		Id:           identity.NativeUserId(uuid.NewV7().String()),
		Username:     username,
		PasswordHash: string(hash),
		Nickname:     nickname,
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	if err := s.store.CreateUser(ctx, &u); err != nil {
		return Profile{}, errcode.ErrStoreFailed.Wrap(err)
	}
	return toProfile(u), nil
}

type Session struct {
	UserId    string `json:"user_id"`
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expires_at"`
}

func (s *Service) Login(ctx context.Context, username, password string, platformId int) (Session, error) {
	if err := s.requireNative(); err != nil {
		return Session{}, err
	}
	if platformId < 1 || platformId > auth.MaxPlatformId {
		return Session{}, errcode.ErrInvalidParam.WithMessage(fmt.Sprintf("platform_id must be 1..%d", auth.MaxPlatformId))
	}
	u, err := s.store.GetUserByUsername(ctx, username)
	if errors.Is(err, store.ErrNotFound) {
		return Session{}, errcode.ErrLoginFailed
	}
	if err != nil {
		return Session{}, errcode.ErrStoreFailed.Wrap(err)
	}
	if bcrypt.CompareHashAndPassword([]byte(u.PasswordHash), []byte(password)) != nil {
		return Session{}, errcode.ErrLoginFailed
	}
	token, exp, err := s.native.Issue(ctx, u.Id, platformId)
	if err != nil {
		return Session{}, errcode.ErrInternal.Wrap(err)
	}
	return Session{UserId: u.Id, Token: token, ExpiresAt: exp.UnixMilli()}, nil
}

func (s *Service) Logout(ctx context.Context, id auth.Identity) error {
	if err := s.requireNative(); err != nil {
		return err
	}
	if id.Source != auth.SourceNative {
		return errcode.ErrInvalidParam.WithMessage("logout applies to native tokens only")
	}
	if id.TokenId == "" {
		return errcode.ErrInvalidParam.WithMessage("token_id is required for logout")
	}
	if err := s.native.Revoke(ctx, id.UserId, id.PlatformId, id.TokenId); err != nil {
		return errcode.ErrInternal.Wrap(err)
	}
	return nil
}

func (s *Service) Get(ctx context.Context, userId string) (Profile, error) {
	u, err := s.store.GetUser(ctx, userId)
	if errors.Is(err, store.ErrNotFound) {
		return Profile{}, errcode.ErrUserNotFound
	}
	if err != nil {
		return Profile{}, errcode.ErrStoreFailed.Wrap(err)
	}
	return toProfile(*u), nil
}

func (s *Service) GetMany(ctx context.Context, userIds []string) ([]Profile, error) {
	users, err := s.store.GetUsers(ctx, userIds)
	if err != nil {
		return nil, errcode.ErrStoreFailed.Wrap(err)
	}
	return lo.Map(users, func(u store.User, _ int) Profile { return toProfile(u) }), nil
}

type Update struct {
	Nickname *string `json:"nickname"`
	Avatar   *string `json:"avatar"`
	Extra    *string `json:"extra"`
}

func (s *Service) Update(ctx context.Context, userId string, in Update) (Profile, error) {
	if err := validateProfile(in.Nickname, in.Avatar, in.Extra); err != nil {
		return Profile{}, err
	}
	err := s.store.UpdateUserProfile(ctx, userId, in.Nickname, in.Avatar, in.Extra, s.now())
	if errors.Is(err, store.ErrNotFound) {
		return Profile{}, errcode.ErrUserNotFound
	}
	if err != nil {
		return Profile{}, errcode.ErrStoreFailed.Wrap(fmt.Errorf("update user: %w", err))
	}
	return s.Get(ctx, userId)
}

// Upsert is the platform's write path for u___ / ag__ users; native ids are rejected.
func (s *Service) Upsert(ctx context.Context, id, nickname, avatar, extra string) (Profile, error) {
	if _, err := identity.ParseActor(id); err != nil {
		return Profile{}, errcode.ErrInvalidParam.WithMessage("id must be a platform user id")
	}
	if err := validateProfile(&nickname, &avatar, &extra); err != nil {
		return Profile{}, err
	}
	u := store.User{Id: id, Nickname: nickname, Avatar: avatar, Extra: extra, UpdatedAt: s.now()}
	if err := s.store.UpsertUser(ctx, &u); err != nil {
		return Profile{}, errcode.ErrStoreFailed.Wrap(err)
	}
	return s.Get(ctx, id)
}
