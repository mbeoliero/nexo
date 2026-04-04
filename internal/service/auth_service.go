package service

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/mbeoliero/kit/log"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"

	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/internal/entity"
	"github.com/mbeoliero/nexo/internal/repository"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/jwt"
)

// AuthService handles authentication logic
type AuthService struct {
	userRepo   *repository.UserRepo
	cfg        *config.Config
	tokenStore *jwt.TokenStore
}

// NewAuthService creates a new AuthService
func NewAuthService(userRepo *repository.UserRepo, cfg *config.Config, rdb *redis.Client) *AuthService {
	return &AuthService{
		userRepo:   userRepo,
		cfg:        cfg,
		tokenStore: jwt.NewTokenStore(rdb, cfg.JWT.ExpireHours),
	}
}

// RegisterRequest represents user registration request
type RegisterRequest struct {
	UserId   string `json:"user_id"`
	Nickname string `json:"nickname"`
	Password string `json:"password"`
	Avatar   string `json:"avatar,omitempty"`
}

// LoginRequest represents user login request
type LoginRequest struct {
	UserId     string `json:"user_id"`
	Password   string `json:"password"`
	PlatformId int    `json:"platform_id"`
}

// LoginResponse represents user login response
type LoginResponse struct {
	Token    string           `json:"token"`
	UserInfo *entity.UserInfo `json:"user_info"`
}

// Register registers a new user
func (s *AuthService) Register(ctx context.Context, req *RegisterRequest) (*entity.UserInfo, error) {
	// Check if user already exists
	exists, err := s.userRepo.Exists(ctx, req.UserId)
	if err != nil {
		log.CtxError(ctx, "check user exists failed: %v", err)
		return nil, errcode.ErrInternalServer
	}
	if exists {
		return nil, errcode.ErrUserExists
	}

	// Generate user Id if not provided
	userId := req.UserId
	if userId == "" {
		userId = uuid.New().String()
	}

	// Hash password with bcrypt
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(req.Password), bcrypt.DefaultCost)
	if err != nil {
		log.CtxError(ctx, "hash password failed: %v", err)
		return nil, errcode.ErrInternalServer
	}

	// Create user
	user := &entity.User{
		Id:       userId,
		Nickname: req.Nickname,
		Password: string(hashedPassword),
		Avatar:   req.Avatar,
	}

	if err = s.userRepo.Create(ctx, user); err != nil {
		log.CtxError(ctx, "create user failed: %v", err)
		return nil, errcode.ErrInternalServer
	}

	log.CtxInfo(ctx, "user registered: user_id=%s", userId)
	return user.ToUserInfo(), nil
}

// Login authenticates a user and returns a token
func (s *AuthService) Login(ctx context.Context, req *LoginRequest) (*LoginResponse, error) {
	// Get user
	user, err := s.userRepo.GetById(ctx, req.UserId)
	if err != nil {
		mappedErr := classifyLoginLookupError(err)
		if errors.Is(mappedErr, errcode.ErrUserNotFound) {
			log.CtxDebug(ctx, "user not found: user_id=%s, error=%v", req.UserId, err)
		} else {
			log.CtxError(ctx, "get user failed during login: user_id=%s, error=%v", req.UserId, err)
		}
		return nil, mappedErr
	}

	// Verify password with bcrypt
	if err = bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(req.Password)); err != nil {
		return nil, errcode.ErrPasswordWrong
	}

	// Generate token
	token, err := jwt.GenerateToken(user.Id, req.PlatformId, s.cfg.JWT.Secret, s.cfg.JWT.ExpireHours)
	if err != nil {
		log.CtxError(ctx, "generate token failed: %v", err)
		return nil, errcode.ErrInternalServer
	}

	// Store token in Redis
	if err = s.tokenStore.StoreToken(ctx, user.Id, req.PlatformId, token); err != nil {
		log.CtxError(ctx, "store token failed: %v", err)
		return nil, errcode.ErrInternalServer
	}

	// Kick other tokens on the same platform (single device per platform policy)
	kickedTokens, err := s.tokenStore.KickOtherTokens(ctx, user.Id, req.PlatformId, token)
	if err != nil {
		log.CtxWarn(ctx, "kick other tokens failed: %v", err)
		// Don't fail login for this
	} else if len(kickedTokens) > 0 {
		log.CtxInfo(ctx, "kicked %d tokens for user_id=%s, platform_id=%d", len(kickedTokens), user.Id, req.PlatformId)
	}

	log.CtxInfo(ctx, "user logged in: user_id=%s, platform_id=%d", user.Id, req.PlatformId)
	return &LoginResponse{
		Token:    token,
		UserInfo: user.ToUserInfo(),
	}, nil
}

func classifyLoginLookupError(err error) error {
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return errcode.ErrUserNotFound
	}
	return errcode.ErrInternalServer
}

func (s *AuthService) ValidateIssuedToken(ctx context.Context, claims *jwt.Claims, token string) error {
	if claims == nil {
		return errcode.ErrTokenInvalid
	}
	valid, err := s.tokenStore.IsTokenValid(ctx, claims.UserId, claims.PlatformId, token)
	if err != nil {
		log.CtxWarn(ctx, "check token status failed: %v", err)
		return errcode.ErrInternalServer
	}
	if !valid {
		return errcode.ErrTokenInvalid
	}
	return nil
}

// ValidateToken validates a token and returns claims
func (s *AuthService) ValidateToken(ctx context.Context, token string) (*jwt.Claims, error) {
	claims, err := jwt.ParseToken(token, s.cfg.JWT.Secret)
	if err != nil {
		return nil, err
	}
	if err := s.ValidateIssuedToken(ctx, claims, token); err != nil {
		return nil, err
	}
	return claims, nil
}

// ValidateTokenWithUser validates token and checks if user matches
func (s *AuthService) ValidateTokenWithUser(ctx context.Context, token, userId string, platformId int) (*jwt.Claims, error) {
	claims, err := jwt.ValidateToken(token, s.cfg.JWT.Secret, userId, platformId)
	if err != nil {
		return nil, err
	}
	if err := s.ValidateIssuedToken(ctx, claims, token); err != nil {
		return nil, err
	}
	return claims, nil
}

// Logout invalidates a user's token
func (s *AuthService) Logout(ctx context.Context, userId string, platformId int, token string) error {
	if err := s.tokenStore.InvalidateToken(ctx, userId, platformId, token); err != nil {
		log.CtxError(ctx, "invalidate token failed: %v", err)
		return errcode.ErrInternalServer
	}
	log.CtxInfo(ctx, "user logged out: user_id=%s, platform_id=%d", userId, platformId)
	return nil
}

// ForceLogout forces logout for a user on all platforms
func (s *AuthService) ForceLogout(ctx context.Context, userId string) error {
	if err := s.tokenStore.ForceLogoutUser(ctx, userId); err != nil {
		log.CtxError(ctx, "force logout failed: %v", err)
		return errcode.ErrInternalServer
	}
	log.CtxInfo(ctx, "user force logged out: user_id=%s", userId)
	return nil
}
