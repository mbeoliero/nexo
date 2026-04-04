package middleware

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/internal/config"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/jwt"
	"github.com/mbeoliero/nexo/pkg/response"
)

var nativeTokenStateValidator func(context.Context, *jwt.Claims, string) error

const (
	// AuthorizationHeader is the header key for authorization
	AuthorizationHeader = "Authorization"
	// BearerPrefix is the prefix for bearer token
	BearerPrefix = "Bearer "
	// UserIdKey is the context key for user Id
	UserIdKey = "user_id"
	// PlatformIdKey is the context key for platform Id
	PlatformIdKey = "platform_id"
)

// JWTAuth is the JWT authentication middleware
func JWTAuth() app.HandlerFunc {
	return func(ctx context.Context, c *app.RequestContext) {
		authHeader := string(c.GetHeader(AuthorizationHeader))
		if authHeader == "" {
			response.ErrorWithCode(ctx, c, errcode.ErrTokenMissing)
			c.Abort()
			return
		}

		if !strings.HasPrefix(authHeader, BearerPrefix) {
			response.ErrorWithCode(ctx, c, errcode.ErrTokenInvalid)
			c.Abort()
			return
		}

		tokenString := strings.TrimPrefix(authHeader, BearerPrefix)
		claims, err := ParseTokenWithFallback(ctx, tokenString, config.GlobalConfig)
		if err != nil {
			response.Error(ctx, c, err)
			c.Abort()
			return
		}

		// Store user info in context
		c.Set(UserIdKey, claims.UserId)
		c.Set(PlatformIdKey, claims.PlatformId)

		c.Next(ctx)
	}
}

func SetNativeTokenStateValidator(validator func(context.Context, *jwt.Claims, string) error) {
	nativeTokenStateValidator = validator
}

// ParseTokenWithFallback tries nexo token first, then falls back to external token if enabled.
func ParseTokenWithFallback(ctx context.Context, tokenString string, cfg *config.Config) (*jwt.Claims, error) {
	if cfg == nil {
		return nil, errcode.ErrInternalServer
	}

	// Try nexo native token first
	claims, err := jwt.ParseToken(tokenString, cfg.JWT.Secret)
	if err == nil {
		if nativeTokenStateValidator != nil {
			if err := nativeTokenStateValidator(ctx, claims, tokenString); err != nil {
				return nil, err
			}
		}
		return claims, nil
	}

	// Fall back to external token if enabled
	if cfg.ExternalJWT.Enabled {
		return jwt.ParseExternalToken(
			tokenString,
			cfg.ExternalJWT.Secret,
			cfg.ExternalJWT.DefaultRole,
			cfg.ExternalJWT.DefaultPlatformId,
		)
	}

	return nil, err
}

// GetUserId gets user Id from context
func GetUserId(c *app.RequestContext) string {
	if v, ok := c.Get(UserIdKey); ok {
		return v.(string)
	}
	return ""
}

// GetPlatformId gets platform Id from context
func GetPlatformId(c *app.RequestContext) int {
	if v, ok := c.Get(PlatformIdKey); ok {
		return v.(int)
	}
	return 0
}
