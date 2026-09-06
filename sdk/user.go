package sdk

import (
	"context"
	"net/url"
	"strings"
)

func (c *Client) Register(ctx context.Context, req RegisterRequest) (Profile, error) {
	var p Profile
	return p, c.post(ctx, "/auth/register", req, &p)
}

// Login stores the returned token on the client.
func (c *Client) Login(ctx context.Context, req LoginRequest) (Session, error) {
	var s Session
	if err := c.post(ctx, "/auth/login", req, &s); err != nil {
		return s, err
	}
	c.SetToken(s.Token)
	return s, nil
}

func (c *Client) Logout(ctx context.Context) error {
	if err := c.post(ctx, "/auth/logout", nil, nil); err != nil {
		return err
	}
	c.SetToken("")
	return nil
}

func (c *Client) Me(ctx context.Context) (Profile, error) {
	var p Profile
	return p, c.get(ctx, "/user/me", nil, &p)
}

func (c *Client) UpdateMe(ctx context.Context, upd ProfileUpdate) (Profile, error) {
	var p Profile
	return p, c.put(ctx, "/user/me", upd, &p)
}

func idsQuery(userIds []string) url.Values {
	return url.Values{"user_ids": {strings.Join(userIds, ",")}}
}

// Users fetches up to 100 profiles.
func (c *Client) Users(ctx context.Context, userIds []string) ([]Profile, error) {
	var out struct {
		Users []Profile `json:"users"`
	}
	return out.Users, c.get(ctx, "/user/info", idsQuery(userIds), &out)
}

func (c *Client) OnlineStatus(ctx context.Context, userIds []string) ([]OnlineStatus, error) {
	var out struct {
		Items []OnlineStatus `json:"items"`
	}
	return out.Items, c.get(ctx, "/user/online_status", idsQuery(userIds), &out)
}

func (c *Client) InternalHealth(ctx context.Context) error {
	return c.internalGet(ctx, "/internal/health", nil, nil, nil)
}

// InternalUpsertUser creates or updates a platform user's profile (u___ / ag__ ids only).
func (c *Client) InternalUpsertUser(ctx context.Context, req UpsertUserRequest) (Profile, error) {
	var p Profile
	return p, c.internalPost(ctx, "/internal/user/upsert", req, &p, nil)
}

func (c *Client) InternalUsers(ctx context.Context, userIds []string) ([]Profile, error) {
	var out struct {
		Users []Profile `json:"users"`
	}
	return out.Users, c.internalGet(ctx, "/internal/user/info", idsQuery(userIds), &out, nil)
}

func (c *Client) InternalOnlineStatus(ctx context.Context, userIds []string) ([]OnlineStatus, error) {
	var out struct {
		Items []OnlineStatus `json:"items"`
	}
	return out.Items, c.internalGet(ctx, "/internal/user/online_status", idsQuery(userIds), &out, nil)
}
