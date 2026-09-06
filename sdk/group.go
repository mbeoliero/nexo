package sdk

import (
	"context"
	"net/url"
)

type groupRequest struct {
	GroupId string `json:"group_id"`
	UserId  string `json:"user_id,omitempty"`
}

func (c *Client) CreateGroup(ctx context.Context, req CreateGroupRequest) (GroupInfo, error) {
	var g GroupInfo
	return g, c.post(ctx, "/group/create", req, &g)
}

func (c *Client) JoinGroup(ctx context.Context, groupId string) error {
	return c.post(ctx, "/group/join", groupRequest{GroupId: groupId}, nil)
}

func (c *Client) QuitGroup(ctx context.Context, groupId string) error {
	return c.post(ctx, "/group/quit", groupRequest{GroupId: groupId}, nil)
}

func (c *Client) KickGroupMember(ctx context.Context, groupId, userId string) error {
	return c.post(ctx, "/group/kick", groupRequest{GroupId: groupId, UserId: userId}, nil)
}

func (c *Client) Group(ctx context.Context, groupId string) (GroupInfo, error) {
	var g GroupInfo
	return g, c.get(ctx, "/group/info", url.Values{"group_id": {groupId}}, &g)
}

func (c *Client) GroupMembers(ctx context.Context, groupId string) ([]GroupMember, error) {
	var out struct {
		Members []GroupMember `json:"members"`
	}
	return out.Members, c.get(ctx, "/group/members", url.Values{"group_id": {groupId}}, &out)
}

// Internal group routes act as AsUser(...).

func (c *Client) InternalCreateGroup(ctx context.Context, req CreateGroupRequest, opts ...RequestOption) (GroupInfo, error) {
	var g GroupInfo
	return g, c.internalPost(ctx, "/internal/group/create", req, &g, opts)
}

func (c *Client) InternalJoinGroup(ctx context.Context, groupId string, opts ...RequestOption) error {
	return c.internalPost(ctx, "/internal/group/join", groupRequest{GroupId: groupId}, nil, opts)
}

func (c *Client) InternalKickGroupMember(ctx context.Context, groupId, userId string, opts ...RequestOption) error {
	return c.internalPost(ctx, "/internal/group/kick", groupRequest{GroupId: groupId, UserId: userId}, nil, opts)
}
