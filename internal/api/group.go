package api

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/service/group"
)

type groupHandler struct {
	svc *group.Service
}

type createGroupReq struct {
	Name         string   `json:"name"`
	Avatar       string   `json:"avatar"`
	Introduction string   `json:"introduction"`
	Extra        string   `json:"extra"`
	MemberIds    []string `json:"member_ids"`
}

func (h groupHandler) create(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.Bind[createGroupReq](ctx, c)
	if !ok {
		return
	}
	info, err := h.svc.Create(ctx, webx.IdentityFrom(c).UserId, group.CreateInput(req))
	webx.Respond(ctx, c, info, err)
}

type groupReq struct {
	GroupId string `json:"group_id"`
	UserId  string `json:"user_id"` // kick target
}

func bindGroup(ctx context.Context, c *app.RequestContext) (groupReq, bool) {
	return webx.BindValid(ctx, c, "group_id is required", func(r groupReq) bool { return r.GroupId != "" })
}

func (h groupHandler) join(ctx context.Context, c *app.RequestContext) {
	req, ok := bindGroup(ctx, c)
	if !ok {
		return
	}
	webx.Respond(ctx, c, nil, h.svc.Join(ctx, req.GroupId, webx.IdentityFrom(c).UserId))
}

func (h groupHandler) quit(ctx context.Context, c *app.RequestContext) {
	req, ok := bindGroup(ctx, c)
	if !ok {
		return
	}
	webx.Respond(ctx, c, nil, h.svc.Quit(ctx, req.GroupId, webx.IdentityFrom(c).UserId))
}

func (h groupHandler) kick(ctx context.Context, c *app.RequestContext) {
	req, ok := bindGroup(ctx, c)
	if !ok {
		return
	}
	if req.UserId == "" {
		webx.Fail(ctx, c, errcode.ErrInvalidParam.WithMessage("user_id is required"))
		return
	}
	webx.Respond(ctx, c, nil, h.svc.Kick(ctx, req.GroupId, webx.IdentityFrom(c).UserId, req.UserId))
}

func (h groupHandler) info(ctx context.Context, c *app.RequestContext) {
	info, err := h.svc.Get(ctx, c.Query("group_id"), webx.IdentityFrom(c).UserId)
	webx.Respond(ctx, c, info, err)
}

func (h groupHandler) members(ctx context.Context, c *app.RequestContext) {
	members, err := h.svc.Members(ctx, c.Query("group_id"), webx.IdentityFrom(c).UserId)
	webx.Respond(ctx, c, map[string]any{"members": members}, err)
}
