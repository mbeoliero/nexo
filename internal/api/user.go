package api

import (
	"context"
	"strings"

	"github.com/cloudwego/hertz/pkg/app"

	"github.com/mbeoliero/nexo/errcode"
	"github.com/mbeoliero/nexo/internal/api/webx"
	"github.com/mbeoliero/nexo/internal/identity"
	"github.com/mbeoliero/nexo/internal/service/user"
)

type userHandler struct {
	svc *user.Service
}

type registerReq struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Nickname string `json:"nickname"`
}

func (h userHandler) register(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.Bind[registerReq](ctx, c)
	if !ok {
		return
	}
	p, err := h.svc.Register(ctx, req.Username, req.Password, req.Nickname)
	webx.Respond(ctx, c, p, err)
}

type loginReq struct {
	Username   string `json:"username"`
	Password   string `json:"password"`
	PlatformId int    `json:"platform_id"`
}

func (h userHandler) login(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.BindValid(ctx, c, "username, password and platform_id are required", func(r loginReq) bool { return r.PlatformId > 0 })
	if !ok {
		return
	}
	sess, err := h.svc.Login(ctx, req.Username, req.Password, req.PlatformId)
	webx.Respond(ctx, c, sess, err)
}

func (h userHandler) logout(ctx context.Context, c *app.RequestContext) {
	webx.Respond(ctx, c, nil, h.svc.Logout(ctx, webx.IdentityFrom(c)))
}

func (h userHandler) me(ctx context.Context, c *app.RequestContext) {
	p, err := h.svc.Get(ctx, webx.IdentityFrom(c).UserId)
	webx.Respond(ctx, c, p, err)
}

func (h userHandler) updateMe(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.Bind[user.Update](ctx, c)
	if !ok {
		return
	}
	p, err := h.svc.Update(ctx, webx.IdentityFrom(c).UserId, req)
	webx.Respond(ctx, c, p, err)
}

const maxInfoIds = 100

func userIds(c *app.RequestContext) ([]string, error) {
	// strings.Split never yields an empty slice ("" splits to [""]), so a missing parameter
	// has to be caught on the raw query value.
	raw := c.Query("user_ids")
	ids := strings.Split(raw, ",")
	if raw == "" || len(ids) > maxInfoIds {
		return nil, errcode.ErrInvalidParam.WithMessage("user_ids: 1-100 comma separated ids")
	}
	for _, id := range ids {
		if !identity.Valid(id) {
			return nil, errcode.ErrInvalidParam.WithMessage("invalid user id: " + id)
		}
	}
	return ids, nil
}

func (h userHandler) onlineStatus(ctx context.Context, c *app.RequestContext) {
	ids, err := userIds(c)
	if err != nil {
		webx.Fail(ctx, c, err)
		return
	}
	items, err := h.svc.OnlineStatus(ctx, ids)
	webx.Respond(ctx, c, map[string]any{"items": items}, err)
}

func (h userHandler) info(ctx context.Context, c *app.RequestContext) {
	ids, err := userIds(c)
	if err != nil {
		webx.Fail(ctx, c, err)
		return
	}
	ps, err := h.svc.GetMany(ctx, ids)
	webx.Respond(ctx, c, map[string]any{"users": ps}, err)
}

type upsertReq struct {
	Id       string `json:"id"`
	Nickname string `json:"nickname"`
	Avatar   string `json:"avatar"`
	Extra    string `json:"extra"`
}

func (h userHandler) upsert(ctx context.Context, c *app.RequestContext) {
	req, ok := webx.Bind[upsertReq](ctx, c)
	if !ok {
		return
	}
	p, err := h.svc.Upsert(ctx, req.Id, req.Nickname, req.Avatar, req.Extra)
	webx.Respond(ctx, c, p, err)
}
