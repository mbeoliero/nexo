package handler

import (
	"context"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/mbeoliero/kit/log"
	"github.com/mbeoliero/nexo/internal/middleware"
	"github.com/mbeoliero/nexo/internal/service"
	"github.com/mbeoliero/nexo/pkg/errcode"
	"github.com/mbeoliero/nexo/pkg/response"
)

// UserHandler handles user-related requests
type UserHandler struct {
	userService   *service.UserService
	presenceQuery PresenceQuery
}

type PresenceQuery interface {
	GetUsersOnlineStatus(ctx context.Context, userIDs []string) (*service.PresenceQueryResult, error)
}

// NewUserHandler creates a new UserHandler
func NewUserHandler(userService *service.UserService, presenceQuery PresenceQuery) *UserHandler {
	return &UserHandler{
		userService:   userService,
		presenceQuery: presenceQuery,
	}
}

// GetUserInfo handles get user info request
func (h *UserHandler) GetUserInfo(ctx context.Context, c *app.RequestContext) {
	userId := middleware.GetUserId(c)
	if userId == "" {
		log.CtxWarn(ctx, "http get user info unauthorized: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrUnauthorized)
		return
	}

	userInfo, err := h.userService.GetUserInfo(ctx, userId)
	if err != nil {
		log.CtxWarn(ctx, "http get user info service failed: user_id=%s path=%s err=%v", userId, c.Path(), err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http get user info succeeded: user_id=%s path=%s", userId, c.Path())

	response.Success(ctx, c, userInfo)
}

// GetUserInfoById handles get user info by Id request
func (h *UserHandler) GetUserInfoById(ctx context.Context, c *app.RequestContext) {
	userId := c.Param("user_id")
	if userId == "" {
		log.CtxWarn(ctx, "http get user info by id missing user id: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	userInfo, err := h.userService.GetUserInfo(ctx, userId)
	if err != nil {
		log.CtxWarn(ctx, "http get user info by id service failed: target_user_id=%s path=%s err=%v", userId, c.Path(), err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http get user info by id succeeded: target_user_id=%s path=%s", userId, c.Path())

	response.Success(ctx, c, userInfo)
}

// UpdateUserInfo handles update user info request
func (h *UserHandler) UpdateUserInfo(ctx context.Context, c *app.RequestContext) {
	userId := middleware.GetUserId(c)
	if userId == "" {
		log.CtxWarn(ctx, "http update user info unauthorized: path=%s", c.Path())
		response.ErrorWithCode(ctx, c, errcode.ErrUnauthorized)
		return
	}

	var req service.UpdateUserRequest
	if err := c.BindAndValidate(&req); err != nil {
		log.CtxWarn(ctx, "http update user info invalid request: user_id=%s path=%s err=%v", userId, c.Path(), err)
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	userInfo, err := h.userService.UpdateUserInfo(ctx, userId, &req)
	if err != nil {
		log.CtxWarn(ctx, "http update user info service failed: user_id=%s path=%s err=%v", userId, c.Path(), err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http update user info succeeded: user_id=%s path=%s", userId, c.Path())

	response.Success(ctx, c, userInfo)
}

// GetUsersInfoReq represents the request for batch getting users' info
type GetUsersInfoReq struct {
	UserIds []string `json:"user_ids" vd:"len($)>0,len($)<=100"`
}

// GetUsersInfo handles batch get users info request
func (h *UserHandler) GetUsersInfo(ctx context.Context, c *app.RequestContext) {
	var req GetUsersInfoReq
	if err := c.BindAndValidate(&req); err != nil {
		log.CtxWarn(ctx, "http get users info invalid request: path=%s err=%v", c.Path(), err)
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	userInfos, err := h.userService.GetUserInfos(ctx, req.UserIds)
	if err != nil {
		log.CtxWarn(ctx, "http get users info service failed: path=%s user_count=%d err=%v", c.Path(), len(req.UserIds), err)
		response.Error(ctx, c, err)
		return
	}
	log.CtxDebug(ctx, "http get users info succeeded: path=%s user_count=%d", c.Path(), len(req.UserIds))

	response.Success(ctx, c, userInfos)
}

// GetUsersOnlineStatusReq represents the request for getting users' online status
type GetUsersOnlineStatusReq struct {
	UserIds []string `json:"user_ids" vd:"len($)>0,len($)<=100"`
}

// GetUsersOnlineStatus handles get users online status request
func (h *UserHandler) GetUsersOnlineStatus(ctx context.Context, c *app.RequestContext) {
	var req GetUsersOnlineStatusReq
	if err := c.BindAndValidate(&req); err != nil {
		log.CtxWarn(ctx, "http get users online status invalid request: path=%s err=%v", c.Path(), err)
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}
	if len(req.UserIds) == 0 || len(req.UserIds) > 100 {
		log.CtxWarn(ctx, "http get users online status invalid user count: path=%s user_count=%d", c.Path(), len(req.UserIds))
		response.ErrorWithCode(ctx, c, errcode.ErrInvalidParam)
		return
	}

	results, err := h.presenceQuery.GetUsersOnlineStatus(ctx, req.UserIds)
	if err != nil {
		log.CtxWarn(ctx, "http get users online status service failed: path=%s user_count=%d err=%v", c.Path(), len(req.UserIds), err)
		response.Error(ctx, c, err)
		return
	}
	if results == nil {
		results = &service.PresenceQueryResult{}
	}

	if results.Partial {
		log.CtxInfo(ctx, "http get users online status partial response: path=%s user_count=%d data_source=%s", c.Path(), len(req.UserIds), results.DataSource)
		response.SuccessWithMeta(ctx, c, results.Users, response.Meta{
			"partial":     true,
			"data_source": results.DataSource,
		})
		return
	}
	log.CtxDebug(ctx, "http get users online status succeeded: path=%s user_count=%d", c.Path(), len(req.UserIds))

	response.Success(ctx, c, results.Users)
}
