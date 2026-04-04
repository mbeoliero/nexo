package service

import (
	"context"

	"github.com/mbeoliero/kit/log"
	"github.com/mbeoliero/nexo/pkg/constant"
)

type OnlineStatusResult struct {
	UserId               string                  `json:"user_id"`
	Status               int                     `json:"status"`
	DetailPlatformStatus []*PlatformStatusDetail `json:"detail_platform_status,omitempty"`
}

type PlatformStatusDetail struct {
	PlatformId   int    `json:"platform_id"`
	PlatformName string `json:"platform_name"`
	ConnId       string `json:"conn_id"`
}

type PresenceConnRef struct {
	UserId     string `json:"user_id"`
	ConnId     string `json:"conn_id"`
	InstanceId string `json:"instance_id"`
	PlatformId int    `json:"platform_id"`
}

type PresenceQueryResult struct {
	Users      []*OnlineStatusResult
	Partial    bool
	DataSource string
}

type LocalPresenceReader interface {
	GetUsersLocalPresence(userIds []string) map[string][]*PlatformStatusDetail
}

type RemotePresenceReader interface {
	GetUsersPresenceConnRefs(ctx context.Context, userIds []string) (map[string][]PresenceConnRef, error)
}

type PresenceStats interface {
	IncPresencePartial()
}

type PresenceService struct {
	currentInstanceId string
	local             LocalPresenceReader
	remote            RemotePresenceReader
	stats             PresenceStats
}

func NewPresenceService(currentInstanceId string, local LocalPresenceReader, remote RemotePresenceReader) *PresenceService {
	return &PresenceService{
		currentInstanceId: currentInstanceId,
		local:             local,
		remote:            remote,
	}
}

func (s *PresenceService) SetStats(stats PresenceStats) *PresenceService {
	if s == nil {
		return nil
	}
	s.stats = stats
	return s
}

func (s *PresenceService) GetUsersOnlineStatus(ctx context.Context, userIds []string) (*PresenceQueryResult, error) {
	localPresence := map[string][]*PlatformStatusDetail{}
	if s.local != nil {
		localPresence = s.local.GetUsersLocalPresence(userIds)
	}

	remotePresence := map[string][]PresenceConnRef{}
	result := &PresenceQueryResult{}
	if s.remote != nil {
		var err error
		remotePresence, err = s.remote.GetUsersPresenceConnRefs(ctx, userIds)
		if err != nil {
			log.CtxWarn(ctx, "presence remote read degraded: instance_id=%s user_count=%d err=%v", s.currentInstanceId, len(userIds), err)
			result.Partial = true
			result.DataSource = "local_only"
			if s.stats != nil {
				s.stats.IncPresencePartial()
			}
		}
	}

	result.Users = make([]*OnlineStatusResult, 0, len(userIds))
	for _, userId := range userIds {
		seen := make(map[string]struct{})
		details := make([]*PlatformStatusDetail, 0)

		for _, detail := range localPresence[userId] {
			if detail == nil {
				continue
			}
			seen[detail.ConnId] = struct{}{}
			details = append(details, detail)
		}

		for _, conn := range remotePresence[userId] {
			if conn.InstanceId == s.currentInstanceId {
				continue
			}
			if _, ok := seen[conn.ConnId]; ok {
				continue
			}
			seen[conn.ConnId] = struct{}{}
			details = append(details, &PlatformStatusDetail{
				PlatformId:   conn.PlatformId,
				PlatformName: constant.PlatformIdToName(conn.PlatformId),
				ConnId:       conn.ConnId,
			})
		}

		status := constant.StatusOffline
		if len(details) > 0 {
			status = constant.StatusOnline
		}
		result.Users = append(result.Users, &OnlineStatusResult{
			UserId:               userId,
			Status:               status,
			DetailPlatformStatus: details,
		})
	}

	if result.Partial {
		onlineUsers := 0
		for _, user := range result.Users {
			if user != nil && user.Status == constant.StatusOnline {
				onlineUsers++
			}
		}
		log.CtxInfo(ctx,
			"presence query completed with partial data: instance_id=%s user_count=%d online_count=%d data_source=%s",
			s.currentInstanceId,
			len(userIds),
			onlineUsers,
			result.DataSource,
		)
	} else {
		onlineUsers := 0
		for _, user := range result.Users {
			if user != nil && user.Status == constant.StatusOnline {
				onlineUsers++
			}
		}
		log.CtxDebug(ctx,
			"presence query completed: instance_id=%s user_count=%d online_count=%d data_source=full",
			s.currentInstanceId,
			len(userIds),
			onlineUsers,
		)
	}

	return result, nil
}
