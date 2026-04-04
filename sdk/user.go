package sdk

import "context"

// GetUserInfo gets the current user's info
func (c *Client) GetUserInfo(ctx context.Context) (*UserInfo, error) {
	var result UserInfo
	if err := c.get(ctx, "/user/info", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetUserInfoById gets a user's info by Id
func (c *Client) GetUserInfoById(ctx context.Context, userId string) (*UserInfo, error) {
	var result UserInfo
	if err := c.get(ctx, "/user/profile/"+userId, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// UpdateUserInfo updates the current user's info
func (c *Client) UpdateUserInfo(ctx context.Context, req *UpdateUserRequest) (*UserInfo, error) {
	var result UserInfo
	if err := c.put(ctx, "/user/update", req, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// GetUsersInfo gets multiple users' info by Ids
func (c *Client) GetUsersInfo(ctx context.Context, userIds []string) ([]*UserInfo, error) {
	var result []*UserInfo
	req := &GetUsersInfoRequest{UserIds: userIds}
	if err := c.post(ctx, "/user/batch_info", req, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// GetUsersOnlineStatus gets online status for multiple users
func (c *Client) GetUsersOnlineStatus(ctx context.Context, userIds []string) ([]*OnlineStatus, error) {
	result, err := c.GetUsersOnlineStatusResult(ctx, userIds)
	if err != nil {
		return nil, err
	}
	statuses := make([]*OnlineStatus, 0, len(result.Users))
	for _, item := range result.Users {
		if item == nil {
			continue
		}
		status := &OnlineStatus{
			UserId: item.UserId,
			Status: item.Status,
		}
		if len(item.DetailPlatformStatus) > 0 && item.DetailPlatformStatus[0] != nil {
			status.Platform = item.DetailPlatformStatus[0].PlatformName
		}
		statuses = append(statuses, status)
	}
	return statuses, nil
}

// GetUsersOnlineStatusResult gets online status with detailed platform info and response metadata.
func (c *Client) GetUsersOnlineStatusResult(ctx context.Context, userIds []string) (*UsersOnlineStatusResult, error) {
	var users []*OnlineStatusResult
	req := &GetUsersOnlineStatusRequest{UserIds: userIds}
	meta, err := c.postWithMeta(ctx, "/user/get_users_online_status", req, &users)
	if err != nil {
		return nil, err
	}
	return &UsersOnlineStatusResult{
		Users: users,
		Meta:  meta,
	}, nil
}
