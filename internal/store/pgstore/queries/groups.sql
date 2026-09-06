-- name: CreateGroup :exec
INSERT INTO chat_groups (id, name, avatar, introduction, owner_id, status, extra, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);

-- name: GetGroup :one
SELECT * FROM chat_groups WHERE id = $1;

-- name: AddGroupMember :exec
INSERT INTO group_members (group_id, user_id, role, nickname, inviter_user_id, joined_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: AddGroupMembers :copyfrom
INSERT INTO group_members (group_id, user_id, role, nickname, inviter_user_id, joined_at)
VALUES ($1, $2, $3, $4, $5, $6);

-- name: RemoveGroupMember :execrows
DELETE FROM group_members WHERE group_id = $1 AND user_id = $2;

-- name: GetGroupMember :one
SELECT * FROM group_members WHERE group_id = $1 AND user_id = $2;

-- name: ListGroupMembers :many
SELECT * FROM group_members WHERE group_id = $1 ORDER BY joined_at, user_id;

-- name: CountGroupMembers :one
SELECT count(*) FROM group_members WHERE group_id = $1;

-- name: ListUserGroupIds :many
SELECT group_id FROM group_members WHERE user_id = $1 ORDER BY joined_at;
