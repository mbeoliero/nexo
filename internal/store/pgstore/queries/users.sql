-- name: GetUser :one
SELECT * FROM users WHERE id = $1;

-- name: GetUsers :many
SELECT * FROM users WHERE id = ANY(sqlc.arg(ids)::text[]);

-- name: GetUserByUsername :one
SELECT * FROM users WHERE username = $1;

-- name: CreateUser :exec
INSERT INTO users (id, username, password_hash, nickname, avatar, extra, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8);

-- name: UpdateUserProfile :execrows
UPDATE users
SET nickname = COALESCE(sqlc.narg(nickname), nickname),
    avatar = COALESCE(sqlc.narg(avatar), avatar),
    extra = COALESCE(sqlc.narg(extra), extra),
    updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(id);

-- name: UpsertUser :exec
INSERT INTO users (id, nickname, avatar, extra, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5, $5)
ON CONFLICT (id) DO UPDATE
SET nickname = EXCLUDED.nickname, avatar = EXCLUDED.avatar, extra = EXCLUDED.extra, updated_at = EXCLUDED.updated_at;
