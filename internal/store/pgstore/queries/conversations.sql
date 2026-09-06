-- name: InsertConversationIfMissing :exec
INSERT INTO conversations (conversation_id, type, group_id, max_seq, created_at, updated_at)
VALUES ($1, $2, $3, 0, $4, $4)
ON CONFLICT (conversation_id) DO NOTHING;

-- name: LockConversation :one
SELECT * FROM conversations WHERE conversation_id = $1 FOR UPDATE;

-- name: GetUserConversation :one
SELECT * FROM user_conversations WHERE owner_id = $1 AND conversation_id = $2;

-- name: GetUserConversationRow :one
SELECT sqlc.embed(uc), c.max_seq AS conv_max_seq
FROM user_conversations uc
JOIN conversations c ON c.conversation_id = uc.conversation_id
WHERE uc.owner_id = $1 AND uc.conversation_id = $2;

-- name: UpsertUserConversation :exec
INSERT INTO user_conversations
  (owner_id, conversation_id, type, peer_user_id, group_id, min_seq, max_seq, read_seq, recv_msg_opt, is_pinned, extra, updated_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $12)
ON CONFLICT (owner_id, conversation_id) DO UPDATE
SET min_seq = EXCLUDED.min_seq, max_seq = EXCLUDED.max_seq, read_seq = EXCLUDED.read_seq, updated_at = EXCLUDED.updated_at;

-- name: SetUserConversationMaxSeq :exec
UPDATE user_conversations SET max_seq = $3 WHERE owner_id = $1 AND conversation_id = $2;

-- name: DeleteUserConversation :exec
DELETE FROM user_conversations WHERE owner_id = $1 AND conversation_id = $2;

-- name: VisibleOwners :many
SELECT owner_id FROM user_conversations
WHERE conversation_id = $1 AND owner_id = ANY(sqlc.arg(owner_ids)::text[])
  AND min_seq <= sqlc.arg(seq) AND (max_seq = 0 OR sqlc.arg(seq) <= max_seq);

-- name: GetConversation :one
SELECT * FROM conversations WHERE conversation_id = $1;

-- name: SetUserConversationOpt :execrows
UPDATE user_conversations
SET recv_msg_opt = COALESCE(sqlc.narg(recv_msg_opt), recv_msg_opt),
    is_pinned = COALESCE(sqlc.narg(is_pinned), is_pinned)
WHERE owner_id = sqlc.arg(owner_id) AND conversation_id = sqlc.arg(conversation_id);

-- name: CreateUserConversations :copyfrom
INSERT INTO user_conversations
  (owner_id, conversation_id, type, peer_user_id, group_id, min_seq, max_seq, read_seq, recv_msg_opt, is_pinned, extra, updated_at, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13);

-- name: MutedOwners :many
SELECT owner_id FROM user_conversations
WHERE conversation_id = $1 AND owner_id = ANY(sqlc.arg(owner_ids)::text[]) AND recv_msg_opt <> 0;
