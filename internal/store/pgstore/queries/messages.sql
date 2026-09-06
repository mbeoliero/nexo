-- name: GetMessageByClientId :one
SELECT * FROM messages WHERE conversation_id = $1 AND sender_id = $2 AND client_msg_id = $3;

-- name: InsertMessage :execrows
INSERT INTO messages (conversation_id, seq, server_msg_id, client_msg_id, sender_id, recv_id, group_id, session_type, content_type, content, send_time, created_at)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
ON CONFLICT DO NOTHING;

-- name: SetConversationMaxSeq :exec
UPDATE conversations SET max_seq = $2, updated_at = $3 WHERE conversation_id = $1;

-- name: ListMessages :many
SELECT * FROM messages
WHERE conversation_id = $1 AND seq >= sqlc.arg(begin_seq) AND seq <= sqlc.arg(end_seq)
ORDER BY seq
LIMIT sqlc.arg(row_limit);

-- name: GetMessages :many
SELECT m.* FROM messages m
JOIN (SELECT unnest(sqlc.arg(conversation_ids)::text[]) AS conversation_id, unnest(sqlc.arg(seqs)::bigint[]) AS seq) k
  ON m.conversation_id = k.conversation_id AND m.seq = k.seq;

-- name: TouchUserConversation :exec
INSERT INTO user_conversations
  (owner_id, conversation_id, type, peer_user_id, group_id, min_seq, max_seq, read_seq, recv_msg_opt, is_pinned, extra, updated_at, created_at)
VALUES ($1, $2, $3, $4, $5, 1, 0, sqlc.arg(read_seq), 0, false, '', sqlc.arg(updated_at), sqlc.arg(updated_at))
ON CONFLICT (owner_id, conversation_id) DO UPDATE
SET updated_at = GREATEST(user_conversations.updated_at, EXCLUDED.updated_at), read_seq = GREATEST(user_conversations.read_seq, EXCLUDED.read_seq);

-- name: TouchConversationMembers :exec
UPDATE user_conversations SET updated_at = GREATEST(updated_at, sqlc.arg(updated_at)::timestamptz) WHERE conversation_id = $1 AND max_seq = 0;

-- name: AdvanceReadSeq :exec
UPDATE user_conversations SET read_seq = GREATEST(read_seq, $3) WHERE owner_id = $1 AND conversation_id = $2;

-- name: ListUserConversations :many
SELECT sqlc.embed(uc), c.max_seq AS conv_max_seq
FROM user_conversations uc
JOIN conversations c ON c.conversation_id = uc.conversation_id
WHERE uc.owner_id = $1 AND (uc.updated_at, uc.conversation_id) < (sqlc.arg(cursor_updated_at)::timestamptz, sqlc.arg(cursor_conversation_id)::text)
ORDER BY uc.updated_at DESC, uc.conversation_id DESC
LIMIT sqlc.arg(row_limit);
