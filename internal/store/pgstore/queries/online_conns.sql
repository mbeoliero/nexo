-- name: UpsertOnlineConn :exec
INSERT INTO online_conns (conn_id, user_id, platform_id, node_id, heartbeat_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (conn_id) DO UPDATE SET user_id = EXCLUDED.user_id, platform_id = EXCLUDED.platform_id, node_id = EXCLUDED.node_id, heartbeat_at = EXCLUDED.heartbeat_at;

-- name: DeleteOnlineConn :exec
DELETE FROM online_conns WHERE conn_id = $1;

-- name: RenewOnlineConns :exec
UPDATE online_conns SET heartbeat_at = sqlc.arg(heartbeat_at)
WHERE node_id = sqlc.arg(node_id) AND conn_id = ANY(sqlc.arg(conn_ids)::text[]);

-- name: ListOnlineConns :many
SELECT * FROM online_conns WHERE user_id = ANY(sqlc.arg(user_ids)::text[]) AND heartbeat_at > sqlc.arg(since);

-- name: DeleteOnlineConnsByNode :exec
DELETE FROM online_conns WHERE node_id = $1;
