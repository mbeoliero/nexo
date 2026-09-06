-- name: Get :one
SELECT value FROM cache
WHERE key = $1 AND (expires_at IS NULL OR expires_at > now());

-- name: MGet :many
SELECT key, value FROM cache
WHERE key = ANY(sqlc.arg(keys)::text[]) AND (expires_at IS NULL OR expires_at > now());

-- expires_at is computed on the database clock so a skewed node cannot shorten or extend a TTL;
-- ttl_seconds <= 0 means no expiry.

-- name: Set :exec
INSERT INTO cache (key, value, expires_at, updated_at)
VALUES ($1, $2, CASE WHEN sqlc.arg(ttl_seconds)::float8 > 0 THEN now() + make_interval(secs => sqlc.arg(ttl_seconds)::float8) END, now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at, updated_at = now();

-- name: SetNX :one
INSERT INTO cache (key, value, expires_at, updated_at)
VALUES ($1, $2, CASE WHEN sqlc.arg(ttl_seconds)::float8 > 0 THEN now() + make_interval(secs => sqlc.arg(ttl_seconds)::float8) END, now())
ON CONFLICT (key) DO UPDATE
SET value = EXCLUDED.value, expires_at = EXCLUDED.expires_at, updated_at = now()
WHERE cache.expires_at IS NOT NULL AND cache.expires_at <= now()
RETURNING key;

-- name: Del :exec
DELETE FROM cache WHERE key = ANY(sqlc.arg(keys)::text[]);

-- name: DelIfValue :exec
DELETE FROM cache
WHERE key = $1 AND value = $2 AND (expires_at IS NULL OR expires_at > now());

-- name: IncrBy :one
INSERT INTO cache (key, value, expires_at, updated_at)
VALUES ($1, (sqlc.arg(delta)::bigint)::text, CASE WHEN sqlc.arg(ttl_seconds)::float8 > 0 THEN now() + make_interval(secs => sqlc.arg(ttl_seconds)::float8) END, now())
ON CONFLICT (key) DO UPDATE
SET value = CASE
      WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= now() THEN EXCLUDED.value
      ELSE ((cache.value)::bigint + sqlc.arg(delta)::bigint)::text
    END,
    expires_at = CASE
      WHEN cache.expires_at IS NOT NULL AND cache.expires_at <= now() THEN EXCLUDED.expires_at
      ELSE cache.expires_at
    END,
    updated_at = now()
RETURNING value::bigint AS value;

-- name: Expire :one
UPDATE cache SET expires_at = CASE WHEN sqlc.arg(ttl_seconds)::float8 > 0 THEN now() + make_interval(secs => sqlc.arg(ttl_seconds)::float8) END, updated_at = now()
WHERE key = $1 AND (expires_at IS NULL OR expires_at > now())
RETURNING key;

-- name: Cleanup :one
WITH expired AS (
  SELECT key FROM cache
  WHERE expires_at IS NOT NULL AND expires_at <= now()
  ORDER BY expires_at
  LIMIT sqlc.arg(limit_rows)
  FOR UPDATE SKIP LOCKED
),
deleted AS (
  DELETE FROM cache c USING expired e WHERE c.key = e.key RETURNING 1
)
SELECT count(*)::bigint AS deleted_count FROM deleted;
