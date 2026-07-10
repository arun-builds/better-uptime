-- name: InsertWebsiteTick :exec
INSERT INTO website_ticks (
    id,
    website_id,
    region_id,
    status,
    response_time_ms,
    created_at
)
VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6
)
ON CONFLICT (id) DO NOTHING;

-- name: GetLatestTickForWebsite :one
SELECT
    website_id,
    region_id,
    status,
    response_time_ms,
    created_at
FROM website_ticks
WHERE website_id = $1
ORDER BY created_at DESC
LIMIT 1;

-- name: GetLatest100TicksForWebsite :many
SELECT
    website_id,
    region_id,
    status,
    response_time_ms,
    created_at
FROM website_ticks
WHERE website_id = $1
ORDER BY created_at DESC
LIMIT 100;

-- name: GetLast24HoursTicksForWebsite :many
SELECT
    website_id,
    region_id,
    status,
    response_time_ms,
    created_at
FROM website_ticks
WHERE website_id = $1
  AND created_at >= NOW() - INTERVAL '24 hours'
ORDER BY created_at DESC;
