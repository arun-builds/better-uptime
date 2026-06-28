-- name: CreateWebsite :one
INSERT INTO websites (
    name,
    url,
    user_id
)
VALUES (
    $1,
    $2,
    $3
)
RETURNING
    id,
    name,
    url,
    user_id,
    created_at,
    updated_at;

-- name: ListWebsitesByUser :many
SELECT
    id,
    name,
    url,
    user_id,
    created_at,
    updated_at
FROM websites
WHERE user_id = $1
ORDER BY created_at DESC;

-- name: DeleteWebsite :execrows
DELETE FROM websites
WHERE id = $1
  AND user_id = $2;
