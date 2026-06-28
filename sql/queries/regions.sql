-- name: ListRegions :many
SELECT
    id,
    name,
    country_code
FROM regions
ORDER BY name ASC;
