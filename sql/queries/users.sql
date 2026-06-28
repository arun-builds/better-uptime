-- name: CreateUser :one
INSERT INTO users (
    name,
    email,
    password_hash
)
VALUES (
    $1,
    $2,
    $3
)
RETURNING
    id,
    name,
    email,
    password_hash;

-- name: GetUserByEmail :one
SELECT
    id,
    name,
    email,
    password_hash
FROM users
WHERE email = $1;
