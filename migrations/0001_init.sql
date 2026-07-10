-- +goose Up

CREATE EXTENSION IF NOT EXISTS pgcrypto;


CREATE TYPE website_status AS ENUM (
    'UP',
    'DOWN',
    'TIMEOUT',
    'DNS_ERROR',
    'SSL_ERROR'
);

create table users (
    id uuid primary key default gen_random_uuid(),
    name text,
    email text unique not null,
    password_hash text not null
);


create table websites(
    id uuid primary key default gen_random_uuid(),
    name text not null,
    url text not null,
    user_id uuid not null references users(id),
    created_at timestamp not null default now(),
    updated_at timestamp not null default now()
);

CREATE INDEX idx_websites_user_id
ON websites(user_id);

create table regions(
    id uuid primary key default gen_random_uuid(),
    name text not null,
    country_code text not null
);

create table website_ticks(
    id uuid primary key,

    website_id uuid not null references websites(id),
    region_id uuid not null references regions(id),

    status website_status not null,
    response_time_ms integer not null,

    created_at timestamptz not null default now()
);

CREATE INDEX idx_ticks_website_region_created
ON website_ticks (website_id, region_id, created_at DESC);


-- +goose Down

DROP TABLE IF EXISTS website_ticks;
DROP TABLE IF EXISTS regions;
DROP TABLE IF EXISTS websites;
DROP TABLE IF EXISTS "users";
DROP TYPE IF EXISTS website_status;
