-- Warehouse bootstrap (idempotent). Executed by src/load_postgres.py.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS raw.events (
    event_id          bigint      NOT NULL,
    event_ts          timestamptz NOT NULL,
    user_id           bigint      NOT NULL,
    event_type        text        NOT NULL,
    content_id        bigint,
    first_ingest_date date        NOT NULL,
    event_date        date        NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_raw_events_event_date ON raw.events (event_date);
CREATE TABLE IF NOT EXISTS raw.events_load (LIKE raw.events);

CREATE TABLE IF NOT EXISTS raw.users_snapshot (
    user_id       bigint  NOT NULL,
    signup_date   date    NOT NULL,
    country       text,
    device        text,
    segment       text,
    is_creator    boolean,
    snapshot_date date    NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_raw_users_snapshot_date ON raw.users_snapshot (snapshot_date);
CREATE TABLE IF NOT EXISTS raw.users_snapshot_load (LIKE raw.users_snapshot);

CREATE TABLE IF NOT EXISTS raw.content (
    content_id   bigint      NOT NULL,
    creator_id   bigint      NOT NULL,
    community_id bigint,
    content_type text,
    created_ts   timestamptz NOT NULL,
    ingest_date  date        NOT NULL
);
CREATE TABLE IF NOT EXISTS raw.content_load (LIKE raw.content);

-- Which lake partitions are loaded, with a content fingerprint. A partition is reloaded when its fingerprint changes.
CREATE TABLE IF NOT EXISTS ops.loaded_partitions (
    dataset   text   NOT NULL,
    part_key  date   NOT NULL,
    row_count bigint NOT NULL,
    checksum  bigint NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, part_key)
);

-- One row per processed ingest date (from the Spark job's done-markers).
CREATE TABLE IF NOT EXISTS ops.ingest_stats (
    ingest_date             date PRIMARY KEY,
    rows_in                 bigint,
    rows_rejected           bigint,
    duplicates_within_batch bigint,
    late_rows               bigint,
    max_lag_days            integer,
    rows_written_by_ingest  bigint,
    rejected_by_reason      text,
    seconds                 double precision
);
CREATE TABLE IF NOT EXISTS ops.ingest_stats_load (LIKE ops.ingest_stats);
