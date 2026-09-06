-- Sawari v2 — schema (Week 1)
-- Run against the timescaledb-ha image (Postgres 16 + TimescaleDB + PostGIS).
-- Idempotent: safe to re-run.

CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

-- ---------------------------------------------------------------- vessels
CREATE TABLE IF NOT EXISTS vessels (
    mmsi          BIGINT PRIMARY KEY,
    name          TEXT,
    call_sign     TEXT,
    imo           BIGINT,
    ship_type     SMALLINT,          -- AIS numeric type code
    length_m      REAL,
    width_m       REAL,
    draught_m     REAL,
    destination   TEXT,
    first_seen    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- -------------------------------------------------------------- positions
-- Time-series hot path. No surrogate key: (mmsi, time) is the natural one.
CREATE TABLE IF NOT EXISTS positions (
    time          TIMESTAMPTZ      NOT NULL,
    mmsi          BIGINT           NOT NULL,
    geom          GEOMETRY(Point, 4326) NOT NULL,
    sog           REAL,             -- speed over ground, knots
    cog           REAL,             -- course over ground, degrees
    heading       SMALLINT,         -- true heading, degrees (511 = unavailable)
    nav_status    SMALLINT,
    ingested_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
);

SELECT create_hypertable('positions', 'time', if_not_exists => TRUE);

-- Dedupe guard: AIS repeats the same report across receivers.
CREATE UNIQUE INDEX IF NOT EXISTS positions_mmsi_time_uniq
    ON positions (mmsi, time DESC);

CREATE INDEX IF NOT EXISTS positions_geom_gist
    ON positions USING GIST (geom);

-- Disk survival on a small VPS. This is a DESIGN.md entry, not an afterthought.
SELECT add_retention_policy('positions', INTERVAL '90 days', if_not_exists => TRUE);

-- Compress anything older than 7 days (Timescale native compression).
ALTER TABLE positions SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'mmsi',
    timescaledb.compress_orderby   = 'time DESC'
);
SELECT add_compression_policy('positions', INTERVAL '7 days', if_not_exists => TRUE);

-- ------------------------------------------------------------------ ports
CREATE TABLE IF NOT EXISTS ports (
    id            SERIAL PRIMARY KEY,
    unlocode      TEXT UNIQUE,
    name          TEXT NOT NULL,
    country       TEXT NOT NULL,
    geom          GEOMETRY(Point, 4326) NOT NULL,
    anchorage_radius_m INTEGER NOT NULL DEFAULT 15000,
    berth_radius_m     INTEGER NOT NULL DEFAULT 3000
);

CREATE INDEX IF NOT EXISTS ports_geom_gist ON ports USING GIST (geom);

-- ------------------------------------------------------------- port_calls
-- One row per vessel visit. Built later by the port-call detector; the ETA
-- labels come from arrival_at, so get the columns right now.
CREATE TABLE IF NOT EXISTS port_calls (
    id            BIGSERIAL PRIMARY KEY,
    mmsi          BIGINT      NOT NULL REFERENCES vessels(mmsi) ON DELETE CASCADE,
    port_id       INTEGER     NOT NULL REFERENCES ports(id),
    approach_at   TIMESTAMPTZ NOT NULL,   -- first fix inside anchorage radius
    arrival_at    TIMESTAMPTZ,            -- first fix inside berth radius, sog < 0.5
    departure_at  TIMESTAMPTZ,
    UNIQUE (mmsi, port_id, approach_at)
);

CREATE INDEX IF NOT EXISTS port_calls_port_time ON port_calls (port_id, approach_at DESC);

-- --------------------------------------------------------- model_registry
CREATE TABLE IF NOT EXISTS model_registry (
    id            SERIAL PRIMARY KEY,
    name          TEXT NOT NULL,
    version       TEXT NOT NULL,
    trained_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    train_rows    INTEGER,
    holdout_start TIMESTAMPTZ,
    baseline_mae_hours REAL,
    model_mae_hours    REAL,
    params        JSONB,
    artifact_path TEXT,
    is_active     BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (name, version)
);

-- ------------------------------------------------------------- port seeds
-- Coordinates are approximate (~0.05 deg) and unverified against UN/LOCODE.
-- berth_radius_m is 3 km, so check each before trusting port_calls.
INSERT INTO ports (unlocode, name, country, geom) VALUES
    ('NLRTM', 'Rotterdam',        'Netherlands',    ST_SetSRID(ST_MakePoint( 4.0500, 51.9500), 4326)),
    ('BEANR', 'Antwerp',          'Belgium',        ST_SetSRID(ST_MakePoint( 4.4000, 51.2300), 4326)),
    ('DEHAM', 'Hamburg',          'Germany',        ST_SetSRID(ST_MakePoint( 9.9700, 53.5400), 4326)),
    ('DEBRV', 'Bremerhaven',      'Germany',        ST_SetSRID(ST_MakePoint( 8.5700, 53.5500), 4326)),
    ('BEZEE', 'Zeebrugge',        'Belgium',        ST_SetSRID(ST_MakePoint( 3.2000, 51.3300), 4326)),
    ('NLAMS', 'Amsterdam',        'Netherlands',    ST_SetSRID(ST_MakePoint( 4.8000, 52.4000), 4326)),
    ('GBFXT', 'Felixstowe',       'United Kingdom', ST_SetSRID(ST_MakePoint( 1.3300, 51.9500), 4326)),
    ('DEWVN', 'Wilhelmshaven',    'Germany',        ST_SetSRID(ST_MakePoint( 8.1300, 53.5200), 4326)),
    ('FRDKK', 'Dunkirk',          'France',         ST_SetSRID(ST_MakePoint( 2.3500, 51.0500), 4326)),
    ('FRLEH', 'Le Havre',         'France',         ST_SetSRID(ST_MakePoint( 0.1500, 49.4800), 4326)),
    ('GBSOU', 'Southampton',      'United Kingdom', ST_SetSRID(ST_MakePoint(-1.4000, 50.9000), 4326)),
    ('NLVLI', 'Vlissingen',       'Netherlands',    ST_SetSRID(ST_MakePoint( 3.6000, 51.4500), 4326))
ON CONFLICT (unlocode) DO NOTHING;
