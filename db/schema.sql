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
INSERT INTO ports (unlocode, name, country, geom) VALUES
    ('OMSOH', 'Sohar',            'Oman', ST_SetSRID(ST_MakePoint(56.6100, 24.5100), 4326)),
    ('OMMCT', 'Muscat (Sultan Qaboos)', 'Oman', ST_SetSRID(ST_MakePoint(58.5650, 23.6250), 4326)),
    ('OMSLL', 'Salalah',          'Oman', ST_SetSRID(ST_MakePoint(54.0050, 16.9350), 4326)),
    ('OMDQM', 'Duqm',             'Oman', ST_SetSRID(ST_MakePoint(57.6900, 19.6700), 4326)),
    ('AEJEA', 'Jebel Ali',        'UAE',  ST_SetSRID(ST_MakePoint(55.0270, 24.9850), 4326)),
    ('AEAUH', 'Khalifa Port',     'UAE',  ST_SetSRID(ST_MakePoint(54.6500, 24.8100), 4326)),
    ('AEFJR', 'Fujairah',         'UAE',  ST_SetSRID(ST_MakePoint(56.3600, 25.1600), 4326)),
    ('AESHJ', 'Sharjah',          'UAE',  ST_SetSRID(ST_MakePoint(55.3700, 25.3600), 4326)),
    ('QAHMD', 'Hamad Port',       'Qatar',ST_SetSRID(ST_MakePoint(51.5900, 25.0100), 4326)),
    ('BHKBS', 'Khalifa Bin Salman','Bahrain', ST_SetSRID(ST_MakePoint(50.6500, 26.2000), 4326)),
    ('KWSAA', 'Shuaiba',          'Kuwait', ST_SetSRID(ST_MakePoint(48.1500, 29.0400), 4326)),
    ('KWSWK', 'Shuwaikh',         'Kuwait', ST_SetSRID(ST_MakePoint(47.9300, 29.3500), 4326)),
    ('SADMM', 'Dammam',           'Saudi Arabia', ST_SetSRID(ST_MakePoint(50.1800, 26.5000), 4326)),
    ('SAJUB', 'Jubail',           'Saudi Arabia', ST_SetSRID(ST_MakePoint(49.6600, 27.0100), 4326)),
    ('SARTA', 'Ras Tanura',       'Saudi Arabia', ST_SetSRID(ST_MakePoint(50.1600, 26.6400), 4326)),
    ('IRBND', 'Bandar Abbas',     'Iran', ST_SetSRID(ST_MakePoint(56.2100, 27.1300), 4326)),
    ('IRBKM', 'Bandar Khomeini',  'Iran', ST_SetSRID(ST_MakePoint(49.0800, 30.4200), 4326)),
    ('IQUQR', 'Umm Qasr',         'Iraq', ST_SetSRID(ST_MakePoint(47.9400, 30.0400), 4326)),
    ('PKKAR', 'Karachi',          'Pakistan', ST_SetSRID(ST_MakePoint(66.9800, 24.8100), 4326)),
    ('INMUN', 'Mundra',           'India', ST_SetSRID(ST_MakePoint(69.7200, 22.7400), 4326))
ON CONFLICT (unlocode) DO NOTHING;
