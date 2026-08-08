-- Coastal Ops relational tables (ports, voyages, snapshots, alerts, notes)
-- Optional: app/agent also auto-create via lakebase.ensure_ops_tables()

CREATE TABLE IF NOT EXISTS ports (
    port_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    lat DOUBLE PRECISION NOT NULL,
    lon DOUBLE PRECISION NOT NULL,
    state TEXT,
    region TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS voyages (
    voyage_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    origin_port_id TEXT NOT NULL REFERENCES ports (port_id),
    dest_port_id TEXT NOT NULL REFERENCES ports (port_id),
    planned_depart_at TIMESTAMPTZ NOT NULL,
    planned_arrive_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'planned'
        CHECK (status IN ('planned', 'deferred', 'underway', 'completed', 'cancelled')),
    notes TEXT,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS marine_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    port_id TEXT NOT NULL REFERENCES ports (port_id),
    wave_height_m DOUBLE PRECISION,
    wind_speed_ms DOUBLE PRECISION,
    wind_direction_deg DOUBLE PRECISION,
    swell_wave_height_m DOUBLE PRECISION,
    swell_wave_period_s DOUBLE PRECISION,
    risk_level TEXT NOT NULL DEFAULT 'unknown'
        CHECK (risk_level IN ('low', 'moderate', 'high', 'severe', 'unknown')),
    summary_text TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS alerts (
    alert_id TEXT PRIMARY KEY,
    voyage_id TEXT REFERENCES voyages (voyage_id) ON DELETE SET NULL,
    port_id TEXT REFERENCES ports (port_id) ON DELETE SET NULL,
    severity TEXT NOT NULL
        CHECK (severity IN ('info', 'watch', 'warning', 'critical')),
    title TEXT NOT NULL,
    message TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'acknowledged', 'resolved')),
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ops_notes (
    note_id TEXT PRIMARY KEY,
    voyage_id TEXT REFERENCES voyages (voyage_id) ON DELETE SET NULL,
    port_id TEXT REFERENCES ports (port_id) ON DELETE SET NULL,
    note_text TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_voyages_status ON voyages (status);
CREATE INDEX IF NOT EXISTS idx_snapshots_port ON marine_snapshots (port_id);
CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status);
