-- App usage events staged in Lakebase; Spark notebook loads these into Delta.

CREATE TABLE IF NOT EXISTS app_events (
    event_id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    user_email TEXT,
    path TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_app_events_type ON app_events (event_type);
CREATE INDEX IF NOT EXISTS idx_app_events_created_at ON app_events (created_at);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'app_events'
ORDER BY ordinal_position;
