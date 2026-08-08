-- marine_documents: Open-Meteo marine narratives + NWS coastal alerts

CREATE TABLE IF NOT EXISTS marine_documents (
    id TEXT PRIMARY KEY,
    location TEXT NOT NULL,
    source_type TEXT NOT NULL
        CHECK (source_type IN ('marine_forecast', 'alert', 'ops_bulletin')),
    source_url TEXT,
    headline TEXT,
    event TEXT,
    narrative_text TEXT NOT NULL,
    issued_at TIMESTAMPTZ,
    effective_at TIMESTAMPTZ,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_marine_documents_location
ON marine_documents (location);

CREATE INDEX IF NOT EXISTS idx_marine_documents_source_type
ON marine_documents (source_type);

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'marine_documents'
ORDER BY ordinal_position;
