"""
Lakebase (Databricks-managed Postgres) connection helper + Coastal Ops DDL.

Resolution order for the connection URL:
1. LAKEBASE_URL environment variable (local .env / explicit override)
2. Databricks secret scope (defaults: database / lakebase-url)
"""

from __future__ import annotations

import base64
import os
from contextlib import contextmanager
from typing import Any, Generator

import psycopg2
from psycopg2.extras import RealDictCursor

_SCOPE = os.environ.get("LAKEBASE_SECRET_SCOPE", "database")
_KEY = os.environ.get("LAKEBASE_SECRET_KEY", "lakebase-url")

_w = None


def _workspace_client():
    """Lazy-init so `import lakebase` does not hang Serverless notebooks."""
    global _w
    if _w is None:
        from databricks.sdk import WorkspaceClient

        _w = WorkspaceClient()
    return _w


def _lakebase_url() -> str:
    """Resolve the Lakebase connection URL from env or Databricks secrets."""
    url = os.environ.get("LAKEBASE_URL")
    if url:
        return url

    secret = _workspace_client().secrets.get_secret(scope=_SCOPE, key=_KEY)
    return base64.b64decode(secret.value).decode("utf-8")


@contextmanager
def get_connection() -> Generator[Any, None, None]:
    """Yield a raw psycopg2 connection with a RealDictCursor factory."""
    conn = psycopg2.connect(_lakebase_url(), cursor_factory=RealDictCursor)
    try:
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params: tuple | dict | None = None) -> list[dict]:
    """Run a read query against Lakebase and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return [dict(row) for row in cur.fetchall()]


def run_write(sql: str, params: tuple | dict | None = None) -> int:
    """Run an INSERT/UPDATE/DELETE against Lakebase, return affected row count."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            conn.commit()
            return cur.rowcount


def run_write_returning(sql: str, params: tuple | dict | None = None) -> dict | None:
    """Run a write that returns a single row (e.g. INSERT ... RETURNING *)."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row else None


# ---------------------------------------------------------------------------
# DDL helpers (app + notebooks call these so SQL files stay optional)
# ---------------------------------------------------------------------------


def ensure_ops_tables() -> None:
    """Create ports, voyages, marine_snapshots, alerts, ops_notes."""
    run_write(
        """
        CREATE TABLE IF NOT EXISTS ports (
            port_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lon DOUBLE PRECISION NOT NULL,
            state TEXT,
            region TEXT,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    run_write(
        """
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
        )
        """
    )
    run_write(
        """
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
        )
        """
    )
    run_write(
        """
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
        )
        """
    )
    run_write(
        """
        CREATE TABLE IF NOT EXISTS ops_notes (
            note_id TEXT PRIMARY KEY,
            voyage_id TEXT REFERENCES voyages (voyage_id) ON DELETE SET NULL,
            port_id TEXT REFERENCES ports (port_id) ON DELETE SET NULL,
            note_text TEXT NOT NULL,
            created_by TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    run_write("CREATE INDEX IF NOT EXISTS idx_voyages_status ON voyages (status)")
    run_write("CREATE INDEX IF NOT EXISTS idx_snapshots_port ON marine_snapshots (port_id)")
    run_write("CREATE INDEX IF NOT EXISTS idx_alerts_status ON alerts (status)")


def ensure_marine_documents_table(table_name: str = "marine_documents") -> None:
    """Create marine_documents (+ indexes) if missing."""
    run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
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
            payload JSONB NOT NULL DEFAULT '{{}}'::jsonb,
            synced_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_location ON {table_name} (location)"
    )
    run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_source_type "
        f"ON {table_name} (source_type)"
    )


def ensure_marine_embeddings_table(table_name: str = "marine_embeddings") -> None:
    """Create marine_embeddings with pgvector(384) + HNSW cosine index."""
    run_write("CREATE EXTENSION IF NOT EXISTS vector")
    run_write(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id TEXT PRIMARY KEY,
            document_id TEXT NOT NULL REFERENCES marine_documents (id) ON DELETE CASCADE,
            chunk_index INT NOT NULL,
            chunk_text TEXT NOT NULL,
            embedding VECTOR(384) NOT NULL,
            model_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    run_write(
        f"CREATE INDEX IF NOT EXISTS idx_{table_name}_document_id "
        f"ON {table_name} (document_id)"
    )
    run_write(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{table_name}_embedding
        ON {table_name}
        USING hnsw (embedding vector_cosine_ops)
        """
    )


def ensure_app_events_table() -> None:
    """Create app_events staging table (usage analytics → Spark/Delta)."""
    run_write(
        """
        CREATE TABLE IF NOT EXISTS app_events (
            event_id TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            user_email TEXT,
            path TEXT,
            payload JSONB NOT NULL DEFAULT '{}'::jsonb,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )
        """
    )
    run_write(
        "CREATE INDEX IF NOT EXISTS idx_app_events_type ON app_events (event_type)"
    )
    run_write(
        "CREATE INDEX IF NOT EXISTS idx_app_events_created_at ON app_events (created_at)"
    )


def ensure_all_tables() -> None:
    """Create every Coastal Ops table used by the app."""
    ensure_ops_tables()
    ensure_marine_documents_table()
    ensure_marine_embeddings_table()
    ensure_app_events_table()
