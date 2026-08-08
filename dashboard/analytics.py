"""
App analytics — log Coastal Ops usage events for Delta analytics.

The Flask app writes events to Lakebase `app_events`.
`notebooks/analytics_app_events_to_delta.py` loads them with Spark into Delta
tables (raw + daily aggregates) — covers "analytics inside Delta about your app".
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import lakebase

logger = logging.getLogger("coastal-analytics")


def ensure_app_events_table() -> None:
    """Create the Lakebase staging table for app usage events."""
    lakebase.run_write(
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
    lakebase.run_write(
        "CREATE INDEX IF NOT EXISTS idx_app_events_type ON app_events (event_type)"
    )
    lakebase.run_write(
        "CREATE INDEX IF NOT EXISTS idx_app_events_created_at ON app_events (created_at)"
    )


def log_event(
    event_type: str,
    *,
    user_email: str | None = None,
    path: str | None = None,
    payload: dict[str, Any] | None = None,
) -> str | None:
    """
    Insert one app usage event. Never raises — analytics must not break the app.
    Returns event_id on success, None on failure.
    """
    event_id = str(uuid.uuid4())
    try:
        ensure_app_events_table()
        lakebase.run_write(
            """
            INSERT INTO app_events (event_id, event_type, user_email, path, payload, created_at)
            VALUES (%s, %s, %s, %s, %s::jsonb, now())
            """,
            (
                event_id,
                event_type,
                user_email,
                path,
                json.dumps(payload or {}, default=str),
            ),
        )
        return event_id
    except Exception:
        logger.exception("Failed to log app event %s", event_type)
        return None
