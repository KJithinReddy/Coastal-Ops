"""
Coastal Ops broker — adapter for agent tools (read + write).

All Lakebase + Open-Meteo/NWS logic lives here so FastMCP tools stay thin
(same pattern as weather_broker.py / alpaca_broker.py).
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import lakebase
from marine_client import DEFAULT_PORTS, MarineClient

logger = logging.getLogger("coastal-broker")

DOCS_TABLE = os.environ.get("MARINE_DOCS_TABLE", "marine_documents")
EMB_TABLE = os.environ.get("MARINE_EMB_TABLE", "marine_embeddings")
EMBEDDING_MODEL = os.environ.get(
    "MARINE_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"
)
MIN_SIMILARITY = max(0.0, min(1.0, float(os.environ.get("MIN_MARINE_SIMILARITY", "0.40"))))

_embedder = None


def _get_embedder():
    global _embedder
    if _embedder is None:
        from sentence_transformers import SentenceTransformer

        logger.info("Loading embedding model %s", EMBEDDING_MODEL)
        _embedder = SentenceTransformer(EMBEDDING_MODEL)
    return _embedder


def _vector_literal(vec: list[float]) -> str:
    return "[" + ",".join(f"{x:.8f}" for x in vec) + "]"


def _ok(payload: dict[str, Any]) -> dict[str, Any]:
    return {"status": "ok", **payload}


def _err(message: str, **extra: Any) -> dict[str, Any]:
    return {"status": "error", "message": message, **extra}


# ---------------------------------------------------------------------------
# Tool implementations (read + write)
# ---------------------------------------------------------------------------


def seed_default_ports() -> dict[str, Any]:
    """Upsert the built-in coastal ports used by demos."""
    lakebase.ensure_ops_tables()
    count = 0
    for port in DEFAULT_PORTS:
        lakebase.run_write(
            """
            INSERT INTO ports (port_id, name, lat, lon, state, region, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (port_id) DO UPDATE SET
                name = EXCLUDED.name,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                state = EXCLUDED.state,
                region = EXCLUDED.region
            """,
            (
                port["port_id"],
                port["name"],
                port["lat"],
                port["lon"],
                port.get("state"),
                port.get("region"),
            ),
        )
        count += 1
    return _ok({"seeded": count, "ports": [p["name"] for p in DEFAULT_PORTS]})


def list_ports() -> dict[str, Any]:
    lakebase.ensure_ops_tables()
    rows = lakebase.run_query(
        "SELECT port_id, name, lat, lon, state, region FROM ports ORDER BY name"
    )
    if not rows:
        seed_default_ports()
        rows = lakebase.run_query(
            "SELECT port_id, name, lat, lon, state, region FROM ports ORDER BY name"
        )
    return _ok({"ports": rows})


def _find_port(query: str) -> dict | None:
    q = (query or "").strip().lower()
    if not q:
        return None
    ports = list_ports()["ports"]
    for p in ports:
        if p["port_id"] == q or p["name"].lower() == q:
            return p
    for p in ports:
        if q in p["name"].lower() or q in (p.get("state") or "").lower():
            return p
    return None


def get_port_conditions(port_name: str) -> dict[str, Any]:
    """Live Open-Meteo marine + wind conditions for a port; also stores a snapshot."""
    port = _find_port(port_name)
    if not port:
        return _err(f"Unknown port {port_name!r}. Try list_ports first.")

    client = MarineClient()
    conditions = client.current_conditions(port["lat"], port["lon"])
    snapshot_id = str(uuid.uuid4())
    lakebase.run_write(
        """
        INSERT INTO marine_snapshots (
            snapshot_id, port_id, wave_height_m, wind_speed_ms, wind_direction_deg,
            swell_wave_height_m, swell_wave_period_s, risk_level, summary_text,
            observed_at, payload, synced_at
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s::timestamptz, %s::jsonb, now()
        )
        """,
        (
            snapshot_id,
            port["port_id"],
            conditions.get("wave_height_m"),
            conditions.get("wind_speed_ms"),
            conditions.get("wind_direction_deg"),
            conditions.get("swell_wave_height_m"),
            conditions.get("swell_wave_period_s"),
            conditions.get("risk_level") or "unknown",
            conditions.get("summary_text") or "",
            conditions.get("observed_at"),
            json.dumps(conditions.get("payload") or {}),
        ),
    )
    return _ok(
        {
            "port": port,
            "snapshot_id": snapshot_id,
            "conditions": {k: v for k, v in conditions.items() if k != "payload"},
        }
    )


def search_marine_context(query: str, top_k: int = 5) -> dict[str, Any]:
    """Semantic search over marine_embeddings (RAG read tool)."""
    lakebase.ensure_marine_embeddings_table(EMB_TABLE)
    count_rows = lakebase.run_query(f"SELECT COUNT(*) AS n FROM {EMB_TABLE}")
    if not count_rows or int(count_rows[0]["n"]) == 0:
        return _ok(
            {
                "query": query,
                "results": [],
                "message": (
                    "No marine embeddings yet. Run POST /marine/sync then "
                    "notebooks/ingest_marine_embeddings.py"
                ),
            }
        )

    top_k = max(1, min(int(top_k), 10))
    model = _get_embedder()
    vector = model.encode([query], show_progress_bar=False)[0].tolist()
    vector_literal = _vector_literal(vector)

    rows = lakebase.run_query(
        f"""
        WITH best_chunks AS (
            SELECT DISTINCT ON (e.document_id)
                d.id,
                d.location,
                d.source_type,
                d.source_url,
                d.headline,
                d.event,
                d.narrative_text,
                e.chunk_text,
                e.chunk_index,
                1 - (e.embedding <=> %s::vector) AS similarity
            FROM {EMB_TABLE} e
            JOIN {DOCS_TABLE} d ON d.id = e.document_id
            WHERE 1 - (e.embedding <=> %s::vector) >= %s
            ORDER BY e.document_id, e.embedding <=> %s::vector
        )
        SELECT * FROM best_chunks
        ORDER BY similarity DESC
        LIMIT %s
        """,
        (vector_literal, vector_literal, MIN_SIMILARITY, vector_literal, top_k),
    )
    results = []
    for row in rows:
        item = dict(row)
        if item.get("similarity") is not None:
            item["similarity"] = float(item["similarity"])
        results.append(item)
    return _ok({"query": query, "top_k": top_k, "results": results})


def list_voyages(status: str | None = None) -> dict[str, Any]:
    lakebase.ensure_ops_tables()
    if status:
        rows = lakebase.run_query(
            """
            SELECT v.*, o.name AS origin_name, d.name AS dest_name
            FROM voyages v
            JOIN ports o ON o.port_id = v.origin_port_id
            JOIN ports d ON d.port_id = v.dest_port_id
            WHERE v.status = %s
            ORDER BY v.planned_depart_at
            """,
            (status,),
        )
    else:
        rows = lakebase.run_query(
            """
            SELECT v.*, o.name AS origin_name, d.name AS dest_name
            FROM voyages v
            JOIN ports o ON o.port_id = v.origin_port_id
            JOIN ports d ON d.port_id = v.dest_port_id
            ORDER BY v.planned_depart_at
            """
        )
    return _ok({"voyages": rows})


def create_voyage(
    name: str,
    origin_port: str,
    dest_port: str,
    planned_depart_at: str | None = None,
    notes: str | None = None,
    created_by: str = "agent",
) -> dict[str, Any]:
    """Write tool: schedule a new coastal voyage."""
    origin = _find_port(origin_port)
    dest = _find_port(dest_port)
    if not origin or not dest:
        return _err(
            "Both origin_port and dest_port must match known ports.",
            hint=list_ports(),
        )

    depart = planned_depart_at
    if not depart:
        depart = (datetime.now(timezone.utc) + timedelta(hours=12)).isoformat()
    arrive = (
        datetime.fromisoformat(depart.replace("Z", "+00:00")) + timedelta(hours=18)
    ).isoformat()

    voyage_id = str(uuid.uuid4())
    row = lakebase.run_write_returning(
        """
        INSERT INTO voyages (
            voyage_id, name, origin_port_id, dest_port_id,
            planned_depart_at, planned_arrive_at, status, notes, created_by
        )
        VALUES (%s, %s, %s, %s, %s::timestamptz, %s::timestamptz, 'planned', %s, %s)
        RETURNING *
        """,
        (
            voyage_id,
            name.strip(),
            origin["port_id"],
            dest["port_id"],
            depart,
            arrive,
            notes,
            created_by,
        ),
    )
    return _ok({"voyage": row, "origin": origin["name"], "dest": dest["name"]})


def reschedule_voyage(
    voyage_id: str,
    new_depart_at: str | None = None,
    reason: str | None = None,
    defer_hours: int = 24,
    created_by: str = "agent",
) -> dict[str, Any]:
    """Write tool: move a voyage and log an alert explaining why."""
    rows = lakebase.run_query(
        "SELECT * FROM voyages WHERE voyage_id = %s", (voyage_id,)
    )
    if not rows:
        return _err(f"Voyage {voyage_id} not found")
    voyage = rows[0]

    if new_depart_at:
        depart = new_depart_at
    else:
        current = voyage["planned_depart_at"]
        if isinstance(current, datetime):
            base = current if current.tzinfo else current.replace(tzinfo=timezone.utc)
        else:
            base = datetime.fromisoformat(str(current).replace("Z", "+00:00"))
        depart = (base + timedelta(hours=int(defer_hours))).isoformat()

    arrive = (
        datetime.fromisoformat(depart.replace("Z", "+00:00")) + timedelta(hours=18)
    ).isoformat()
    note_suffix = reason or "Deferred by Coastal Ops agent due to marine risk."
    updated = lakebase.run_write_returning(
        """
        UPDATE voyages
        SET planned_depart_at = %s::timestamptz,
            planned_arrive_at = %s::timestamptz,
            status = 'deferred',
            notes = COALESCE(notes, '') || %s,
            updated_at = now()
        WHERE voyage_id = %s
        RETURNING *
        """,
        (depart, arrive, f"\n[reschedule] {note_suffix}", voyage_id),
    )

    alert = create_alert(
        title=f"Voyage deferred: {voyage['name']}",
        message=note_suffix,
        severity="warning",
        voyage_id=voyage_id,
        port_id=voyage["origin_port_id"],
        created_by=created_by,
    )
    return _ok(
        {
            "voyage": updated,
            "alert": alert.get("alert"),
            "new_depart_at": depart,
        }
    )


def create_alert(
    title: str,
    message: str,
    severity: str = "watch",
    voyage_id: str | None = None,
    port_id: str | None = None,
    port_name: str | None = None,
    created_by: str = "agent",
) -> dict[str, Any]:
    """Write tool: persist an operational alert."""
    if severity not in {"info", "watch", "warning", "critical"}:
        severity = "watch"
    if port_name and not port_id:
        port = _find_port(port_name)
        port_id = port["port_id"] if port else None

    alert_id = str(uuid.uuid4())
    row = lakebase.run_write_returning(
        """
        INSERT INTO alerts (
            alert_id, voyage_id, port_id, severity, title, message, status, created_by
        )
        VALUES (%s, %s, %s, %s, %s, %s, 'open', %s)
        RETURNING *
        """,
        (alert_id, voyage_id, port_id, severity, title.strip(), message.strip(), created_by),
    )
    return _ok({"alert": row})


def save_ops_note(
    note_text: str,
    voyage_id: str | None = None,
    port_name: str | None = None,
    created_by: str = "agent",
) -> dict[str, Any]:
    """Write tool: save an ops note tied to a voyage or port."""
    port_id = None
    if port_name:
        port = _find_port(port_name)
        port_id = port["port_id"] if port else None
    note_id = str(uuid.uuid4())
    row = lakebase.run_write_returning(
        """
        INSERT INTO ops_notes (note_id, voyage_id, port_id, note_text, created_by)
        VALUES (%s, %s, %s, %s, %s)
        RETURNING *
        """,
        (note_id, voyage_id, port_id, note_text.strip(), created_by),
    )
    return _ok({"note": row})


def list_alerts(status: str = "open") -> dict[str, Any]:
    lakebase.ensure_ops_tables()
    rows = lakebase.run_query(
        """
        SELECT * FROM alerts
        WHERE status = %s
        ORDER BY created_at DESC
        LIMIT 50
        """,
        (status,),
    )
    return _ok({"alerts": rows})


def assess_and_act(
    port_name: str,
    voyage_id: str | None = None,
    created_by: str = "agent",
) -> dict[str, Any]:
    """
    Composite demo tool: check conditions; if risk is high/severe,
    create an alert and optionally defer a voyage.
    """
    conditions = get_port_conditions(port_name)
    if conditions.get("status") == "error" or conditions.get("error"):
        return conditions

    risk = (conditions.get("conditions") or {}).get("risk_level", "unknown")
    actions: list[dict] = []
    summary = conditions["conditions"]["summary_text"]

    if risk in {"high", "severe"}:
        alert = create_alert(
            title=f"{risk.upper()} marine risk at {port_name}",
            message=summary,
            severity="critical" if risk == "severe" else "warning",
            voyage_id=voyage_id,
            port_name=port_name,
            created_by=created_by,
        )
        actions.append({"type": "create_alert", "result": alert})
        if voyage_id:
            defer = reschedule_voyage(
                voyage_id=voyage_id,
                reason=f"Deferred: {risk} marine risk at {port_name}. {summary}",
                defer_hours=24 if risk == "high" else 48,
                created_by=created_by,
            )
            actions.append({"type": "reschedule_voyage", "result": defer})
    else:
        note = save_ops_note(
            note_text=f"Conditions OK ({risk}) at {port_name}. {summary}",
            voyage_id=voyage_id,
            port_name=port_name,
            created_by=created_by,
        )
        actions.append({"type": "save_ops_note", "result": note})

    return _ok({"risk_level": risk, "conditions": conditions, "actions": actions})


TOOL_SPECS: list[dict[str, Any]] = [
    {
        "name": "list_ports",
        "description": "List known coastal ports in Lakebase.",
        "parameters": {},
    },
    {
        "name": "get_port_conditions",
        "description": "Fetch live marine + wind conditions for a port and store a snapshot.",
        "parameters": {"port_name": "string"},
    },
    {
        "name": "search_marine_context",
        "description": "Semantic search over marine forecast/alert embeddings.",
        "parameters": {"query": "string", "top_k": "integer optional"},
    },
    {
        "name": "list_voyages",
        "description": "List scheduled voyages, optionally filtered by status.",
        "parameters": {"status": "string optional"},
    },
    {
        "name": "create_voyage",
        "description": "Create a new voyage between two ports.",
        "parameters": {
            "name": "string",
            "origin_port": "string",
            "dest_port": "string",
            "planned_depart_at": "ISO datetime optional",
            "notes": "string optional",
        },
    },
    {
        "name": "reschedule_voyage",
        "description": "Defer/reschedule a voyage and create a warning alert.",
        "parameters": {
            "voyage_id": "string",
            "new_depart_at": "ISO datetime optional",
            "reason": "string optional",
            "defer_hours": "integer optional",
        },
    },
    {
        "name": "create_alert",
        "description": "Create an operational alert.",
        "parameters": {
            "title": "string",
            "message": "string",
            "severity": "info|watch|warning|critical",
            "voyage_id": "string optional",
            "port_name": "string optional",
        },
    },
    {
        "name": "save_ops_note",
        "description": "Save an ops note for a voyage or port.",
        "parameters": {
            "note_text": "string",
            "voyage_id": "string optional",
            "port_name": "string optional",
        },
    },
    {
        "name": "list_alerts",
        "description": "List open (or other status) alerts.",
        "parameters": {"status": "string optional"},
    },
    {
        "name": "assess_and_act",
        "description": (
            "Check marine risk at a port; if high/severe, create an alert and "
            "optionally defer a voyage. Best demo tool for go/no-go decisions."
        ),
        "parameters": {"port_name": "string", "voyage_id": "string optional"},
    },
]

TOOL_IMPLS: dict[str, Callable[..., dict[str, Any]]] = {
    "list_ports": lambda **_: list_ports(),
    "get_port_conditions": get_port_conditions,
    "search_marine_context": search_marine_context,
    "list_voyages": list_voyages,
    "create_voyage": create_voyage,
    "reschedule_voyage": reschedule_voyage,
    "create_alert": create_alert,
    "save_ops_note": save_ops_note,
    "list_alerts": list_alerts,
    "assess_and_act": assess_and_act,
}


def run_tool(
    name: str,
    arguments: dict[str, Any] | None = None,
    created_by: str = "agent",
) -> dict[str, Any]:
    """Execute a named tool with JSON-able arguments."""
    if name not in TOOL_IMPLS:
        return {"error": f"Unknown tool {name!r}", "available": list(TOOL_IMPLS)}
    args = dict(arguments or {})
    fn = TOOL_IMPLS[name]
    try:
        if name in {
            "create_voyage",
            "reschedule_voyage",
            "create_alert",
            "save_ops_note",
            "assess_and_act",
        }:
            args.setdefault("created_by", created_by)
        result = fn(**args)
        return {"tool": name, "arguments": args, "result": result}
    except TypeError as exc:
        return {"error": f"Bad arguments for {name}: {exc}", "arguments": args}
    except Exception as exc:
        logger.exception("Tool %s failed", name)
        return {"error": str(exc), "tool": name, "arguments": args}
