"""
Coastal Ops dashboard — Flask app for sync, search, and ops state.

Same pattern as HW3's dashboard: this app never talks to the MCP server
directly. It imports coastal_broker / marine_client / lakebase (duplicated
because each Databricks App deploys from its own folder).

The agent lives in Playground (MCP → export app), not in this repo.

Endpoints:
  GET  /healthz
  GET  /
  POST /ports/seed
  GET  /ports
  POST /marine/sync
  POST /marine/search
  GET  /voyages
  POST /voyages
  GET  /alerts

Run locally:
    python app.py
Deploy as its own Databricks App (dashboard/), separate from mcp_server/.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, request

import analytics
import coastal_broker
import lakebase
from marine_client import DEFAULT_PORTS, MarineClient

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("coastal-ops")

app = Flask(__name__)

DOCS_TABLE = os.environ.get("MARINE_DOCS_TABLE", "marine_documents")
DEFAULT_LOCATIONS = [
    loc.strip()
    for loc in os.environ.get(
        "MARINE_LOCATIONS",
        "Miami, FL;Boston, MA;Seattle, WA;Galveston, TX;Norfolk, VA",
    ).split(";")
    if loc.strip()
]


def _serialize_row(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


def _serialize_rows(rows: list[dict]) -> list[dict]:
    return [_serialize_row(r) for r in rows]  # type: ignore[misc]


def _current_user_email() -> str:
    header_email = request.headers.get("X-Forwarded-Email")
    if header_email:
        return header_email.strip()
    try:
        from databricks.sdk import WorkspaceClient

        return WorkspaceClient().current_user.me().user_name
    except Exception:
        return "local_dev@example.com"


def _upsert_marine_docs(docs: list[dict[str, Any]]) -> int:
    count = 0
    with lakebase.get_connection() as conn:
        with conn.cursor() as cur:
            for doc in docs:
                cur.execute(
                    f"""
                    INSERT INTO {DOCS_TABLE} (
                        id, location, source_type, source_url, headline, event,
                        narrative_text, issued_at, effective_at, payload, synced_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                    ON CONFLICT (id) DO UPDATE SET
                        location = EXCLUDED.location,
                        source_type = EXCLUDED.source_type,
                        source_url = EXCLUDED.source_url,
                        headline = EXCLUDED.headline,
                        event = EXCLUDED.event,
                        narrative_text = EXCLUDED.narrative_text,
                        issued_at = EXCLUDED.issued_at,
                        effective_at = EXCLUDED.effective_at,
                        payload = EXCLUDED.payload,
                        synced_at = EXCLUDED.synced_at
                    """,
                    (
                        doc["id"],
                        doc["location"],
                        doc["source_type"],
                        doc.get("source_url"),
                        doc.get("headline"),
                        doc.get("event"),
                        doc["narrative_text"],
                        doc.get("issued_at"),
                        doc.get("effective_at"),
                        json.dumps(doc.get("payload") or {}),
                    ),
                )
                count += 1
            conn.commit()
    return count


@app.errorhandler(Exception)
def handle_exception(err: Exception):
    logger.exception("Unhandled exception while processing request")
    status_code = getattr(err, "code", 500)
    if not isinstance(status_code, int):
        status_code = 500
    return jsonify({"error": str(err)}), status_code


@app.route("/healthz")
def healthz():
    return jsonify({"status": "ok", "service": "coastal-ops-dashboard"})


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ports/seed", methods=["POST"])
def seed_ports():
    result = coastal_broker.seed_default_ports()
    analytics.log_event(
        "ports_seed",
        user_email=_current_user_email(),
        path="/ports/seed",
        payload={"seeded": result.get("seeded")},
    )
    return jsonify(result)


@app.route("/ports", methods=["GET"])
def get_ports():
    result = coastal_broker.list_ports()
    analytics.log_event(
        "ports_list",
        user_email=_current_user_email(),
        path="/ports",
        payload={"count": len(result.get("ports") or [])},
    )
    return jsonify({"ports": _serialize_rows(result["ports"])})


@app.route("/marine/sync", methods=["POST"])
def sync_marine():
    """
    Harvest Open-Meteo marine narratives + NWS coastal alerts into marine_documents.

    Body (optional JSON):
      {"locations": ["Miami, FL", "Boston, MA"], "limit": 40}
    """
    lakebase.ensure_all_tables()
    coastal_broker.seed_default_ports()

    body = request.json if request.is_json else {}
    locations = body.get("locations") or DEFAULT_LOCATIONS
    locations = [loc.strip() for loc in locations if isinstance(loc, str) and loc.strip()]
    if not locations:
        return jsonify({"error": "Provide at least one location in 'locations'"}), 400

    limit = int(body.get("limit", 40))
    limit = max(1, min(limit, 200))

    ports_by_name = {p["name"].lower(): p for p in coastal_broker.list_ports()["ports"]}

    client = MarineClient()
    total = 0
    snapshots = 0
    errors: list[dict[str, str]] = []

    for location in locations:
        try:
            port = ports_by_name.get(location.lower())
            if port:
                docs = client.harvest_location(
                    port["name"],
                    lat=port["lat"],
                    lon=port["lon"],
                    state=port.get("state"),
                    limit=limit,
                )
                conditions = client.current_conditions(port["lat"], port["lon"])
                lakebase.run_write(
                    """
                    INSERT INTO marine_snapshots (
                        snapshot_id, port_id, wave_height_m, wind_speed_ms,
                        wind_direction_deg, swell_wave_height_m, swell_wave_period_s,
                        risk_level, summary_text, observed_at, payload, synced_at
                    )
                    VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s::timestamptz, %s::jsonb, now()
                    )
                    """,
                    (
                        str(uuid.uuid4()),
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
                snapshots += 1
            else:
                docs = client.harvest_location(location, limit=limit)
            total += _upsert_marine_docs(docs)
        except Exception as exc:
            logger.exception("Marine sync failed for %s", location)
            errors.append({"location": location, "error": str(exc)})

    response = {
        "synced": total,
        "snapshots": snapshots,
        "locations": locations,
        "errors": errors,
        "next_step": "Run notebooks/ingest_marine_embeddings.py (or the Spark pipeline notebook).",
    }
    analytics.log_event(
        "marine_sync",
        user_email=_current_user_email(),
        path="/marine/sync",
        payload={
            "synced": total,
            "snapshots": snapshots,
            "locations": locations,
            "error_count": len(errors),
        },
    )
    return jsonify(response)


@app.route("/marine/search", methods=["POST"])
def search_marine():
    body = request.json if request.is_json else {}
    query = body.get("query") if isinstance(body, dict) else None
    if not isinstance(query, str) or not query.strip():
        return jsonify({"error": "Missing or empty 'query' string"}), 400
    try:
        top_k = int(body.get("top_k", 5))
    except (TypeError, ValueError):
        return jsonify({"error": "'top_k' must be an integer"}), 400

    result = coastal_broker.search_marine_context(query.strip(), top_k=top_k)
    analytics.log_event(
        "marine_search",
        user_email=_current_user_email(),
        path="/marine/search",
        payload={
            "query": query.strip(),
            "top_k": top_k,
            "hit_count": len(result.get("results") or []),
        },
    )
    return jsonify(result)


@app.route("/voyages", methods=["GET"])
def get_voyages():
    status = request.args.get("status")
    result = coastal_broker.list_voyages(status=status)
    analytics.log_event(
        "voyages_list",
        user_email=_current_user_email(),
        path="/voyages",
        payload={"status": status, "count": len(result.get("voyages") or [])},
    )
    return jsonify({"voyages": _serialize_rows(result["voyages"])})


@app.route("/voyages", methods=["POST"])
def post_voyage():
    if not request.is_json:
        return jsonify({"error": "Expected JSON body"}), 400
    data = request.get_json(silent=True) or {}
    for field in ("name", "origin_port", "dest_port"):
        if not isinstance(data.get(field), str) or not data[field].strip():
            return jsonify({"error": f"Field '{field}' is required"}), 400

    result = coastal_broker.create_voyage(
        name=data["name"],
        origin_port=data["origin_port"],
        dest_port=data["dest_port"],
        planned_depart_at=data.get("planned_depart_at"),
        notes=data.get("notes"),
        created_by=_current_user_email(),
    )
    if result.get("status") == "error" or result.get("error"):
        err = result.get("message") or result.get("error")
        analytics.log_event(
            "voyage_create_failed",
            user_email=_current_user_email(),
            path="/voyages",
            payload={"error": err, "name": data.get("name")},
        )
        return jsonify(result), 400
    voyage = result.get("voyage") or {}
    analytics.log_event(
        "voyage_created",
        user_email=_current_user_email(),
        path="/voyages",
        payload={
            "voyage_id": voyage.get("voyage_id"),
            "name": voyage.get("name"),
            "origin": result.get("origin"),
            "dest": result.get("dest"),
        },
    )
    return jsonify(
        {
            "voyage": _serialize_row(result["voyage"]),
            "origin": result.get("origin"),
            "dest": result.get("dest"),
        }
    ), 201


@app.route("/alerts", methods=["GET"])
def get_alerts():
    status = request.args.get("status", "open")
    result = coastal_broker.list_alerts(status=status)
    analytics.log_event(
        "alerts_list",
        user_email=_current_user_email(),
        path="/alerts",
        payload={"status": status, "count": len(result.get("alerts") or [])},
    )
    return jsonify({"alerts": _serialize_rows(result["alerts"])})


def _bootstrap() -> None:
    try:
        lakebase.ensure_all_tables()
        coastal_broker.seed_default_ports()
        logger.info("Coastal Ops tables ready (%d default ports)", len(DEFAULT_PORTS))
    except Exception:
        logger.exception(
            "Could not ensure tables at startup — "
            "they will be needed before first successful request"
        )


_bootstrap()

if __name__ == "__main__":
    host = os.getenv("FLASK_RUN_HOST", "0.0.0.0")
    port = int(os.getenv("FLASK_RUN_PORT", os.getenv("DATABRICKS_APP_PORT", "8000")))
    logger.info("Starting Coastal Ops dashboard on http://%s:%s", host, port)
    app.run(
        debug=os.getenv("FLASK_DEBUG", "false").lower() == "true",
        host=host,
        port=port,
    )
