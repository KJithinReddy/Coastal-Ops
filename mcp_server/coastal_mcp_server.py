"""
Coastal Ops MCP server.

Exposes coastal_broker tools over MCP so Playground (then an exported agent
app) can call them:

  Read:  list_ports, get_port_conditions, search_marine_context,
         list_voyages, list_alerts
  Write: create_voyage, reschedule_voyage, create_alert, save_ops_note,
         assess_and_act

Deploy this folder as its own Databricks App (separate from dashboard/),
register the URL as an external MCP, use Playground, then export the app.

Run locally:
    python coastal_mcp_server.py
"""

from __future__ import annotations

import logging
import os

from fastmcp import FastMCP

import coastal_broker
import lakebase

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("coastal-mcp-server")

mcp = FastMCP("coastal-ops")


@mcp.tool
def list_ports() -> dict:
    """
    List known coastal ports stored in Lakebase.

    Returns:
        A dict with a `ports` list (port_id, name, lat, lon, state, region).
        On failure returns {"status": "error", "message": "..."}.
    """
    try:
        lakebase.ensure_ops_tables()
        return coastal_broker.list_ports()
    except Exception as exc:
        logger.exception("list_ports failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def get_port_conditions(port_name: str) -> dict:
    """
    Fetch live Open-Meteo marine + wind conditions for a port and store a snapshot.

    Args:
        port_name: Port display name or id (e.g. "Miami, FL" or "miami").

    Returns:
        Port metadata, snapshot_id, and conditions (wave height, wind, risk_level).
        On failure returns {"status": "error", "message": "..."}.
    """
    try:
        return coastal_broker.get_port_conditions(port_name)
    except Exception as exc:
        logger.exception("get_port_conditions failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def search_marine_context(query: str, top_k: int = 5) -> dict:
    """
    Semantic (RAG) search over marine forecast and coastal-alert embeddings.

    Args:
        query: Natural-language marine question (e.g. "dangerous coastal waves").
        top_k: Max documents to return (1–10, default 5).

    Returns:
        A dict with `results` (similarity, headline, narrative_text, …).
        On failure returns {"status": "error", "message": "..."}.
    """
    try:
        return coastal_broker.search_marine_context(query, top_k=top_k)
    except Exception as exc:
        logger.exception("search_marine_context failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def list_voyages(status: str = "") -> dict:
    """
    List scheduled coastal voyages from Lakebase.

    Args:
        status: Optional filter — planned, deferred, underway, completed,
            cancelled. Pass empty string for all.

    Returns:
        A dict with a `voyages` list including origin/dest names.
    """
    try:
        return coastal_broker.list_voyages(status=status or None)
    except Exception as exc:
        logger.exception("list_voyages failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def create_voyage(
    name: str,
    origin_port: str,
    dest_port: str,
    planned_depart_at: str = "",
    notes: str = "",
) -> dict:
    """
    Create (write) a new coastal voyage between two known ports.

    Args:
        name: Voyage display name.
        origin_port: Origin port name or id (e.g. "Miami, FL").
        dest_port: Destination port name or id (e.g. "Boston, MA").
        planned_depart_at: Optional ISO datetime; defaults to ~12h from now.
        notes: Optional free-text notes.

    Returns:
        Created voyage row plus origin/dest names. On failure
        {"status": "error", "message": "..."}.
    """
    try:
        return coastal_broker.create_voyage(
            name=name,
            origin_port=origin_port,
            dest_port=dest_port,
            planned_depart_at=planned_depart_at or None,
            notes=notes or None,
            created_by="mcp-agent",
        )
    except Exception as exc:
        logger.exception("create_voyage failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def reschedule_voyage(
    voyage_id: str,
    new_depart_at: str = "",
    reason: str = "",
    defer_hours: int = 24,
) -> dict:
    """
    Defer/reschedule a voyage and create a warning alert (write).

    Args:
        voyage_id: UUID of the voyage to move.
        new_depart_at: Optional ISO datetime; otherwise defer by defer_hours.
        reason: Why the voyage was deferred (stored on voyage + alert).
        defer_hours: Hours to push departure if new_depart_at is empty (default 24).

    Returns:
        Updated voyage, alert, and new_depart_at.
    """
    try:
        return coastal_broker.reschedule_voyage(
            voyage_id=voyage_id,
            new_depart_at=new_depart_at or None,
            reason=reason or None,
            defer_hours=defer_hours,
            created_by="mcp-agent",
        )
    except Exception as exc:
        logger.exception("reschedule_voyage failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def create_alert(
    title: str,
    message: str,
    severity: str = "watch",
    voyage_id: str = "",
    port_name: str = "",
) -> dict:
    """
    Create an operational alert in Lakebase (write).

    Args:
        title: Short alert title.
        message: Alert body / explanation.
        severity: info | watch | warning | critical (default watch).
        voyage_id: Optional related voyage UUID.
        port_name: Optional related port name.

    Returns:
        Created alert row.
    """
    try:
        return coastal_broker.create_alert(
            title=title,
            message=message,
            severity=severity,
            voyage_id=voyage_id or None,
            port_name=port_name or None,
            created_by="mcp-agent",
        )
    except Exception as exc:
        logger.exception("create_alert failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def save_ops_note(note_text: str, voyage_id: str = "", port_name: str = "") -> dict:
    """
    Save an ops note tied to a voyage and/or port (write).

    Args:
        note_text: Note content.
        voyage_id: Optional voyage UUID.
        port_name: Optional port name.

    Returns:
        Created note row.
    """
    try:
        return coastal_broker.save_ops_note(
            note_text=note_text,
            voyage_id=voyage_id or None,
            port_name=port_name or None,
            created_by="mcp-agent",
        )
    except Exception as exc:
        logger.exception("save_ops_note failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def list_alerts(status: str = "open") -> dict:
    """
    List operational alerts from Lakebase.

    Args:
        status: Alert status filter (default "open").

    Returns:
        A dict with an `alerts` list.
    """
    try:
        return coastal_broker.list_alerts(status=status or "open")
    except Exception as exc:
        logger.exception("list_alerts failed")
        return {"status": "error", "message": str(exc)}


@mcp.tool
def assess_and_act(port_name: str, voyage_id: str = "") -> dict:
    """
    Go/no-go: check marine risk at a port; if high/severe, create an alert
    and optionally defer a voyage (read + write). Best demo tool.

    Args:
        port_name: Port to assess (e.g. "Miami, FL").
        voyage_id: Optional voyage UUID to defer when risk is high/severe.

    Returns:
        risk_level, conditions snapshot, and actions taken.
    """
    try:
        return coastal_broker.assess_and_act(
            port_name=port_name,
            voyage_id=voyage_id or None,
            created_by="mcp-agent",
        )
    except Exception as exc:
        logger.exception("assess_and_act failed")
        return {"status": "error", "message": str(exc)}


if __name__ == "__main__":
    port = int(os.getenv("DATABRICKS_APP_PORT", os.getenv("PORT", 8000)))
    logger.info("Starting Coastal Ops MCP server on 0.0.0.0:%s", port)
    try:
        lakebase.ensure_all_tables()
        coastal_broker.seed_default_ports()
    except Exception:
        logger.exception("Could not bootstrap Lakebase tables at startup")
    mcp.run(transport="http", host="0.0.0.0", port=port)
