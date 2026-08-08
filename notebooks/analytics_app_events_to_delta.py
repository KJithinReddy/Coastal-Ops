"""
App analytics → Delta (capstone: "Analytics inside Delta Table about your app").

Flow:
1. Flask app logs usage to Lakebase `app_events` (via analytics.log_event)
2. This Spark job reads those events
3. Writes:
   - Delta bronze: coastal_ops_app_events (full event log)
   - Delta gold:   coastal_ops_app_analytics_daily (counts by day / event_type)
   - Delta gold:   coastal_ops_agent_tool_usage (agent tool call frequency)

Run on a Databricks cluster / Serverless Spark:
    python notebooks/analytics_app_events_to_delta.py
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("app-analytics-delta")


def _project_root() -> Path:
    try:
        return Path(__file__).resolve().parents[1]
    except NameError:
        cwd = Path.cwd().resolve()
        for candidate in (cwd, cwd.parent, *cwd.parents):
            if (candidate / "dashboard" / "lakebase.py").exists():
                return candidate
            if (candidate / "lakebase.py").exists():
                return candidate
        return cwd.parent if cwd.name == "notebooks" else cwd


_ROOT = _project_root()
_DASHBOARD = _ROOT / "dashboard"
for _path in (_DASHBOARD, _ROOT):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

EVENTS_DELTA_PATH = os.environ.get(
    "APP_EVENTS_DELTA_PATH",
    "/tmp/coastal_ops/app_events_bronze",
)
DAILY_DELTA_PATH = os.environ.get(
    "APP_ANALYTICS_DAILY_DELTA_PATH",
    "/tmp/coastal_ops/app_analytics_daily",
)
TOOLS_DELTA_PATH = os.environ.get(
    "APP_AGENT_TOOLS_DELTA_PATH",
    "/tmp/coastal_ops/agent_tool_usage",
)
# Optional Unity Catalog tables, e.g. main.coastal.app_events
EVENTS_TABLE = os.environ.get("APP_EVENTS_SPARK_TABLE", "")
DAILY_TABLE = os.environ.get("APP_ANALYTICS_DAILY_SPARK_TABLE", "")
TOOLS_TABLE = os.environ.get("APP_AGENT_TOOLS_SPARK_TABLE", "")


def _get_spark():
    try:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    except Exception as exc:
        raise RuntimeError(
            "Spark is required. Run this notebook on a Databricks cluster."
        ) from exc


def fetch_app_events() -> list[dict]:
    """Pull staged app usage events from Lakebase."""
    import lakebase

    lakebase.ensure_app_events_table()
    rows = lakebase.run_query(
        """
        SELECT event_id, event_type, user_email, path, payload::text AS payload_json, created_at
        FROM app_events
        ORDER BY created_at
        """
    )
    out: list[dict] = []
    for row in rows:
        created = row.get("created_at")
        if hasattr(created, "isoformat"):
            created = created.isoformat()
        out.append(
            {
                "event_id": row["event_id"],
                "event_type": row["event_type"],
                "user_email": row.get("user_email"),
                "path": row.get("path"),
                "payload_json": row.get("payload_json") or "{}",
                "created_at": str(created) if created is not None else None,
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    return out


def write_delta_analytics(events: list[dict]) -> dict:
    """Transform with Spark and write bronze + gold Delta tables."""
    from pyspark.sql import functions as F

    spark = _get_spark()
    if not events:
        logger.warning("No app_events in Lakebase yet — writing empty schemas")
        # Seed a placeholder so Delta paths exist for the demo.
        events = [
            {
                "event_id": "placeholder",
                "event_type": "pipeline_bootstrap",
                "user_email": "system",
                "path": None,
                "payload_json": "{}",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
        ]

    bronze = spark.createDataFrame(events).withColumn(
        "event_date", F.to_date(F.col("created_at"))
    )

    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(EVENTS_DELTA_PATH)
    )
    logger.info("Wrote bronze Delta app events → %s (%d rows)", EVENTS_DELTA_PATH, bronze.count())

    if EVENTS_TABLE:
        (
            bronze.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(EVENTS_TABLE)
        )
        logger.info("Wrote UC table %s", EVENTS_TABLE)

    daily = (
        bronze.groupBy("event_date", "event_type")
        .agg(
            F.count("*").alias("event_count"),
            F.countDistinct("user_email").alias("unique_users"),
        )
        .orderBy("event_date", "event_type")
    )
    (
        daily.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(DAILY_DELTA_PATH)
    )
    logger.info("Wrote daily analytics Delta → %s", DAILY_DELTA_PATH)

    if DAILY_TABLE:
        (
            daily.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(DAILY_TABLE)
        )

    # Explode agent tool names from agent_chat payloads when present.
    agent = bronze.filter(F.col("event_type") == "agent_chat")
    # payload_json may contain {"tools": ["create_voyage", ...]}
    tools_df = (
        agent.withColumn(
            "tools",
            F.from_json(F.col("payload_json"), "tools ARRAY<STRING>").getField("tools"),
        )
        .withColumn("tool_name", F.explode_outer("tools"))
        .filter(F.col("tool_name").isNotNull())
        .groupBy("event_date", "tool_name")
        .agg(F.count("*").alias("call_count"))
        .orderBy(F.desc("call_count"))
    )

    # If no tools parsed, still write an empty-friendly frame with schema.
    if tools_df.rdd.isEmpty():
        tools_df = spark.createDataFrame(
            [],
            schema="event_date DATE, tool_name STRING, call_count LONG",
        )

    (
        tools_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(TOOLS_DELTA_PATH)
    )
    logger.info("Wrote agent tool usage Delta → %s", TOOLS_DELTA_PATH)

    if TOOLS_TABLE:
        (
            tools_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(TOOLS_TABLE)
        )

    summary = {
        "events_total": bronze.filter(F.col("event_id") != "placeholder").count(),
        "event_types": [
            r.asDict()
            for r in bronze.groupBy("event_type")
            .count()
            .orderBy(F.desc("count"))
            .collect()
        ],
        "daily_rows": daily.count(),
        "tool_usage_rows": tools_df.count(),
        "paths": {
            "bronze_events": EVENTS_DELTA_PATH,
            "daily_analytics": DAILY_DELTA_PATH,
            "agent_tools": TOOLS_DELTA_PATH,
        },
    }
    logger.info("Analytics summary: %s", json.dumps(summary, default=str))
    return summary


def main() -> dict:
    logger.info("Starting app analytics → Delta pipeline")
    events = fetch_app_events()
    logger.info("Fetched %d app_events from Lakebase", len(events))
    summary = write_delta_analytics(events)
    print(json.dumps(summary, indent=2, default=str))
    logger.info(
        "Done. Demo tip: display(spark.read.format('delta').load('%s'))",
        DAILY_DELTA_PATH,
    )
    return summary


_should_run = __name__ == "__main__"
try:
    _should_run = _should_run or get_ipython() is not None  # noqa: F821
except NameError:
    pass

if _should_run:
    main()
