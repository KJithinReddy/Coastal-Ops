"""
Spark marine pipeline (capstone requirement).

Playground demo path (no Flask dashboard):
1. Run this script / notebook → Delta + Lakebase docs
2. Run notebooks/ingest_marine_embeddings.ipynb
3. Use Playground + mcp_server tools

Runs on a Databricks cluster / Serverless Spark session:
1. Builds a ports DataFrame
2. Fetches Open-Meteo marine + wind forecasts (driver-side collect + map)
3. Writes a Delta bronze/silver table for structured conditions
4. Generates unstructured narrative documents
5. Upserts narratives + snapshots into Lakebase via psycopg2 (not Spark JDBC)

Usage (Databricks notebook):
    %run ./spark_marine_pipeline
or open notebooks/spark_marine_pipeline.ipynb
"""

from __future__ import annotations

import json
import logging
import os
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("spark-marine")


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
for _path in (_ROOT / "dashboard", _ROOT):
    if _path.exists() and str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

DELTA_PATH = os.environ.get("MARINE_DELTA_PATH", "")  # optional path or UC volume
CATALOG_TABLE = os.environ.get("MARINE_SPARK_TABLE", "")  # e.g. main.default.coastal_ops_marine_conditions
_LAST_DELTA_TARGET: str | None = None


def _get_spark():
    try:
        from pyspark.sql import SparkSession

        return SparkSession.builder.getOrCreate()
    except Exception as exc:
        raise RuntimeError(
            "Spark is required for this notebook. Run it on a Databricks cluster."
        ) from exc


def _default_uc_table(spark) -> str:
    """Unity Catalog table when public DBFS /tmp is disabled."""
    if CATALOG_TABLE:
        return CATALOG_TABLE
    try:
        catalog = spark.sql("SELECT current_catalog()").collect()[0][0]
        schema = spark.sql("SELECT current_schema()").collect()[0][0]
        return f"{catalog}.{schema}.coastal_ops_marine_conditions"
    except Exception:
        return "main.default.coastal_ops_marine_conditions"


def _is_dbfs_disabled_error(exc: BaseException) -> bool:
    text = f"{type(exc).__name__}: {exc}"
    return (
        "DBFS_DISABLED" in text
        or "Public DBFS root is disabled" in text
        or "56038" in text
    )


def _delta_table_identifier(kind: str, target: str) -> str:
    """SQL identifier for a UC table name or a Delta path."""
    if kind == "path":
        return f"delta.`{target}`"
    return target


def enable_change_data_feed(kind: str | None = None, target: str | None = None) -> str:
    """
    Capstone CDF requirement: turn on Delta Change Data Feed.

      ALTER TABLE … SET TBLPROPERTIES (delta.enableChangeDataFeed = true)

    Returns the SQL identifier that was altered.
    """
    spark = _get_spark()
    resolved_kind = kind
    resolved_target = target or _LAST_DELTA_TARGET or _default_uc_table(spark)
    if resolved_kind is None:
        resolved_kind = "path" if str(resolved_target).startswith("/") else "table"

    ident = _delta_table_identifier(resolved_kind, resolved_target)
    spark.sql(
        f"ALTER TABLE {ident} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
    )
    logger.info("Enabled Delta Change Data Feed on %s", ident)
    try:
        props = spark.sql(f"SHOW TBLPROPERTIES {ident}").collect()
        for row in props:
            key = row[0] if len(row) > 0 else None
            val = row[1] if len(row) > 1 else None
            if key and "enableChangeDataFeed" in str(key):
                logger.info("TBLPROPERTIES %s = %s", key, val)
    except Exception as exc:
        logger.warning("Could not SHOW TBLPROPERTIES for %s: %s", ident, exc)
    return ident


def fetch_conditions_rows() -> list[dict]:
    """Pull live marine conditions for default ports (shared client with the app)."""
    from marine_client import DEFAULT_PORTS, MarineClient

    client = MarineClient()
    rows: list[dict] = []
    for port in DEFAULT_PORTS:
        try:
            cond = client.current_conditions(port["lat"], port["lon"])
            rows.append(
                {
                    "port_id": port["port_id"],
                    "port_name": port["name"],
                    "lat": float(port["lat"]),
                    "lon": float(port["lon"]),
                    "state": port.get("state"),
                    "region": port.get("region"),
                    "wave_height_m": cond.get("wave_height_m"),
                    "wind_speed_ms": cond.get("wind_speed_ms"),
                    "wind_direction_deg": cond.get("wind_direction_deg"),
                    "swell_wave_height_m": cond.get("swell_wave_height_m"),
                    "swell_wave_period_s": cond.get("swell_wave_period_s"),
                    "risk_level": cond.get("risk_level"),
                    "summary_text": cond.get("summary_text"),
                    "observed_at": str(cond.get("observed_at")),
                    "ingested_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        except Exception as exc:
            logger.exception("Failed conditions for %s: %s", port["name"], exc)
    return rows


def write_spark_tables(rows: list[dict]):
    """Transform with Spark and write Delta (UC table by default; path/volume optional)."""
    global _LAST_DELTA_TARGET
    from pyspark.sql import functions as F

    spark = _get_spark()
    df = spark.createDataFrame(rows)
    silver = (
        df.withColumn("wave_height_m", F.col("wave_height_m").cast("double"))
        .withColumn("wind_speed_ms", F.col("wind_speed_ms").cast("double"))
        .withColumn(
            "is_go_no_go",
            F.when(F.col("risk_level").isin("high", "severe"), F.lit("NO-GO")).otherwise(
                F.lit("GO")
            ),
        )
        .withColumn("pipeline_run_id", F.lit(str(uuid.uuid4())))
    )

    # Prefer an explicit path/volume when set; otherwise write a UC managed table.
    # Workspaces with public DBFS disabled reject /tmp/... path writes.
    targets: list[tuple[str, str]] = []
    if DELTA_PATH:
        targets.append(("path", DELTA_PATH))
    targets.append(("table", _default_uc_table(spark)))

    last_error: BaseException | None = None
    for kind, target in targets:
        try:
            writer = (
                silver.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
            )
            if kind == "path":
                writer.save(target)
            else:
                writer.saveAsTable(target)
            _LAST_DELTA_TARGET = target
            logger.info(
                "Wrote Delta silver (%s) → %s (%d rows)",
                kind,
                target,
                silver.count(),
            )
            # Capstone: enable CDF so later jobs can readChangeFeed.
            try:
                enable_change_data_feed(kind=kind, target=target)
            except Exception as cdf_exc:
                logger.warning("Could not enable Change Data Feed on %s: %s", target, cdf_exc)
            break
        except Exception as exc:
            last_error = exc
            if kind == "path" and _is_dbfs_disabled_error(exc):
                logger.warning(
                    "Path write blocked (DBFS disabled): %s — falling back to UC table",
                    target,
                )
                continue
            if kind == "table":
                raise
            logger.warning("Path write failed for %s: %s — trying UC table", target, exc)
    else:
        if last_error:
            raise last_error

    return silver.toPandas().to_dict(orient="records")


def sync_lakebase(silver_rows: list[dict], also_harvest_docs: bool = True) -> dict:
    """
    Upsert structured snapshots (+ optional unstructured docs) into Lakebase.

    Embeddings stay on the HW2-style psycopg2 MiniLM path — not Spark JDBC.
    """
    import lakebase
    from marine_client import MarineClient

    lakebase.ensure_all_tables()

    # Seed ports referenced by snapshots.
    for row in silver_rows:
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
                row["port_id"],
                row["port_name"],
                row["lat"],
                row["lon"],
                row.get("state"),
                row.get("region"),
            ),
        )

    snap_count = 0
    for row in silver_rows:
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
                str(uuid.uuid4()),
                row["port_id"],
                row.get("wave_height_m"),
                row.get("wind_speed_ms"),
                row.get("wind_direction_deg"),
                row.get("swell_wave_height_m"),
                row.get("swell_wave_period_s"),
                row.get("risk_level") or "unknown",
                row.get("summary_text") or "",
                row.get("observed_at"),
                json.dumps({"spark_pipeline": True, "is_go_no_go": row.get("is_go_no_go")}),
            ),
        )
        snap_count += 1

    doc_count = 0
    if also_harvest_docs:
        client = MarineClient()
        with lakebase.get_connection() as conn:
            with conn.cursor() as cur:
                for row in silver_rows:
                    docs = client.harvest_location(
                        row["port_name"],
                        lat=row["lat"],
                        lon=row["lon"],
                        state=row.get("state"),
                        limit=24,
                    )
                    for doc in docs:
                        cur.execute(
                            """
                            INSERT INTO marine_documents (
                                id, location, source_type, source_url, headline, event,
                                narrative_text, issued_at, effective_at, payload, synced_at
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, now())
                            ON CONFLICT (id) DO UPDATE SET
                                narrative_text = EXCLUDED.narrative_text,
                                headline = EXCLUDED.headline,
                                event = EXCLUDED.event,
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
                        doc_count += 1
                conn.commit()

    return {
        "snapshots": snap_count,
        "documents": doc_count,
        "delta_target": _LAST_DELTA_TARGET or CATALOG_TABLE or DELTA_PATH or "",
    }


def main() -> None:
    logger.info("Starting Spark marine pipeline")
    rows = fetch_conditions_rows()
    if not rows:
        logger.error("No marine rows fetched — aborting")
        return
    silver_rows = write_spark_tables(rows)
    result = sync_lakebase(silver_rows, also_harvest_docs=True)
    logger.info("Pipeline complete: %s", result)
    logger.info(
        "Next: run notebooks/run_cdf_marine_analytics.ipynb (CDF → analytics), "
        "then notebooks/ingest_marine_embeddings.py."
    )


if __name__ == "__main__":
    main()
