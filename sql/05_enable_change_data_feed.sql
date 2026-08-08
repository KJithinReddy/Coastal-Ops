-- Capstone: enable Delta Change Data Feed on the marine conditions table.
-- Prefer running notebooks/run_cdf_marine_analytics.ipynb (does enable + read + gold write).
--
-- Replace catalog/schema if needed, or use current_catalog()/current_schema() in a notebook.

-- ALTER TABLE main.default.coastal_ops_marine_conditions
-- SET TBLPROPERTIES (delta.enableChangeDataFeed = true);

-- Verify:
-- SHOW TBLPROPERTIES main.default.coastal_ops_marine_conditions;

-- Read CDF (PySpark):
--   spark.read.format("delta")
--        .option("readChangeFeed", "true")
--        .option("startingVersion", 0)
--        .table("main.default.coastal_ops_marine_conditions")
--
-- Analytics tables written by cdf_marine_analytics_to_delta.py:
--   …coastal_ops_marine_cdf_changes   (bronze)
--   …coastal_ops_marine_cdf_analytics (gold)
