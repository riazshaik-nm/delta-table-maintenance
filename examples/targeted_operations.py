"""Targeted maintenance — run individual operations on specific tables.

Use this approach when you need fine-grained control over which
operations run on which tables, rather than the all-in-one runner.
"""

from pyspark.sql import SparkSession

from delta_maintenance import TableConfig, run_optimize, run_vacuum, run_zorder
from delta_maintenance.analyzer import TableAnalyzer
from delta_maintenance.reports import print_health_report

spark = (
    SparkSession.builder
    .appName("delta-targeted-ops")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

orders_table = TableConfig(
    path="/delta/fact_orders",
    name="fact_orders",
    zorder_columns=["order_date", "customer_id"],
    vacuum_retention_hours=168,
)

# --- Analyze first ---
analyzer = TableAnalyzer(spark)
health = analyzer.analyze(orders_table)
print(f"Health score: {health.health_score}/100")
print(f"Small files: {health.small_file_count} ({health.small_file_pct}%)")

# --- Conditionally optimize ---
if health.needs_optimize:
    opt_result = run_optimize(spark, orders_table)
    print(f"OPTIMIZE: compacted {opt_result.files_compacted} files in {opt_result.duration_seconds}s")
else:
    print("OPTIMIZE: not needed")

# --- Z-ORDER separately ---
zorder_result = run_zorder(spark, orders_table, columns=["order_date"])
print(f"Z-ORDER: rewrote {zorder_result.files_rewritten} files")

# --- VACUUM with custom retention ---
vac_result = run_vacuum(spark, orders_table, retention_hours=72)
print(f"VACUUM: freed {vac_result.space_freed_mb} MB")

spark.stop()
