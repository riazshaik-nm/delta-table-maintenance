"""Basic Delta table maintenance example.

Demonstrates how to:
1. Configure maintenance for multiple tables
2. Run health analysis
3. Execute OPTIMIZE + VACUUM
4. View the maintenance report
"""

from pyspark.sql import SparkSession

from delta_maintenance import (
    MaintenanceConfig,
    MaintenanceRunner,
    TableConfig,
)
from delta_maintenance.reports import print_health_report, print_maintenance_report

spark = (
    SparkSession.builder
    .appName("delta-maintenance-example")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

# --- Option 1: Configure programmatically ---
config = MaintenanceConfig(
    tables=[
        TableConfig(
            path="/delta/fact_orders",
            name="fact_orders",
            zorder_columns=["order_date", "customer_id"],
            vacuum_retention_hours=168,
        ),
        TableConfig(
            path="/delta/dim_customers",
            name="dim_customers",
            zorder_columns=["customer_id"],
            vacuum_retention_hours=720,
        ),
    ],
    dry_run=True,  # Preview mode — no changes made
)

# --- Option 2: Load from YAML ---
# config = MaintenanceConfig.from_yaml("maintenance.yml")

runner = MaintenanceRunner(spark, config)

# Step 1: Analyze table health
print("=== Health Analysis ===")
health_reports = runner.analyze_all()
print_health_report(health_reports)

# Step 2: Run maintenance
print("\n=== Running Maintenance ===")
report = runner.run_all()
print_maintenance_report(report)

# Step 3: Check the summary
print(f"\nSummary: {report.summary()}")

spark.stop()
