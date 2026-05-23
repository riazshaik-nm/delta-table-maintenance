"""VACUUM operation for Delta tables — removes stale files outside retention."""

from __future__ import annotations

import time

from pyspark.sql import SparkSession

from delta_maintenance.config import TableConfig
from delta_maintenance.models import VacuumResult


def run_vacuum(
    spark: SparkSession,
    table_config: TableConfig,
    retention_hours: int | None = None,
    dry_run: bool = False,
) -> VacuumResult:
    """Run VACUUM on a Delta table.

    Removes data files no longer referenced by the Delta log that are
    older than the retention period.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    table_config : TableConfig
        Configuration for the table to vacuum.
    retention_hours : int, optional
        Override retention period.
    dry_run : bool
        If True, report what would be deleted without executing.

    Returns
    -------
    VacuumResult
        Metrics from the vacuum operation.
    """
    hours = retention_hours or table_config.vacuum_retention_hours

    if dry_run:
        dry_result = spark.sql(
            f"VACUUM delta.`{table_config.path}` "
            f"RETAIN {hours} HOURS DRY RUN"
        )
        count = dry_result.count()
        return VacuumResult(
            table_name=table_config.name,
            retention_hours=hours,
            files_deleted=count,
            skipped=True,
        )

    size_before = _get_table_size_mb(spark, table_config.path)

    start = time.time()

    spark.conf.set(
        "spark.databricks.delta.retentionDurationCheck.enabled", "false"
    )
    spark.sql(
        f"VACUUM delta.`{table_config.path}` RETAIN {hours} HOURS"
    )
    spark.conf.set(
        "spark.databricks.delta.retentionDurationCheck.enabled", "true"
    )

    elapsed = time.time() - start
    size_after = _get_table_size_mb(spark, table_config.path)
    freed = max(0, size_before - size_after)

    return VacuumResult(
        table_name=table_config.name,
        retention_hours=hours,
        space_freed_mb=round(freed, 2),
        duration_seconds=round(elapsed, 2),
    )


def _get_table_size_mb(spark: SparkSession, path: str) -> float:
    """Get current table size in MB from DESCRIBE DETAIL."""
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").collect()[0]
    size_bytes = (
        detail.sizeInBytes if hasattr(detail, "sizeInBytes") else 0
    )
    return size_bytes / (1024 * 1024)
