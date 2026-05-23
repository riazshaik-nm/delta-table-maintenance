"""Z-ORDER optimization for Delta tables — co-locate data for faster queries."""

from __future__ import annotations

import time

from pyspark.sql import SparkSession

from delta_maintenance.config import TableConfig
from delta_maintenance.models import ZOrderResult


def run_zorder(
    spark: SparkSession,
    table_config: TableConfig,
    columns: list[str] | None = None,
    dry_run: bool = False,
) -> ZOrderResult:
    """Run Z-ORDER optimization on a Delta table.

    Z-ordering co-locates related data in the same files, dramatically
    improving query performance for filters on the Z-ordered columns.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    table_config : TableConfig
        Table configuration (path, name, etc.).
    columns : list[str], optional
        Override Z-ORDER columns.
    dry_run : bool
        If True, report what would happen without executing.

    Returns
    -------
    ZOrderResult
        Metrics from the Z-ORDER operation.
    """
    zorder_cols = columns or table_config.zorder_columns

    if not zorder_cols:
        return ZOrderResult(
            table_name=table_config.name,
            columns=[],
            skipped=True,
        )

    if dry_run:
        return ZOrderResult(
            table_name=table_config.name,
            columns=zorder_cols,
            skipped=True,
        )

    detail_before = spark.sql(
        f"DESCRIBE DETAIL delta.`{table_config.path}`"
    ).collect()[0]
    files_before = (
        detail_before.numFiles
        if hasattr(detail_before, "numFiles")
        else 0
    )

    start = time.time()
    col_list = ", ".join(zorder_cols)
    spark.sql(
        f"OPTIMIZE delta.`{table_config.path}` ZORDER BY ({col_list})"
    )
    elapsed = time.time() - start

    return ZOrderResult(
        table_name=table_config.name,
        columns=zorder_cols,
        files_rewritten=files_before,
        duration_seconds=round(elapsed, 2),
    )
