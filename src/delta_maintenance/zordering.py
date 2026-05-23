"""Z-ORDER optimization for Delta tables — co-locate data for faster queries."""

from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark.sql import SparkSession

from delta_maintenance.config import TableConfig


@dataclass(frozen=True)
class ZOrderResult:
    """Result of a Z-ORDER operation.

    Attributes
    ----------
    table_name : str
        Name of the Z-ordered table.
    columns : list[str]
        Columns used for Z-ordering.
    files_rewritten : int
        Number of files rewritten.
    duration_seconds : float
        Wall clock time for the operation.
    skipped : bool
        True if skipped (no zorder_columns configured or dry run).
    """

    table_name: str
    columns: list[str]
    files_rewritten: int = 0
    duration_seconds: float = 0.0
    skipped: bool = False


def run_zorder(
    spark: SparkSession,
    table_config: TableConfig,
    columns: list[str] | None = None,
    dry_run: bool = False,
) -> ZOrderResult:
    """Run Z-ORDER optimization on a Delta table.

    Z-ordering co-locates related data in the same files, dramatically
    improving query performance for filters on the Z-ordered columns.
    Best used on high-cardinality columns that appear in WHERE clauses.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    table_config : TableConfig
        Table configuration (path, name, etc.).
    columns : list[str], optional
        Override Z-ORDER columns. Defaults to table_config.zorder_columns.
    dry_run : bool
        If True, report what would happen without executing.

    Returns
    -------
    ZOrderResult
        Metrics from the Z-ORDER operation.

    Notes
    -----
    Z-ORDER is executed as part of OPTIMIZE. This function is a convenience
    wrapper that makes the Z-ORDER intent explicit in your maintenance code.

    Best practices for choosing Z-ORDER columns:
    - Pick 1-4 columns used most often in WHERE clauses
    - High cardinality columns benefit most (e.g., user_id, timestamp)
    - Order matters: put the most-filtered column first
    - Don't Z-ORDER on partition columns (already segregated)

    Examples
    --------
    >>> result = run_zorder(spark, table_config, columns=["customer_id", "order_date"])
    >>> print(f"Rewrote {result.files_rewritten} files")
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
    files_before = detail_before.numFiles if hasattr(detail_before, "numFiles") else 0

    start = time.time()
    col_list = ", ".join(zorder_cols)
    spark.sql(f"OPTIMIZE delta.`{table_config.path}` ZORDER BY ({col_list})")
    elapsed = time.time() - start

    return ZOrderResult(
        table_name=table_config.name,
        columns=zorder_cols,
        files_rewritten=files_before,
        duration_seconds=round(elapsed, 2),
    )
