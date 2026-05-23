"""OPTIMIZE operation for Delta tables — compacts small files."""

from __future__ import annotations

import time
from dataclasses import dataclass

from pyspark.sql import SparkSession

from delta_maintenance.config import TableConfig


@dataclass(frozen=True)
class OptimizeResult:
    """Result of an OPTIMIZE operation.

    Attributes
    ----------
    table_name : str
        Name of the optimized table.
    files_before : int
        Number of files before optimization.
    files_after : int
        Number of files after optimization.
    files_removed : int
        Number of small files that were compacted.
    bytes_written : int
        Bytes written during compaction.
    duration_seconds : float
        Wall clock time for the operation.
    skipped : bool
        True if the operation was skipped (dry run or already optimal).
    """

    table_name: str
    files_before: int = 0
    files_after: int = 0
    files_removed: int = 0
    bytes_written: int = 0
    duration_seconds: float = 0.0
    skipped: bool = False

    @property
    def files_compacted(self) -> int:
        return max(0, self.files_before - self.files_after)


def run_optimize(
    spark: SparkSession,
    table_config: TableConfig,
    dry_run: bool = False,
) -> OptimizeResult:
    """Run OPTIMIZE on a Delta table.

    Compacts small files into larger ones, targeting ``target_file_size_mb``.
    When ``zorder_columns`` is set, data is co-located by those columns
    for faster query performance.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    table_config : TableConfig
        Configuration for the table to optimize.
    dry_run : bool
        If True, report what would happen without executing.

    Returns
    -------
    OptimizeResult
        Metrics from the optimization.

    Examples
    --------
    >>> result = run_optimize(spark, table_config)
    >>> print(f"Compacted {result.files_compacted} files in {result.duration_seconds:.1f}s")
    """
    detail_before = spark.sql(
        f"DESCRIBE DETAIL delta.`{table_config.path}`"
    ).collect()[0]
    files_before = detail_before.numFiles if hasattr(detail_before, "numFiles") else 0

    if dry_run:
        return OptimizeResult(
            table_name=table_config.name,
            files_before=files_before,
            skipped=True,
        )

    start = time.time()

    if table_config.zorder_columns:
        zorder_cols = ", ".join(table_config.zorder_columns)
        spark.sql(
            f"OPTIMIZE delta.`{table_config.path}` ZORDER BY ({zorder_cols})"
        )
    else:
        spark.sql(f"OPTIMIZE delta.`{table_config.path}`")

    elapsed = time.time() - start

    detail_after = spark.sql(
        f"DESCRIBE DETAIL delta.`{table_config.path}`"
    ).collect()[0]
    files_after = detail_after.numFiles if hasattr(detail_after, "numFiles") else 0
    size_after = detail_after.sizeInBytes if hasattr(detail_after, "sizeInBytes") else 0

    return OptimizeResult(
        table_name=table_config.name,
        files_before=files_before,
        files_after=files_after,
        files_removed=max(0, files_before - files_after),
        bytes_written=size_after,
        duration_seconds=round(elapsed, 2),
    )
