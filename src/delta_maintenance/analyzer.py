"""Delta table health analyzer — file size distribution, small file detection, staleness."""

from __future__ import annotations

import pyspark.sql.functions as fn
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

from delta_maintenance.config import TableConfig
from delta_maintenance.models import TableHealth


class TableAnalyzer:
    """Analyzes Delta table health and identifies maintenance needs.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.

    Examples
    --------
    >>> analyzer = TableAnalyzer(spark)
    >>> health = analyzer.analyze(table_config)
    >>> print(f"Health: {health.health_score}/100")
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def analyze(self, table_config: TableConfig) -> TableHealth:
        """Analyze a Delta table and return its health report."""
        if not DeltaTable.isDeltaTable(self._spark, table_config.path):
            return TableHealth(
                table_name=table_config.name,
                table_path=table_config.path,
            )

        detail = self._spark.sql(
            f"DESCRIBE DETAIL delta.`{table_config.path}`"
        ).collect()[0]

        version = detail.version if hasattr(detail, "version") else 0
        num_files = detail.numFiles if hasattr(detail, "numFiles") else 0
        size_bytes = detail.sizeInBytes if hasattr(detail, "sizeInBytes") else 0
        partitions = (
            len(detail.partitionColumns)
            if hasattr(detail, "partitionColumns")
            else 0
        )

        total_size_mb = size_bytes / (1024 * 1024)
        avg_size_mb = total_size_mb / max(num_files, 1)

        file_stats = self._get_file_stats(table_config.path)

        small_file_count = file_stats.get("small_count", 0)
        small_pct = (small_file_count / max(num_files, 1)) * 100

        target_half = table_config.target_file_size_mb * 0.5
        needs_opt = small_pct > 20 or (
            num_files > 100 and avg_size_mb < target_half
        )

        last_mod = (
            detail.lastModified
            if hasattr(detail, "lastModified")
            else None
        )

        return TableHealth(
            table_name=table_config.name,
            table_path=table_config.path,
            total_files=num_files,
            total_size_mb=round(total_size_mb, 2),
            avg_file_size_mb=round(avg_size_mb, 2),
            min_file_size_mb=round(file_stats.get("min_mb", 0), 2),
            max_file_size_mb=round(file_stats.get("max_mb", 0), 2),
            small_file_count=small_file_count,
            small_file_pct=round(small_pct, 1),
            num_partitions=partitions,
            last_modified=last_mod,
            version=version,
            needs_optimize=needs_opt,
            needs_vacuum=version > 10,
        )

    def _get_file_stats(self, path: str) -> dict:
        """Get file-level statistics from the Delta log."""
        try:
            log_df = self._spark.read.json(f"{path}/_delta_log/*.json")
            if "add" in log_df.columns:
                adds = log_df.select("add.size").filter("add IS NOT NULL")
                if adds.count() > 0:
                    stats = adds.agg(
                        fn.min("size").alias("min_size"),
                        fn.max("size").alias("max_size"),
                    ).collect()[0]
                    return {
                        "min_mb": (stats.min_size or 0) / (1024 * 1024),
                        "max_mb": (stats.max_size or 0) / (1024 * 1024),
                        "small_count": adds.filter(
                            fn.col("size") < 10 * 1024 * 1024
                        ).count(),
                    }
        except Exception:
            pass
        return {"min_mb": 0, "max_mb": 0, "small_count": 0}
