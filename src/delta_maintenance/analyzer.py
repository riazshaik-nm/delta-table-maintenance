"""Delta table health analyzer — file size distribution, small file detection, staleness."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from delta_maintenance.config import TableConfig


@dataclass
class TableHealth:
    """Health report for a single Delta table.

    Attributes
    ----------
    table_name : str
        Name of the table.
    table_path : str
        Path to the Delta table.
    total_files : int
        Total number of data files.
    total_size_mb : float
        Total size of all data files in MB.
    avg_file_size_mb : float
        Average file size in MB.
    min_file_size_mb : float
        Smallest file size in MB.
    max_file_size_mb : float
        Largest file size in MB.
    small_file_count : int
        Number of files below the small-file threshold.
    small_file_pct : float
        Percentage of files that are small.
    num_partitions : int
        Number of partitions (0 if unpartitioned).
    last_modified : datetime | None
        Timestamp of the most recent file modification.
    version : int
        Current Delta table version.
    needs_optimize : bool
        True if the table has significant small-file issues.
    needs_vacuum : bool
        True if there are files outside retention to clean.
    """

    table_name: str
    table_path: str
    total_files: int = 0
    total_size_mb: float = 0.0
    avg_file_size_mb: float = 0.0
    min_file_size_mb: float = 0.0
    max_file_size_mb: float = 0.0
    small_file_count: int = 0
    small_file_pct: float = 0.0
    num_partitions: int = 0
    last_modified: datetime | None = None
    version: int = 0
    needs_optimize: bool = False
    needs_vacuum: bool = False

    @property
    def is_healthy(self) -> bool:
        return not self.needs_optimize and not self.needs_vacuum

    @property
    def health_score(self) -> int:
        """Score from 0-100. Higher is healthier."""
        score = 100
        if self.small_file_pct > 50:
            score -= 40
        elif self.small_file_pct > 25:
            score -= 20
        elif self.small_file_pct > 10:
            score -= 10
        if self.needs_vacuum:
            score -= 15
        if self.total_files > 1000 and self.small_file_pct > 30:
            score -= 15
        return max(0, score)


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
    >>> print(f"Small files: {health.small_file_count} ({health.small_file_pct:.1f}%)")
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

        dt = DeltaTable.forPath(self._spark, table_config.path)
        detail = self._spark.sql(f"DESCRIBE DETAIL delta.`{table_config.path}`").collect()[0]

        version = detail.version if hasattr(detail, "version") else 0
        num_files = detail.numFiles if hasattr(detail, "numFiles") else 0
        size_bytes = detail.sizeInBytes if hasattr(detail, "sizeInBytes") else 0
        partitions = len(detail.partitionColumns) if hasattr(detail, "partitionColumns") else 0

        total_size_mb = size_bytes / (1024 * 1024)
        avg_size_mb = total_size_mb / max(num_files, 1)

        # Analyze file sizes from the Delta log
        add_files = (
            self._spark.read.format("delta")
            .load(table_config.path)
        )

        file_stats = self._get_file_stats(table_config.path)

        small_file_count = file_stats.get("small_count", 0)
        small_pct = (small_file_count / max(num_files, 1)) * 100

        needs_opt = small_pct > 20 or (num_files > 100 and avg_size_mb < table_config.target_file_size_mb * 0.5)

        last_mod = detail.lastModified if hasattr(detail, "lastModified") else None

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
                        F.min("size").alias("min_size"),
                        F.max("size").alias("max_size"),
                    ).collect()[0]
                    return {
                        "min_mb": (stats.min_size or 0) / (1024 * 1024),
                        "max_mb": (stats.max_size or 0) / (1024 * 1024),
                        "small_count": adds.filter(
                            F.col("size") < 10 * 1024 * 1024
                        ).count(),
                    }
        except Exception:
            pass
        return {"min_mb": 0, "max_mb": 0, "small_count": 0}
