"""Orchestrates maintenance operations across multiple Delta tables."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from pyspark.sql import SparkSession

from delta_maintenance.analyzer import TableAnalyzer, TableHealth
from delta_maintenance.config import MaintenanceConfig, TableConfig
from delta_maintenance.optimizer import OptimizeResult, run_optimize
from delta_maintenance.vacuum import VacuumResult, run_vacuum
from delta_maintenance.zordering import ZOrderResult, run_zorder


@dataclass
class TableMaintenanceResult:
    """Combined result of all maintenance operations on a single table."""

    table_name: str
    table_path: str
    health_before: TableHealth | None = None
    optimize_result: OptimizeResult | None = None
    vacuum_result: VacuumResult | None = None
    zorder_result: ZOrderResult | None = None
    health_after: TableHealth | None = None
    error: str | None = None
    duration_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return self.error is None


@dataclass
class MaintenanceReport:
    """Report from a full maintenance run across all configured tables."""

    results: list[TableMaintenanceResult] = field(default_factory=list)
    started_at: datetime = field(default_factory=datetime.now)
    completed_at: datetime | None = None
    total_duration_seconds: float = 0.0

    @property
    def tables_processed(self) -> int:
        return len(self.results)

    @property
    def tables_succeeded(self) -> int:
        return sum(1 for r in self.results if r.success)

    @property
    def tables_failed(self) -> int:
        return sum(1 for r in self.results if not r.success)

    def summary(self) -> dict[str, Any]:
        return {
            "tables_processed": self.tables_processed,
            "tables_succeeded": self.tables_succeeded,
            "tables_failed": self.tables_failed,
            "total_duration_seconds": round(self.total_duration_seconds, 2),
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }


class MaintenanceRunner:
    """Orchestrates maintenance across all configured Delta tables.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    config : MaintenanceConfig
        Maintenance configuration for all tables.

    Examples
    --------
    >>> config = MaintenanceConfig.from_yaml("maintenance.yml")
    >>> runner = MaintenanceRunner(spark, config)
    >>> report = runner.run_all()
    >>> print(f"Processed {report.tables_processed} tables")
    >>> for result in report.results:
    ...     print(f"  {result.table_name}: {'OK' if result.success else result.error}")
    """

    def __init__(self, spark: SparkSession, config: MaintenanceConfig) -> None:
        self._spark = spark
        self._config = config
        self._analyzer = TableAnalyzer(spark)

    def run_all(self) -> MaintenanceReport:
        """Run maintenance on all enabled tables."""
        report = MaintenanceReport()
        start = time.time()

        for table_config in self._config.enabled_tables:
            result = self.run_table(table_config)
            report.results.append(result)

        report.total_duration_seconds = round(time.time() - start, 2)
        report.completed_at = datetime.now()
        return report

    def run_table(self, table_config: TableConfig) -> TableMaintenanceResult:
        """Run all configured maintenance operations on a single table."""
        result = TableMaintenanceResult(
            table_name=table_config.name,
            table_path=table_config.path,
        )
        start = time.time()

        try:
            # Health check before
            result.health_before = self._analyzer.analyze(table_config)

            # OPTIMIZE
            if table_config.optimize:
                result.optimize_result = run_optimize(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            # Z-ORDER (runs as part of OPTIMIZE, but tracked separately)
            if table_config.zorder_columns and not table_config.optimize:
                result.zorder_result = run_zorder(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            # VACUUM
            if table_config.vacuum:
                result.vacuum_result = run_vacuum(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            # Health check after
            if not self._config.dry_run:
                result.health_after = self._analyzer.analyze(table_config)

        except Exception as e:
            result.error = str(e)

        result.duration_seconds = round(time.time() - start, 2)
        return result

    def analyze_all(self) -> list[TableHealth]:
        """Run health analysis on all enabled tables without performing maintenance."""
        return [
            self._analyzer.analyze(table_config)
            for table_config in self._config.enabled_tables
        ]
