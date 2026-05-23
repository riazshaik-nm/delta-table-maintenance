"""Orchestrates maintenance operations across multiple Delta tables."""

from __future__ import annotations

import time
from datetime import datetime

from pyspark.sql import SparkSession

from delta_maintenance.analyzer import TableAnalyzer
from delta_maintenance.config import MaintenanceConfig, TableConfig
from delta_maintenance.models import (
    MaintenanceReport,
    TableHealth,
    TableMaintenanceResult,
)
from delta_maintenance.optimizer import run_optimize
from delta_maintenance.vacuum import run_vacuum
from delta_maintenance.zordering import run_zorder


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

    def run_table(
        self, table_config: TableConfig
    ) -> TableMaintenanceResult:
        """Run all configured maintenance operations on a single table."""
        result = TableMaintenanceResult(
            table_name=table_config.name,
            table_path=table_config.path,
        )
        start = time.time()

        try:
            result.health_before = self._analyzer.analyze(table_config)

            if table_config.optimize:
                result.optimize_result = run_optimize(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            if table_config.zorder_columns and not table_config.optimize:
                result.zorder_result = run_zorder(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            if table_config.vacuum:
                result.vacuum_result = run_vacuum(
                    self._spark, table_config, dry_run=self._config.dry_run
                )

            if not self._config.dry_run:
                result.health_after = self._analyzer.analyze(table_config)

        except Exception as e:
            result.error = str(e)

        result.duration_seconds = round(time.time() - start, 2)
        return result

    def analyze_all(self) -> list[TableHealth]:
        """Run health analysis on all enabled tables."""
        return [
            self._analyzer.analyze(table_config)
            for table_config in self._config.enabled_tables
        ]
