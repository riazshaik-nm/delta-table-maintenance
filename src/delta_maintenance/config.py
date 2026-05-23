"""Configuration models for Delta table maintenance."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass
class TableConfig:
    """Maintenance configuration for a single Delta table.

    Parameters
    ----------
    path : str
        Path to the Delta table (e.g. "dbfs:/mnt/warehouse/customers").
    name : str
        Human-readable table name for reporting.
    optimize : bool
        Whether to run OPTIMIZE on this table.
    vacuum : bool
        Whether to run VACUUM on this table.
    vacuum_retention_hours : int
        Retention period for VACUUM in hours (default 168 = 7 days).
    zorder_columns : list[str]
        Columns to Z-ORDER by during OPTIMIZE. Empty = no Z-ORDER.
    target_file_size_mb : int
        Target file size in MB for OPTIMIZE (default 128).
    small_file_threshold_mb : float
        Files below this size are flagged as small files (default 10).
    schedule : str
        Cron expression or frequency hint (e.g. "daily", "weekly").
    enabled : bool
        Whether this table's maintenance is active.

    Examples
    --------
    >>> table = TableConfig(
    ...     path="/delta/customers",
    ...     name="customers",
    ...     zorder_columns=["customer_id", "region"],
    ... )
    """

    path: str
    name: str = ""
    optimize: bool = True
    vacuum: bool = True
    vacuum_retention_hours: int = 168
    zorder_columns: list[str] = field(default_factory=list)
    target_file_size_mb: int = 128
    small_file_threshold_mb: float = 10.0
    schedule: str = "daily"
    enabled: bool = True

    def __post_init__(self) -> None:
        if not self.name:
            self.name = self.path.rstrip("/").split("/")[-1]


@dataclass
class MaintenanceConfig:
    """Top-level configuration for maintaining multiple Delta tables.

    Parameters
    ----------
    tables : list[TableConfig]
        List of table configurations.
    default_vacuum_retention_hours : int
        Default retention for tables that don't specify their own.
    dry_run : bool
        If True, analyze tables but don't execute any maintenance.
    parallel : bool
        If True, process tables in parallel (Databricks only).
    log_path : str | None
        Path to write maintenance logs.

    Examples
    --------
    >>> config = MaintenanceConfig.from_yaml("maintenance.yml")
    >>> runner = MaintenanceRunner(spark, config)
    >>> runner.run_all()
    """

    tables: list[TableConfig] = field(default_factory=list)
    default_vacuum_retention_hours: int = 168
    dry_run: bool = False
    parallel: bool = False
    log_path: str | None = None

    @classmethod
    def from_yaml(cls, path: str | Path) -> MaintenanceConfig:
        """Load configuration from a YAML file.

        Example YAML:

        .. code-block:: yaml

            dry_run: false
            default_vacuum_retention_hours: 168
            tables:
              - path: /delta/customers
                name: customers
                zorder_columns: [customer_id, region]
              - path: /delta/orders
                name: orders
                vacuum_retention_hours: 720
                zorder_columns: [order_date, customer_id]
        """
        raw = yaml.safe_load(Path(path).read_text())
        return cls._from_dict(raw)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> MaintenanceConfig:
        return cls._from_dict(data)

    @classmethod
    def _from_dict(cls, data: dict[str, Any]) -> MaintenanceConfig:
        tables = [TableConfig(**t) for t in data.get("tables", [])]
        return cls(
            tables=tables,
            default_vacuum_retention_hours=data.get("default_vacuum_retention_hours", 168),
            dry_run=data.get("dry_run", False),
            parallel=data.get("parallel", False),
            log_path=data.get("log_path"),
        )

    @property
    def enabled_tables(self) -> list[TableConfig]:
        return [t for t in self.tables if t.enabled]
