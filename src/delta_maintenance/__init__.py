"""Automated Delta Lake table maintenance — OPTIMIZE, VACUUM, Z-ORDER, and health checks."""

from __future__ import annotations

from delta_maintenance.config import MaintenanceConfig, TableConfig
from delta_maintenance.analyzer import TableAnalyzer, TableHealth
from delta_maintenance.optimizer import run_optimize
from delta_maintenance.vacuum import run_vacuum
from delta_maintenance.zordering import run_zorder
from delta_maintenance.runner import MaintenanceRunner

__all__ = [
    "MaintenanceConfig",
    "TableConfig",
    "TableAnalyzer",
    "TableHealth",
    "run_optimize",
    "run_vacuum",
    "run_zorder",
    "MaintenanceRunner",
]

__version__ = "0.1.0"
