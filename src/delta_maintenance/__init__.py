"""Automated Delta Lake table maintenance — OPTIMIZE, VACUUM, Z-ORDER, and health checks."""

from __future__ import annotations

from delta_maintenance.config import MaintenanceConfig, TableConfig

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


def __getattr__(name: str) -> object:
    """Lazy imports for modules that require pyspark/delta at runtime."""
    if name == "TableAnalyzer":
        from delta_maintenance.analyzer import TableAnalyzer

        return TableAnalyzer
    if name == "TableHealth":
        from delta_maintenance.analyzer import TableHealth

        return TableHealth
    if name == "run_optimize":
        from delta_maintenance.optimizer import run_optimize

        return run_optimize
    if name == "run_vacuum":
        from delta_maintenance.vacuum import run_vacuum

        return run_vacuum
    if name == "run_zorder":
        from delta_maintenance.zordering import run_zorder

        return run_zorder
    if name == "MaintenanceRunner":
        from delta_maintenance.runner import MaintenanceRunner

        return MaintenanceRunner
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
