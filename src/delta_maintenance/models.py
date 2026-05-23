"""Pure-Python data models for maintenance results — no PySpark dependency."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any


@dataclass
class TableHealth:
    """Health report for a single Delta table."""

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


@dataclass(frozen=True)
class OptimizeResult:
    """Result of an OPTIMIZE operation."""

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


@dataclass(frozen=True)
class VacuumResult:
    """Result of a VACUUM operation."""

    table_name: str
    retention_hours: int = 168
    files_deleted: int = 0
    space_freed_mb: float = 0.0
    duration_seconds: float = 0.0
    skipped: bool = False


@dataclass(frozen=True)
class ZOrderResult:
    """Result of a Z-ORDER operation."""

    table_name: str
    columns: list[str] = field(default_factory=list)
    files_rewritten: int = 0
    duration_seconds: float = 0.0
    skipped: bool = False


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
            "completed_at": (
                self.completed_at.isoformat() if self.completed_at else None
            ),
        }
