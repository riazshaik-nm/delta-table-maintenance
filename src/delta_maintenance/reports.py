"""Rich console reports for Delta table maintenance."""

from __future__ import annotations

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from delta_maintenance.analyzer import TableHealth
from delta_maintenance.runner import MaintenanceReport, TableMaintenanceResult


def print_health_report(health_list: list[TableHealth], title: str = "Delta Table Health") -> None:
    """Print a health summary for multiple Delta tables."""
    console = Console()

    table = Table(title=title, show_lines=True)
    table.add_column("Table", style="cyan")
    table.add_column("Files", justify="right")
    table.add_column("Size (MB)", justify="right")
    table.add_column("Avg File (MB)", justify="right")
    table.add_column("Small Files", justify="right")
    table.add_column("Score", justify="center")
    table.add_column("Status", justify="center")

    for h in health_list:
        score = h.health_score
        if score >= 80:
            score_style = "green"
            status = "Healthy"
        elif score >= 50:
            score_style = "yellow"
            status = "Warning"
        else:
            score_style = "red"
            status = "Critical"

        table.add_row(
            h.table_name,
            str(h.total_files),
            f"{h.total_size_mb:,.1f}",
            f"{h.avg_file_size_mb:.1f}",
            f"{h.small_file_count} ({h.small_file_pct:.0f}%)",
            Text(f"{score}/100", style=score_style),
            Text(status, style=score_style),
        )

    console.print(table)


def print_maintenance_report(report: MaintenanceReport) -> None:
    """Print a maintenance run report."""
    console = Console()

    # Summary panel
    summary = Table.grid(padding=(0, 2))
    summary.add_column(style="bold cyan", justify="right")
    summary.add_column()
    summary.add_row("Tables processed", str(report.tables_processed))
    summary.add_row("Succeeded", f"[green]{report.tables_succeeded}[/green]")
    summary.add_row("Failed", f"[red]{report.tables_failed}[/red]")
    summary.add_row("Duration", f"{report.total_duration_seconds:.1f}s")

    console.print(Panel(summary, title="Maintenance Run Summary", border_style="green"))

    # Per-table details
    table = Table(title="Table Details", show_lines=True)
    table.add_column("Table", style="cyan")
    table.add_column("OPTIMIZE", justify="center")
    table.add_column("VACUUM", justify="center")
    table.add_column("Duration", justify="right")
    table.add_column("Status", justify="center")

    for r in report.results:
        opt_status = "-"
        if r.optimize_result:
            if r.optimize_result.skipped:
                opt_status = "[dim]dry run[/dim]"
            else:
                opt_status = f"[green]{r.optimize_result.files_compacted} compacted[/green]"

        vac_status = "-"
        if r.vacuum_result:
            if r.vacuum_result.skipped:
                vac_status = "[dim]dry run[/dim]"
            else:
                vac_status = f"[green]{r.vacuum_result.space_freed_mb:.1f} MB freed[/green]"

        status = "[green]OK[/green]" if r.success else f"[red]{r.error}[/red]"

        table.add_row(
            r.table_name,
            opt_status,
            vac_status,
            f"{r.duration_seconds:.1f}s",
            status,
        )

    console.print(table)
