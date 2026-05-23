"""CLI for Delta table maintenance operations."""

from __future__ import annotations

import argparse

from rich.console import Console


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="delta-maintain",
        description="Automated Delta Lake table maintenance",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Analyze table health")
    analyze_parser.add_argument("--config", required=True, help="Path to YAML config")

    # run command
    run_parser = subparsers.add_parser("run", help="Run maintenance operations")
    run_parser.add_argument("--config", required=True, help="Path to YAML config")
    run_parser.add_argument("--dry-run", action="store_true", help="Preview without executing")

    args = parser.parse_args(argv)
    console = Console()

    if args.command == "analyze":
        console.print("[cyan]Loading config...[/cyan]")
        from delta_maintenance.config import MaintenanceConfig

        config = MaintenanceConfig.from_yaml(args.config)
        console.print(f"[green]Loaded {len(config.enabled_tables)} tables from config[/green]")
        console.print("[dim]Run with a Spark session to analyze tables[/dim]")

    elif args.command == "run":
        console.print("[cyan]Loading config...[/cyan]")
        from delta_maintenance.config import MaintenanceConfig

        config = MaintenanceConfig.from_yaml(args.config)
        if args.dry_run:
            config.dry_run = True
        console.print(
            f"[green]Loaded {len(config.enabled_tables)} tables "
            f"(dry_run={config.dry_run})[/green]"
        )
        console.print("[dim]Run with a Spark session to execute maintenance[/dim]")
