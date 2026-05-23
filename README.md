# delta-table-maintenance

[![CI](https://github.com/riazshaik-nm/delta-table-maintenance/actions/workflows/ci.yml/badge.svg)](https://github.com/riazshaik-nm/delta-table-maintenance/actions)
[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

**Automated Delta Lake table maintenance for Databricks — OPTIMIZE, VACUUM, Z-ORDER, and health monitoring.**

Every production Delta Lake deployment needs regular maintenance. This toolkit automates the boring parts: compacting small files, cleaning up stale data, Z-ordering for query performance, and monitoring table health — all driven by a simple YAML config.

## Features

- **OPTIMIZE** — compact small files into target-sized chunks
- **VACUUM** — remove stale files outside retention to free storage
- **Z-ORDER** — co-locate data for faster query performance
- **Health Analyzer** — score tables 0-100 based on file distribution, small files, and staleness
- **YAML-driven config** — define maintenance rules for all tables in one file
- **Rich CLI reports** — color-coded health dashboards and maintenance summaries
- **Dry run mode** — preview what maintenance would do without changing anything
- **Granular control** — run all operations at once, or target individual tables/operations

## Installation

```bash
pip install delta-table-maintenance
```

Or from source:

```bash
git clone https://github.com/riazshaik-nm/delta-table-maintenance.git
cd delta-table-maintenance
pip install -e .
```

## Quick Start

### 1. Define your maintenance config

```yaml
# maintenance.yml
dry_run: false
default_vacuum_retention_hours: 168

tables:
  - path: /mnt/delta/fact_orders
    name: fact_orders
    zorder_columns: [order_date, customer_id]
    vacuum_retention_hours: 168

  - path: /mnt/delta/dim_customers
    name: dim_customers
    zorder_columns: [customer_id, region]
    vacuum_retention_hours: 720
```

### 2. Run maintenance

```python
from delta_maintenance import MaintenanceConfig, MaintenanceRunner
from delta_maintenance.reports import print_maintenance_report

config = MaintenanceConfig.from_yaml("maintenance.yml")
runner = MaintenanceRunner(spark, config)

report = runner.run_all()
print_maintenance_report(report)
```

## Table Health Analysis

Before running maintenance, analyze your tables to understand their current state:

```python
from delta_maintenance import MaintenanceRunner, MaintenanceConfig
from delta_maintenance.reports import print_health_report

runner = MaintenanceRunner(spark, config)
health = runner.analyze_all()
print_health_report(health)
```

Output:

```
                Delta Table Health
┌──────────────┬───────┬──────────┬───────────┬─────────────┬───────┬──────────┐
│ Table        │ Files │ Size(MB) │ Avg(MB)   │ Small Files │ Score │ Status   │
├──────────────┼───────┼──────────┼───────────┼─────────────┼───────┼──────────┤
│ fact_orders  │  2847 │ 45,200.0 │     15.9  │ 1423 (50%)  │ 45    │ Critical │
│ dim_customer │    42 │    320.0 │      7.6  │   28 (67%)  │ 30    │ Critical │
│ stg_events   │   150 │  8,400.0 │     56.0  │   12 (8%)   │ 100   │ Healthy  │
└──────────────┴───────┴──────────┴───────────┴─────────────┴───────┴──────────┘
```

### Health Score Breakdown

| Score     | Status   | Meaning                                    |
|-----------|----------|--------------------------------------------|
| 80 - 100  | Healthy  | Table is well-maintained                   |
| 50 - 79   | Warning  | Some small files or stale data present      |
| 0 - 49    | Critical | Significant small-file problem, needs OPTIMIZE |

## Individual Operations

Run specific operations when you need fine-grained control:

```python
from delta_maintenance import TableConfig, run_optimize, run_vacuum, run_zorder

table = TableConfig(
    path="/delta/fact_orders",
    zorder_columns=["order_date", "customer_id"],
)

# OPTIMIZE only
result = run_optimize(spark, table)
print(f"Compacted {result.files_compacted} files")

# VACUUM with custom retention
result = run_vacuum(spark, table, retention_hours=72)
print(f"Freed {result.space_freed_mb} MB")

# Z-ORDER specific columns
result = run_zorder(spark, table, columns=["order_date"])
print(f"Rewrote {result.files_rewritten} files")
```

## Configuration Reference

### TableConfig

| Parameter                | Type       | Default | Description                                |
|--------------------------|------------|---------|--------------------------------------------|
| `path`                   | str        | —       | Path to Delta table (required)             |
| `name`                   | str        | auto    | Human-readable name (defaults from path)   |
| `optimize`               | bool       | True    | Run OPTIMIZE                               |
| `vacuum`                 | bool       | True    | Run VACUUM                                 |
| `vacuum_retention_hours` | int        | 168     | VACUUM retention period (hours)            |
| `zorder_columns`         | list[str]  | []      | Columns for Z-ORDER                        |
| `target_file_size_mb`    | int        | 128     | Target file size for OPTIMIZE              |
| `small_file_threshold_mb`| float      | 10.0    | Threshold for small file detection         |
| `schedule`               | str        | daily   | Maintenance frequency hint                 |
| `enabled`                | bool       | True    | Whether maintenance is active              |

### MaintenanceConfig

| Parameter                      | Type  | Default | Description                    |
|--------------------------------|-------|---------|--------------------------------|
| `tables`                       | list  | []      | List of TableConfig entries    |
| `default_vacuum_retention_hours`| int  | 168     | Default VACUUM retention       |
| `dry_run`                      | bool  | False   | Preview mode (no changes)      |
| `parallel`                     | bool  | False   | Parallel execution (future)    |
| `log_path`                     | str   | None    | Path for maintenance logs      |

## Z-ORDER Best Practices

- Pick **1-4 columns** used most in WHERE clauses
- High cardinality columns benefit most (user_id, timestamp)
- Put the most-filtered column first
- Don't Z-ORDER on partition columns (already segregated)
- Re-run Z-ORDER after significant data changes

## Examples

See the [`examples/`](examples/) directory:

- **[basic_maintenance.py](examples/basic_maintenance.py)** — Full workflow: config, analyze, maintain, report
- **[targeted_operations.py](examples/targeted_operations.py)** — Run individual operations on specific tables
- **[maintenance.yml](examples/maintenance.yml)** — Sample YAML configuration

## Development

```bash
git clone https://github.com/riazshaik-nm/delta-table-maintenance.git
cd delta-table-maintenance
pip install -e ".[dev]"
pytest -v
```

## License

MIT License — see [LICENSE](LICENSE) for details.
