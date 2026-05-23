import tempfile

from delta_maintenance.config import MaintenanceConfig, TableConfig


def test_table_config_defaults():
    t = TableConfig(path="/delta/customers")
    assert t.name == "customers"
    assert t.optimize is True
    assert t.vacuum is True
    assert t.vacuum_retention_hours == 168
    assert t.zorder_columns == []
    assert t.target_file_size_mb == 128
    assert t.enabled is True


def test_table_config_custom():
    t = TableConfig(
        path="/delta/orders",
        name="orders_table",
        zorder_columns=["order_date", "customer_id"],
        vacuum_retention_hours=720,
        small_file_threshold_mb=5.0,
    )
    assert t.name == "orders_table"
    assert t.zorder_columns == ["order_date", "customer_id"]
    assert t.vacuum_retention_hours == 720


def test_table_config_auto_name():
    t = TableConfig(path="/mnt/warehouse/fact_sales/")
    assert t.name == "fact_sales"


def test_maintenance_config_enabled_tables():
    config = MaintenanceConfig(
        tables=[
            TableConfig(path="/delta/a", enabled=True),
            TableConfig(path="/delta/b", enabled=False),
            TableConfig(path="/delta/c", enabled=True),
        ]
    )
    assert len(config.enabled_tables) == 2


def test_config_from_dict():
    data = {
        "dry_run": True,
        "default_vacuum_retention_hours": 336,
        "tables": [
            {"path": "/delta/users", "name": "users", "zorder_columns": ["user_id"]},
            {"path": "/delta/events", "vacuum_retention_hours": 48},
        ],
    }
    config = MaintenanceConfig.from_dict(data)
    assert config.dry_run is True
    assert config.default_vacuum_retention_hours == 336
    assert len(config.tables) == 2
    assert config.tables[0].zorder_columns == ["user_id"]
    assert config.tables[1].vacuum_retention_hours == 48


def test_config_from_yaml():
    yaml_content = """
dry_run: false
default_vacuum_retention_hours: 168
tables:
  - path: /delta/customers
    name: customers
    zorder_columns:
      - customer_id
      - region
  - path: /delta/orders
    name: orders
    vacuum_retention_hours: 720
"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        f.write(yaml_content)
        f.flush()
        config = MaintenanceConfig.from_yaml(f.name)

    assert len(config.tables) == 2
    assert config.tables[0].name == "customers"
    assert config.tables[0].zorder_columns == ["customer_id", "region"]
    assert config.tables[1].vacuum_retention_hours == 720


def test_config_empty_tables():
    config = MaintenanceConfig()
    assert config.enabled_tables == []
    assert config.dry_run is False
