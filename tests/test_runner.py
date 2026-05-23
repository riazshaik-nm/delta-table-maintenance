from datetime import datetime

from delta_maintenance.models import (
    MaintenanceReport,
    TableMaintenanceResult,
)


def test_report_summary():
    report = MaintenanceReport(
        results=[
            TableMaintenanceResult(table_name="a", table_path="/a"),
            TableMaintenanceResult(table_name="b", table_path="/b", error="failed"),
            TableMaintenanceResult(table_name="c", table_path="/c"),
        ],
        total_duration_seconds=45.2,
    )
    assert report.tables_processed == 3
    assert report.tables_succeeded == 2
    assert report.tables_failed == 1


def test_result_success():
    r = TableMaintenanceResult(table_name="test", table_path="/test")
    assert r.success is True


def test_result_failure():
    r = TableMaintenanceResult(table_name="test", table_path="/test", error="boom")
    assert r.success is False


def test_report_summary_dict():
    report = MaintenanceReport(
        results=[
            TableMaintenanceResult(table_name="x", table_path="/x"),
        ],
        total_duration_seconds=10.0,
        completed_at=datetime(2026, 1, 1),
    )
    s = report.summary()
    assert s["tables_processed"] == 1
    assert s["tables_succeeded"] == 1
    assert s["tables_failed"] == 0
