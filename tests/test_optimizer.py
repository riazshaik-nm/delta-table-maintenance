from delta_maintenance.models import OptimizeResult


def test_optimize_result_files_compacted():
    r = OptimizeResult(
        table_name="test",
        files_before=1000,
        files_after=50,
        files_removed=950,
    )
    assert r.files_compacted == 950


def test_optimize_result_skipped():
    r = OptimizeResult(table_name="test", skipped=True)
    assert r.skipped is True
    assert r.files_compacted == 0


def test_optimize_result_no_change():
    r = OptimizeResult(
        table_name="test",
        files_before=10,
        files_after=10,
    )
    assert r.files_compacted == 0
