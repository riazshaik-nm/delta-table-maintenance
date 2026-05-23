from delta_maintenance.analyzer import TableHealth


def test_health_score_perfect():
    h = TableHealth(
        table_name="test",
        table_path="/test",
        total_files=100,
        small_file_count=5,
        small_file_pct=5.0,
        needs_optimize=False,
        needs_vacuum=False,
    )
    assert h.health_score == 100
    assert h.is_healthy is True


def test_health_score_many_small_files():
    h = TableHealth(
        table_name="test",
        table_path="/test",
        total_files=1000,
        small_file_count=600,
        small_file_pct=60.0,
        needs_optimize=True,
        needs_vacuum=False,
    )
    assert h.health_score <= 50
    assert h.is_healthy is False


def test_health_score_needs_vacuum():
    h = TableHealth(
        table_name="test",
        table_path="/test",
        total_files=50,
        small_file_count=5,
        small_file_pct=10.0,
        needs_optimize=False,
        needs_vacuum=True,
    )
    assert h.health_score == 85  # -15 for vacuum


def test_health_score_moderate_small_files():
    h = TableHealth(
        table_name="test",
        table_path="/test",
        total_files=200,
        small_file_count=60,
        small_file_pct=30.0,
        needs_optimize=True,
        needs_vacuum=True,
    )
    assert h.health_score == 65  # -20 for small files, -15 for vacuum
