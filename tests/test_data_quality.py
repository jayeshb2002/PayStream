
from src.orchestration.data_quality import run_data_quality_checks


class FakeCursor:
    def __init__(self, results):
        self.results = iter(results)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query):
        self.query = query

    def fetchone(self):
        return next(self.results)


class FakeConnection:
    def __init__(self, results):
        self.results = results

    def cursor(self):
        return FakeCursor(self.results)


def test_all_quality_checks_pass():
    connection = FakeConnection([(100,)] + [(0,)] * 13)

    report = run_data_quality_checks(connection)

    assert report["total_records"] == 100
    assert report["total_checks"] == 13
    assert report["passed_checks"] == 13
    assert report["failed_checks_count"] == 0
    assert report["status"] == "PASSED"


def test_quality_check_detects_invalid_amounts():
    results = [(100,)] + [(0,)] * 5 + [(3,)] + [(0,)] * 7
    connection = FakeConnection(results)

    report = run_data_quality_checks(connection)

    assert report["status"] == "FAILED"
    assert report["failed_checks"]["invalid_amounts"] == 3
    assert report["failed_checks_count"] == 1
