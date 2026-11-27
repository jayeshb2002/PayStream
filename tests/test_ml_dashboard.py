from unittest.mock import MagicMock

import pandas as pd

from src.dashboard import ml_dashboard


def test_load_ml_runs_aliases_stored_anomaly_rate(monkeypatch):
    connection = MagicMock()
    read_sql_query = MagicMock(
        return_value=pd.DataFrame(columns=["anomaly_rate"])
    )

    monkeypatch.setattr(
        ml_dashboard,
        "get_connection",
        lambda: connection,
    )
    monkeypatch.setattr(
        ml_dashboard.pd,
        "read_sql_query",
        read_sql_query,
    )

    runs = ml_dashboard.load_ml_runs()

    query = read_sql_query.call_args.args[0]
    assert "anomaly_rate_percent AS anomaly_rate" in query
    assert "anomaly_rate" in runs.columns
    connection.close.assert_called_once()
