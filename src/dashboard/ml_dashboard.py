
"""Streamlit dashboard components for ML-based transaction anomaly detection."""

from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st


def get_connection():
    """Create a connection to the PayStream PostgreSQL database."""
    return psycopg2.connect(
        host=os.getenv("PAYSTREAM_DB_HOST", "localhost"),
        port=int(os.getenv("PAYSTREAM_DB_PORT", "5433")),
        dbname=os.getenv("PAYSTREAM_DB_NAME", "paystream"),
        user=os.getenv("PAYSTREAM_DB_USER", "paystream"),
        password=os.getenv("PAYSTREAM_DB_PASSWORD", "password"),
        connect_timeout=10,
    )


def load_ml_predictions() -> pd.DataFrame:
    """Load the latest ML prediction for each transaction."""
    query = """
        SELECT *
        FROM latest_ml_predictions
        ORDER BY predicted_at DESC
    """

    with get_connection() as connection:
        return pd.read_sql_query(query, connection)


def load_ml_runs() -> pd.DataFrame:
    """Load recent ML prediction runs."""
    connection = get_connection()
    try:
        return pd.read_sql_query(
            """
            SELECT
                run_id,
                model_name,
                threshold,
                percentile,
                total_transactions,
                anomaly_count,
                normal_count,
                anomaly_rate_percent AS anomaly_rate,
                duration_ms,
                completed_at
            FROM ml_prediction_runs
            WHERE status = 'COMPLETED'
            ORDER BY completed_at DESC
            LIMIT 30
            """,
            connection,
        )
    finally:
        connection.close()


def load_integrated_risk_results() -> pd.DataFrame:
    """Load latest integrated statistical and ML risk results."""
    query = """
        SELECT *
        FROM latest_integrated_risk_results
    """

    with get_connection() as connection:
        return pd.read_sql_query(query, connection)


def find_column(
    dataframe: pd.DataFrame,
    candidates: list[str],
) -> str | None:
    """Return the first candidate column present in a dataframe."""
    for column in candidates:
        if column in dataframe.columns:
            return column
    return None


def render_ml_dashboard() -> None:
    """Render the ML anomaly detection dashboard."""
    st.header("Machine Learning Fraud Detection")
    st.caption(
        "Isolation Forest predictions and integrated transaction risk indicators"
    )

    try:
        predictions = load_ml_predictions()
        runs = load_ml_runs()
        integrated = load_integrated_risk_results()
    except Exception as exc:
        st.error(f"Unable to load ML dashboard data: {exc}")
        st.info(
            "Check PostgreSQL connectivity and confirm that Milestone 6 "
            "prediction and integration tables have been created."
        )
        return

    if runs.empty and predictions.empty:
        st.info(
            "No ML predictions are available yet. Run the Milestone 6 ML pipeline "
            "from Airflow to populate this dashboard."
        )
        return

    # Resolve common column names from the latest-predictions view.
    anomaly_column = find_column(
        predictions,
        ["is_anomaly", "ml_is_anomaly"],
    )
    score_column = find_column(
        predictions,
        ["anomaly_score", "ml_anomaly_score"],
    )
    transaction_column = find_column(
        predictions,
        ["transaction_id"],
    )
    threshold_column = find_column(
        predictions,
        ["threshold", "anomaly_threshold"],
    )
    prediction_column = find_column(
        predictions,
        ["prediction", "ml_prediction"],
    )

    # Use the latest completed run for the KPI summary when available.
    latest_run = runs.iloc[0] if not runs.empty else None

    total = (
        int(latest_run["total_transactions"])
        if latest_run is not None
        else len(predictions)
    )

    anomalies = (
        int(latest_run["anomaly_count"])
        if latest_run is not None
        else (
            int(predictions[anomaly_column].fillna(False).astype(bool).sum())
            if anomaly_column
            else 0
        )
    )

    normal = (
        int(latest_run["normal_count"])
        if latest_run is not None
        else max(total - anomalies, 0)
    )

    anomaly_rate = (
        float(latest_run["anomaly_rate"])
        if latest_run is not None
        else (anomalies / total * 100 if total else 0.0)
    )

    threshold = (
        float(latest_run["threshold"])
        if latest_run is not None
        else (
            float(predictions[threshold_column].dropna().iloc[0])
            if threshold_column and not predictions[threshold_column].dropna().empty
            else None
        )
    )

    st.subheader("ML Prediction Summary")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("Transactions Analysed", f"{total:,}")
    col2.metric("Anomalies Detected", f"{anomalies:,}")
    col3.metric("Normal Transactions", f"{normal:,}")
    col4.metric("Anomaly Rate", f"{anomaly_rate:.2f}%")

    if threshold is not None:
        st.caption(f"Configured anomaly threshold: {threshold:.6f}")

    st.divider()

    # Prediction distribution and score visualization.
    st.subheader("Anomaly Detection Analysis")

    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        if total > 0:
            distribution = pd.DataFrame(
                {
                    "Prediction": ["Anomaly", "Normal"],
                    "Transactions": [anomalies, normal],
                }
            )

            figure = px.pie(
                distribution,
                names="Prediction",
                values="Transactions",
                hole=0.45,
                title="Prediction Distribution",
            )
            st.plotly_chart(figure, use_container_width=True)
        else:
            st.info("No prediction distribution is available.")

    with chart_col2:
        if score_column and not predictions.empty:
            score_data = predictions[[score_column]].copy()
            score_data[score_column] = pd.to_numeric(
                score_data[score_column],
                errors="coerce",
            )
            score_data = score_data.dropna()

            if not score_data.empty:
                figure = px.histogram(
                    score_data,
                    x=score_column,
                    nbins=30,
                    title="ML Anomaly Score Distribution",
                    labels={score_column: "Anomaly Score"},
                )

                if threshold is not None:
                    figure.add_vline(
                        x=threshold,
                        line_dash="dash",
                        annotation_text="Threshold",
                    )

                st.plotly_chart(figure, use_container_width=True)
            else:
                st.info("No valid anomaly scores are available.")
        else:
            st.info("Anomaly score data is not available.")

    st.divider()

    # Transactions flagged by the latest prediction view.
    st.subheader("Highest ML Anomaly Scores")

    if not predictions.empty and score_column:
        anomaly_data = predictions.copy()
        anomaly_data[score_column] = pd.to_numeric(
            anomaly_data[score_column],
            errors="coerce",
        )

        if anomaly_column:
            anomaly_data = anomaly_data[
                anomaly_data[anomaly_column].fillna(False).astype(bool)
            ]

        anomaly_data = (
            anomaly_data.dropna(subset=[score_column])
            .sort_values(score_column, ascending=False)
            .head(10)
        )

        display_columns = [
            column
            for column in [
                transaction_column,
                score_column,
                anomaly_column,
                prediction_column,
                threshold_column,
                "model_name",
                "percentile",
                "predicted_at",
            ]
            if column and column in anomaly_data.columns
        ]

        if not anomaly_data.empty:
            st.dataframe(
                anomaly_data[display_columns],
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No anomalous transactions were found in the latest results.")
    else:
        st.info("Transaction-level ML prediction details are unavailable.")

    st.divider()

    # Compare statistical risk with integrated risk.
    st.subheader("Statistical Risk vs ML-Integrated Risk")

    if not integrated.empty:
        statistical_score_column = find_column(
            integrated,
            ["statistical_risk_score", "risk_score"],
        )
        integrated_score_column = find_column(
            integrated,
            ["integrated_risk_score"],
        )
        integrated_category_column = find_column(
            integrated,
            ["integrated_risk_category"],
        )

        if statistical_score_column and integrated_score_column:
            comparison = integrated[
                [statistical_score_column, integrated_score_column]
            ].copy()

            comparison[statistical_score_column] = pd.to_numeric(
                comparison[statistical_score_column],
                errors="coerce",
            )
            comparison[integrated_score_column] = pd.to_numeric(
                comparison[integrated_score_column],
                errors="coerce",
            )
            comparison = comparison.dropna()

            if not comparison.empty:
                figure = px.scatter(
                    comparison,
                    x=statistical_score_column,
                    y=integrated_score_column,
                    title="Risk Score Comparison",
                    labels={
                        statistical_score_column: "Statistical Risk Score",
                        integrated_score_column: "Integrated Risk Score",
                    },
                    opacity=0.65,
                )
                figure.add_trace(
                    go.Scatter(
                        x=[0, 100],
                        y=[0, 100],
                        mode="lines",
                        name="No score change",
                        line={"dash": "dash"},
                    )
                )
                figure.update_xaxes(range=[0, 100])
                figure.update_yaxes(range=[0, 100])
                st.plotly_chart(figure, use_container_width=True)
            else:
                st.info("No valid risk-score comparison data is available.")

        if integrated_category_column:
            st.markdown("**Integrated Risk Category Distribution**")
            category_counts = (
                integrated[integrated_category_column]
                .fillna("UNKNOWN")
                .value_counts()
                .rename_axis("Risk Category")
                .reset_index(name="Transactions")
            )

            figure = px.bar(
                category_counts,
                x="Risk Category",
                y="Transactions",
                title="Integrated Risk Categories",
            )
            st.plotly_chart(figure, use_container_width=True)

        st.markdown("**Integrated Risk Results**")
        st.dataframe(
            integrated.head(20),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info(
            "No integrated risk results are available. Run the ML risk "
            "integration task in the Airflow pipeline."
        )

    st.divider()

    # Recent model execution history.
    st.subheader("ML Pipeline Run History")

    if not runs.empty:
        history_columns = [
            column
            for column in [
                "completed_at",
                "model_name",
                "total_transactions",
                "anomaly_count",
                "normal_count",
                "anomaly_rate",
                "threshold",
                "duration_ms",
            ]
            if column in runs.columns
        ]

        st.dataframe(
            runs[history_columns],
            use_container_width=True,
            hide_index=True,
        )

    # Transaction-level inspection.
    st.subheader("Transaction ML Details")

    if not predictions.empty and transaction_column:
        transaction_ids = (
            predictions[transaction_column]
            .dropna()
            .astype(str)
            .drop_duplicates()
            .tolist()
        )

        if transaction_ids:
            selected_transaction = st.selectbox(
                "Select a transaction",
                transaction_ids,
                key="ml_dashboard_transaction",
            )

            selected_rows = predictions[
                predictions[transaction_column].astype(str)
                == selected_transaction
            ]

            if not selected_rows.empty:
                st.json(selected_rows.iloc[0].to_dict())
