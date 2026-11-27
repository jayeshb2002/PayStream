
import logging
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from src.dashboard.ml_dashboard import render_ml_dashboard

from src.dashboard.queries import (
    get_dashboard_summary,
    get_processing_trends,
    get_quality_trends,
    get_latest_quality_checks,
    get_failure_summary,
    get_recent_failures,
    get_live_component_health
)

import json
import pandas as pd
import plotly.express as px
import streamlit as st

from src.dashboard.queries import (
    get_risk_dashboard_summary,
    get_risk_category_distribution,
    get_highest_risk_transactions,
    get_transaction_risk_details,
    get_risk_analysis_trends,
)

from src.dashboard.ml_dashboard import render_ml_dashboard

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


st.set_page_config(
    page_title="PayStream Monitoring",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_data(ttl=30)
def load_summary():
    return get_dashboard_summary()


@st.cache_data(ttl=30)
def load_processing_trends(hours):
    return get_processing_trends(hours)


@st.cache_data(ttl=30)
def load_quality_trends(days):
    return get_quality_trends(days)


@st.cache_data(ttl=30)
def load_quality_checks():
    return get_latest_quality_checks()


@st.cache_data(ttl=30)
def load_failure_summary():
    return get_failure_summary()


@st.cache_data(ttl=30)
def load_recent_failures():
    return get_recent_failures()


def display_metric(label, value, help_text=None):
    if value is None or pd.isna(value):
        value = "N/A"
    elif isinstance(value, float):
        value = f"{value:,.2f}"
    elif isinstance(value, int):
        value = f"{value:,}"

    st.metric(
        label=label,
        value=value,
        help=help_text
    )


def render_health():
    st.subheader("Pipeline Component Health")

    health = get_live_component_health()

    columns = st.columns(len(health))

    for column, (component, result) in zip(columns, health.items()):
        with column:
            status = result["status"]

            if status == "HEALTHY":
                st.success(f"{component}: {status}")
            elif status == "NOT CONFIGURED":
                st.warning(f"{component}: {status}")
            else:
                st.error(f"{component}: {status}")

            st.caption(result["details"])


def render_overview(summary):
    st.subheader("Pipeline Overview")

    columns = st.columns(4)

    with columns[0]:
        display_metric(
            "Records Processed (24h)",
            summary.get("records_processed", 0)
        )

    with columns[1]:
        display_metric(
            "Records Rejected (24h)",
            summary.get("records_rejected", 0)
        )

    with columns[2]:
        display_metric(
            "Records Late (24h)",
            summary.get("records_late", 0)
        )

    with columns[3]:
        display_metric(
            "Records Submitted to Sink (24h)",
            summary.get("records_submitted", 0)
        )

    columns = st.columns(3)

    with columns[0]:
        display_metric(
            "Latest Data Quality Score",
            summary.get("quality_score"),
            "Percentage of validation checks that passed."
        )

    with columns[1]:
        display_metric(
            "Open Failures",
            summary.get("open_failures", 0)
        )

    with columns[2]:
        display_metric(
            "Critical Open Failures",
            summary.get("critical_failures", 0)
        )

    quality_status = summary.get("quality_status", "NO DATA")

    if quality_status == "PASSED":
        st.success("Latest data quality monitoring status: PASSED")
    elif quality_status == "WARNING":
        st.warning("Latest data quality monitoring status: WARNING")
    else:
        st.info(f"Latest data quality monitoring status: {quality_status}")


def render_processing_trends(hours):
    st.subheader("Processing Metrics")

    data = load_processing_trends(hours)

    if data.empty:
        st.info("No processing metrics available for this time range.")
        return

    metric_columns = [
        "records_processed",
        "records_rejected",
        "records_late",
        "records_submitted"
    ]

    for column in metric_columns:
        if column in data.columns:
            data[column] = data[column].fillna(0)

    chart_data = data.melt(
        id_vars=["metric_hour"],
        value_vars=metric_columns,
        var_name="Metric",
        value_name="Records"
    )

    chart_data["Metric"] = chart_data["Metric"].map({
        "records_processed": "Processed",
        "records_rejected": "Rejected",
        "records_late": "Late",
        "records_submitted": "Sink submitted"
    })

    figure = px.line(
        chart_data,
        x="metric_hour",
        y="Records",
        color="Metric",
        markers=True,
        title="Hourly Pipeline Processing Volume"
    )

    figure.update_layout(
        xaxis_title="Time",
        yaxis_title="Records",
        legend_title="Metric",
        hovermode="x unified"
    )

    st.plotly_chart(figure, use_container_width=True)

    st.dataframe(
        data,
        use_container_width=True,
        hide_index=True
    )


def render_quality_monitoring(days):
    st.subheader("Data Quality Monitoring")

    trends = load_quality_trends(days)

    if trends.empty:
        st.info("No historical data quality runs available.")
    else:
        figure = px.line(
            trends,
            x="created_at",
            y="quality_score",
            markers=True,
            title="Data Quality Score Trend"
        )

        figure.add_scatter(
            x=trends["created_at"],
            y=trends["threshold_score"],
            mode="lines",
            name="Configured Threshold",
            line={"dash": "dash"}
        )

        figure.update_layout(
            xaxis_title="Monitoring Time",
            yaxis_title="Quality Score (%)",
            yaxis_range=[0, 100]
        )

        st.plotly_chart(figure, use_container_width=True)

        st.dataframe(
            trends,
            use_container_width=True,
            hide_index=True
        )

    st.markdown("#### Latest Validation Check Results")

    checks = load_quality_checks()

    if checks.empty:
        st.info("No individual validation results available.")
    else:
        st.dataframe(
            checks,
            use_container_width=True,
            hide_index=True
        )


def render_failure_monitoring():
    st.subheader("Failure Monitoring")

    summary = load_failure_summary()

    if summary.empty:
        st.success("No open failure incidents.")
    else:
        figure = px.bar(
            summary,
            x="component",
            y="failure_count",
            color="severity",
            barmode="group",
            title="Open Failures by Component and Severity"
        )

        st.plotly_chart(
            figure,
            use_container_width=True
        )

        st.dataframe(
            summary,
            use_container_width=True,
            hide_index=True
        )

    st.markdown("#### Recent Failure Incidents")

    failures = load_recent_failures()

    if failures.empty:
        st.info("No failure incidents recorded.")
    else:
        st.dataframe(
            failures,
            use_container_width=True,
            hide_index=True
        )


# =========================================================
# Transaction Risk Analytics
# =========================================================

from src.dashboard.risk_queries import (
    get_latest_risk_run,
    get_latest_risk_scores,
    get_risk_trends,
)

st.divider()

st.header("Transaction Risk Analytics")

try:
    risk_run = get_latest_risk_run()

    if risk_run.empty:
        st.info(
            "No risk analysis has been executed yet."
        )
    else:
        latest_run = risk_run.iloc[0]

        risk_scores = get_latest_risk_scores()
        risk_trends = get_risk_trends()

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Analysed Transactions",
            int(latest_run["analysed_transactions"]),
        )

        col2.metric(
            "Anomalies",
            int(latest_run["anomaly_count"]),
        )

        col3.metric(
            "High Risk",
            int(latest_run["high_risk_count"]),
        )

        col4.metric(
            "Critical Risk",
            int(latest_run["critical_risk_count"]),
        )

        st.subheader("Risk Category Distribution")

        if not risk_scores.empty:
            distribution = (
                risk_scores["risk_category"]
                .value_counts()
                .rename_axis("Risk Category")
                .reset_index(name="Transactions")
            )

            st.bar_chart(
                distribution.set_index("Risk Category")
            )

        st.subheader("Flagged Transactions")

        if not risk_scores.empty:
            flagged = risk_scores[
                (risk_scores["is_anomaly"] == True)
                | (risk_scores["risk_category"].isin(
                    ["HIGH", "CRITICAL"]
                ))
            ].sort_values(
                "risk_score",
                ascending=False,
            )

            st.dataframe(
                flagged,
                use_container_width=True,
                hide_index=True,
            )

        st.subheader("Risk Analysis Trends")

        if not risk_trends.empty:
            st.line_chart(
                risk_trends.set_index("analysis_date")[
                    [
                        "high_risk_transactions",
                        "anomalies",
                    ]
                ]
            )

except Exception as exc:
    st.error("Unable to load transaction risk analytics.")

    with st.expander("Technical details"):
        st.exception(exc)


def format_risk_flags(flags):
    """Convert JSONB risk explanations into readable text."""

    if flags is None:
        return "No risk flags"

    if isinstance(flags, str):
        try:
            flags = json.loads(flags)
        except (ValueError, TypeError):
            return flags

    if not isinstance(flags, list) or not flags:
        return "No risk flags"

    return "\n".join(
        f"- {flag.get('flag', 'UNKNOWN')}: "
        f"{flag.get('description', '')} "
        f"(+{flag.get('points', 0)} points)"
        for flag in flags
    )


def render_risk_kpis():
    """Display the latest transaction risk KPIs."""

    st.subheader("Transaction Risk Overview")

    summary = get_risk_dashboard_summary()

    if summary.empty:
        st.info("No transaction risk results available yet.")
        return

    row = summary.iloc[0]

    total = int(row["total_transactions"] or 0)
    flagged = int(row["flagged_transactions"] or 0)
    high = int(row["high_risk_transactions"] or 0)
    critical = int(row["critical_risk_transactions"] or 0)
    average = row["average_risk_score"]

    average = float(average) if pd.notna(average) else 0.0

    col1, col2, col3, col4, col5 = st.columns(5)

    col1.metric("Total Transactions", f"{total:,}")
    col2.metric("Flagged Transactions", f"{flagged:,}")
    col3.metric("High Risk", f"{high:,}")
    col4.metric("Critical Risk", f"{critical:,}")
    col5.metric("Average Risk Score", f"{average:.2f}/100")


def render_risk_distribution():
    """Display the latest risk category distribution."""

    st.subheader("Risk Category Distribution")

    distribution = get_risk_category_distribution()

    if distribution.empty:
        st.info("No risk category data available.")
        return

    fig = px.pie(
        distribution,
        names="risk_category",
        values="transaction_count",
        title="Transaction Distribution by Risk Category",
        hole=0.45,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_risk_trends():
    """Display changes in risk distribution across analysis runs."""

    st.subheader("Risk Analysis Trends")

    trends = get_risk_analysis_trends(limit=30)

    if trends.empty:
        st.info("No risk analysis history available.")
        return

    trends = trends.sort_values("completed_at")

    category_columns = [
        "low_risk_count",
        "medium_risk_count",
        "high_risk_count",
        "critical_risk_count",
    ]

    chart_data = trends[
        ["completed_at"] + category_columns
    ].copy()

    chart_data = chart_data.rename(
        columns={
            "low_risk_count": "LOW",
            "medium_risk_count": "MEDIUM",
            "high_risk_count": "HIGH",
            "critical_risk_count": "CRITICAL",
        }
    )

    chart_data = chart_data.melt(
        id_vars=["completed_at"],
        var_name="Risk Category",
        value_name="Transactions",
    )

    fig = px.line(
        chart_data,
        x="completed_at",
        y="Transactions",
        color="Risk Category",
        markers=True,
        title="Risk Category Counts Across Analysis Runs",
    )

    st.plotly_chart(fig, use_container_width=True)

    run_history = trends[
        [
            "completed_at",
            "total_transactions",
            "flagged_transactions",
            "low_risk_count",
            "medium_risk_count",
            "high_risk_count",
            "critical_risk_count",
        ]
    ].copy()

    st.dataframe(
        run_history.sort_values(
            "completed_at",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )


def render_highest_risk_transactions():
    """Display the highest-risk transactions and their explanations."""

    st.subheader("Highest-Risk Transactions")

    transactions = get_highest_risk_transactions(limit=20)

    if transactions.empty:
        st.info("No transaction risk results available.")
        return

    display_data = transactions[
        [
            "transaction_id",
            "risk_score",
            "risk_category",
            "payment_rail",
            "amount_minor",
            "currency",
            "latest_status",
            "risk_flag_count",
            "is_flagged",
        ]
    ].copy()

    display_data["amount"] = (
        display_data["amount_minor"].fillna(0) / 100
    ).round(2)

    display_data = display_data.drop(columns=["amount_minor"])

    display_data = display_data.rename(
        columns={
            "transaction_id": "Transaction ID",
            "risk_score": "Risk Score",
            "risk_category": "Risk Category",
            "payment_rail": "Payment Rail",
            "currency": "Currency",
            "latest_status": "Latest Status",
            "risk_flag_count": "Risk Flags",
            "is_flagged": "Flagged",
        }
    )

    st.dataframe(
        display_data,
        use_container_width=True,
        hide_index=True,
    )

    st.caption(
        "Amounts are displayed in major currency units. "
        "For INR, amount_minor is stored in paise."
    )

    transaction_ids = transactions[
        "transaction_id"
    ].astype(str).tolist()

    selected_transaction = st.selectbox(
        "Investigate a transaction",
        transaction_ids,
        key="risk_transaction_selector",
    )

    if selected_transaction:
        selected = transactions[
            transactions["transaction_id"].astype(str)
            == selected_transaction
        ]

        if not selected.empty:
            row = selected.iloc[0]

            st.markdown("#### Transaction Risk Explanation")

            st.write(f"**Transaction:** {selected_transaction}")
            st.write(f"**Risk Score:** {row['risk_score']}/100")
            st.write(f"**Risk Category:** {row['risk_category']}")

            st.write(
                format_risk_flags(row["risk_flags"])
            )

            with st.expander("View complete risk assessment history"):
                history = get_transaction_risk_details(
                    selected_transaction
                )

                if history.empty:
                    st.info("No historical assessments found.")
                else:
                    history = history.copy()

                    history["risk_flags"] = history[
                        "risk_flags"
                    ].apply(format_risk_flags)

                    st.dataframe(
                        history,
                        use_container_width=True,
                        hide_index=True,
                    )


def render_risk_dashboard():
    """Render the complete transaction risk dashboard."""

    st.header("Transaction Risk Monitoring")

    try:
        render_risk_kpis()

        st.divider()

        render_risk_distribution()

        st.divider()

        render_risk_trends()

        st.divider()

        render_highest_risk_transactions()

    except Exception as exc:
        logger.exception("Unable to render risk dashboard")

        st.error(
            "Unable to load transaction risk monitoring data. "
            "Check the database connection and risk analysis tables."
        )
        st.caption(str(exc))



def main():
    st.title("PayStream Monitoring Dashboard")

    st.caption(
        "Real-time transaction pipeline observability and monitoring"
    )

    with st.sidebar:
        st.header("Dashboard Controls")

        hours = st.selectbox(
            "Processing time range",
            options=[6, 12, 24, 48, 72],
            index=2,
            format_func=lambda value: f"Last {value} hours"
        )

        days = st.selectbox(
            "Data quality history",
            options=[1, 3, 7, 14, 30],
            index=2,
            format_func=lambda value: f"Last {value} days"
        )

        if st.button("Refresh Dashboard", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        st.caption("Dashboard data cache: 30 seconds")

    try:
        summary = load_summary()

        render_health()

        st.divider()

        render_overview(summary)

        st.divider()

        render_processing_trends(hours)

        st.divider()

        render_quality_monitoring(days)

        st.divider()

        render_failure_monitoring()

        st.divider()

        # Milestone 5.6
        render_risk_dashboard()

        st.divider()

        render_ml_dashboard()

        st.divider()

        st.caption(
            "PayStream | Milestone 4.4.5 | Observability Dashboard"
        )

    except Exception as exc:
        logger.exception("Dashboard data loading failed")

        st.error(
            "Unable to load dashboard data. "
            "Check PostgreSQL connectivity and monitoring table migrations."
        )

        with st.expander("Technical error details"):
            st.code(str(exc))


if __name__ == "__main__":
    main()
