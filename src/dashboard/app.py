
import logging
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from dotenv import load_dotenv

from src.dashboard.queries import (
    get_dashboard_summary,
    get_processing_trends,
    get_quality_trends,
    get_latest_quality_checks,
    get_failure_summary,
    get_recent_failures,
    get_live_component_health
)


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
