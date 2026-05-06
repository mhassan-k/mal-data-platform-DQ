"""Elementary-inspired Data Quality Monitoring Dashboard for Mal Payment Platform."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from dq_checks import (
    check_freshness,
    check_null_rates,
    check_schema_compliance,
    check_volume_anomalies,
    compliance_by_source,
    compute_health_score,
)

st.set_page_config(page_title="Mal DQ Monitor", layout="wide", page_icon="🛡️")

RAG_COLORS = {"green": "#27ae60", "yellow": "#f39c12", "red": "#e74c3c"}
RAG_EMOJI = {"green": "🟢", "yellow": "🟡", "red": "🔴"}

# ── Data Loading ──
PARQUET_LOCAL = Path(__file__).parent / "data" / "payment_events.parquet"
PARQUET_SIBLING = (
    Path(__file__).parent.parent
    / "mal-cross-product-data-platform"
    / "data"
    / "output"
    / "payment_events.parquet"
)
PARQUET = PARQUET_LOCAL if PARQUET_LOCAL.exists() else PARQUET_SIBLING

if not PARQUET.exists():
    st.error(
        "No Parquet file found. Run the payment pipeline first or place payment_events.parquet in data/."
    )
    st.stop()


@st.cache_data(ttl=300)
def load_data() -> pd.DataFrame:
    con = duckdb.connect()
    df = con.execute(f"SELECT * FROM read_parquet('{PARQUET}')").fetchdf()
    con.close()
    return df


df = load_data()

# ── Sidebar Navigation ──
st.sidebar.image("https://img.icons8.com/fluency/48/shield.png", width=40)
st.sidebar.title("Mal DQ Monitor")
page = st.sidebar.radio(
    "Navigate",
    ["Health Overview", "Schema Compliance", "Data Freshness", "Anomaly Detection"],
    label_visibility="collapsed",
)
st.sidebar.divider()
st.sidebar.caption(f"Source: {PARQUET.name}")
st.sidebar.caption(f"Records: {len(df):,}")
st.sidebar.caption(f"Sources: {', '.join(sorted(df['source_system'].unique()))}")


# ═══════════════════════════════════════════════════════════════
# PAGE 1: HEALTH OVERVIEW
# ═══════════════════════════════════════════════════════════════
if page == "Health Overview":
    st.title("🛡️ Data Health Overview")
    st.caption(
        "Inspired by Elementary — unified health scoring across all payment squads"
    )

    health = compute_health_score(df)

    # Overall score
    rag = health["overall_rag"]
    st.markdown(
        f"<div style='text-align:center;padding:20px;border-radius:12px;"
        f"background:{RAG_COLORS[rag]}22;border:2px solid {RAG_COLORS[rag]}'>"
        f"<h1 style='color:{RAG_COLORS[rag]};margin:0'>{RAG_EMOJI[rag]} {health['overall']}/100</h1>"
        f"<p style='margin:0;color:gray'>Overall Data Health Score</p></div>",
        unsafe_allow_html=True,
    )
    st.write("")

    # Dimension pillars
    dims = health["dimensions"]
    cols = st.columns(len(dims))
    for col, (name, info) in zip(cols, dims.items()):
        r = info["rag"]
        col.markdown(
            f"<div style='text-align:center;padding:15px;border-radius:8px;"
            f"background:{RAG_COLORS[r]}15;border:1px solid {RAG_COLORS[r]}55'>"
            f"<h3 style='margin:0;color:{RAG_COLORS[r]}'>{info['score']}</h3>"
            f"<p style='margin:0;font-size:0.85em'>{RAG_EMOJI[r]} {name}</p></div>",
            unsafe_allow_html=True,
        )
    st.write("")

    # Per-source cards
    st.subheader("Source System Health")
    compliance = compliance_by_source(df)
    freshness = check_freshness(df)
    null_rates = check_null_rates(df)

    src_cols = st.columns(3)
    for i, src in enumerate(sorted(df["source_system"].unique())):
        with src_cols[i]:
            comp_row = compliance[compliance["source_system"] == src]
            fresh_row = freshness[freshness["source_system"] == src]
            src_nulls = null_rates[null_rates["source_system"] == src]

            comp_pct = comp_row["compliance_pct"].iloc[0] if len(comp_row) else 0
            fresh_status = fresh_row["status"].iloc[0] if len(fresh_row) else "red"
            avg_null = src_nulls["null_pct"].mean() if len(src_nulls) else 0
            n_records = len(df[df["source_system"] == src])

            st.markdown(f"**{src.replace('_', ' ').title()}**")
            st.metric("Records", f"{n_records:,}")
            st.metric("Schema Compliance", f"{comp_pct}%")
            st.markdown(
                f"Freshness: {RAG_EMOJI[fresh_status]} **{fresh_status.upper()}**"
            )
            st.metric("Avg Null Rate", f"{avg_null:.1f}%")


# ═══════════════════════════════════════════════════════════════
# PAGE 2: SCHEMA COMPLIANCE
# ═══════════════════════════════════════════════════════════════
elif page == "Schema Compliance":
    st.title("📋 Schema Compliance")
    st.caption("Field-level validation across all source systems")

    # Overall compliance by source
    compliance = compliance_by_source(df)
    cols = st.columns(len(compliance))
    for i, (_, row) in enumerate(compliance.iterrows()):
        rag = (
            "green"
            if row["compliance_pct"] >= 99
            else ("yellow" if row["compliance_pct"] >= 95 else "red")
        )
        cols[i].metric(
            row["source_system"].replace("_", " ").title(),
            f"{row['compliance_pct']}%",
            f"{row['failures']} failures",
            delta_color="inverse",
        )

    st.divider()

    # Detailed rule results
    st.subheader("Validation Rules")
    checks = check_schema_compliance(df)
    st.dataframe(
        checks.style.background_gradient(
            subset=["pass_rate"], cmap="RdYlGn", vmin=90, vmax=100
        ),
        use_container_width=True,
        hide_index=True,
    )

    # Per-source breakdown
    st.subheader("Compliance by Source & Rule")
    for src in sorted(df["source_system"].unique()):
        with st.expander(f"{src.replace('_', ' ').title()}"):
            src_checks = check_schema_compliance(df[df["source_system"] == src])
            failed = src_checks[src_checks["failures"] > 0]
            if len(failed):
                st.dataframe(failed, use_container_width=True, hide_index=True)
            else:
                st.success("All rules passing")

    # Compliance trend by date
    st.subheader("Compliance Trend by Date")
    df_copy = df.copy()
    df_copy["event_date"] = pd.to_datetime(df_copy["event_timestamp"]).dt.date
    dates = sorted(df_copy["event_date"].unique())
    trend_rows = []
    sample_dates = dates[:: max(1, len(dates) // 30)]
    for d in sample_dates:
        day_df = df_copy[df_copy["event_date"] == d]
        day_checks = check_schema_compliance(day_df)
        total = day_checks["total"].sum()
        failures = day_checks["failures"].sum()
        rate = round((total - failures) / total * 100, 2) if total else 100
        trend_rows.append({"date": d, "compliance_pct": rate})
    trend_df = pd.DataFrame(trend_rows).set_index("date")
    st.line_chart(trend_df)


# ═══════════════════════════════════════════════════════════════
# PAGE 3: DATA FRESHNESS
# ═══════════════════════════════════════════════════════════════
elif page == "Data Freshness":
    st.title("⏱️ Data Freshness")
    st.caption("Time since last successful ingestion per source system")

    freshness = check_freshness(df)

    # Freshness cards
    cols = st.columns(len(freshness))
    for i, (_, row) in enumerate(freshness.iterrows()):
        r = row["status"]
        cols[i].markdown(
            f"<div style='text-align:center;padding:20px;border-radius:10px;"
            f"background:{RAG_COLORS[r]}18;border:2px solid {RAG_COLORS[r]}'>"
            f"<h4 style='margin:0'>{row['source_system'].replace('_', ' ').title()}</h4>"
            f"<h2 style='color:{RAG_COLORS[r]};margin:5px 0'>{RAG_EMOJI[r]} {row['hours_ago']:.0f}h ago</h2>"
            f"<p style='margin:0;font-size:0.8em;color:gray'>Last: {row['last_event'][:19]}</p></div>",
            unsafe_allow_html=True,
        )

    st.write("")
    st.subheader("Freshness Thresholds")
    st.markdown(
        "- 🟢 **Green**: Last event within 24 hours\n- 🟡 **Yellow**: 1-7 days ago\n- 🔴 **Red**: More than 7 days ago"
    )

    # Freshness timeline
    st.subheader("Event Timeline by Source")
    df_copy = df.copy()
    df_copy["event_date"] = pd.to_datetime(df_copy["event_timestamp"]).dt.date
    timeline = (
        df_copy.groupby(["event_date", "source_system"])
        .size()
        .reset_index(name="events")
    )
    pivot = timeline.pivot(
        index="event_date", columns="source_system", values="events"
    ).fillna(0)
    st.line_chart(pivot)

    # Staleness detail table
    st.subheader("Freshness Detail")
    st.dataframe(freshness, use_container_width=True, hide_index=True)


# ═══════════════════════════════════════════════════════════════
# PAGE 4: ANOMALY DETECTION
# ═══════════════════════════════════════════════════════════════
elif page == "Anomaly Detection":
    st.title("🔍 Anomaly Detection")
    st.caption("Volume shifts, null rate spikes, and amount outliers across sources")

    # Volume anomalies
    st.subheader("Daily Volume Anomalies")
    vol = check_volume_anomalies(df)
    anomaly_count = int(vol["is_anomaly"].sum())
    total_days = len(vol)
    st.metric(
        "Anomalous Days",
        f"{anomaly_count} / {total_days}",
        f"{anomaly_count / total_days * 100:.1f}%",
    )

    for src in sorted(vol["source_system"].unique()):
        src_vol = vol[vol["source_system"] == src].copy()
        src_vol["event_date"] = pd.to_datetime(src_vol["event_date"])
        with st.expander(
            f"{src.replace('_', ' ').title()} — Volume Trend", expanded=True
        ):
            chart_data = src_vol.set_index("event_date")[
                ["count", "rolling_mean"]
            ].rename(columns={"count": "Actual", "rolling_mean": "Rolling Mean"})
            st.line_chart(chart_data)
            anomalies = src_vol[src_vol["is_anomaly"]]
            if len(anomalies):
                st.warning(f"{len(anomalies)} anomalous days detected")
                st.dataframe(
                    anomalies[["event_date", "count", "rolling_mean", "z_score"]],
                    use_container_width=True,
                    hide_index=True,
                )
            else:
                st.success("No volume anomalies detected")

    st.divider()

    # Null rate analysis
    st.subheader("Null Rate by Column")
    null_rates = check_null_rates(df)
    high_nulls = null_rates[null_rates["null_pct"] > 0].sort_values(
        "null_pct", ascending=False
    )
    if len(high_nulls):
        st.dataframe(
            high_nulls.style.background_gradient(
                subset=["null_pct"], cmap="OrRd", vmin=0, vmax=100
            ),
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.success("No null values detected in any column")

    # Null rate pivot heatmap
    st.subheader("Null Rate Heatmap (Source x Column)")
    pivot_nulls = null_rates[null_rates["null_pct"] > 0].pivot_table(
        index="source_system", columns="column", values="null_pct", fill_value=0
    )
    if len(pivot_nulls):
        st.dataframe(
            pivot_nulls.style.background_gradient(cmap="OrRd", vmin=0, vmax=100).format(
                "{:.1f}%"
            ),
            use_container_width=True,
        )

    st.divider()

    # Amount anomalies
    st.subheader("Amount Distribution by Source")
    df_copy = df.copy()
    df_copy["amt"] = pd.to_numeric(df_copy["amount"], errors="coerce")
    for src in sorted(df_copy["source_system"].unique()):
        src_df = df_copy[df_copy["source_system"] == src]
        with st.expander(f"{src.replace('_', ' ').title()} — Amount Stats"):
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Mean", f"${src_df['amt'].mean():,.0f}")
            c2.metric("Median", f"${src_df['amt'].median():,.0f}")
            c3.metric("Std Dev", f"${src_df['amt'].std():,.0f}")
            c4.metric("Max", f"${src_df['amt'].max():,.0f}")
