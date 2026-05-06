"""Data quality check functions for the unified payment platform."""

from __future__ import annotations

from datetime import datetime, timezone

import numpy as np
import pandas as pd

VALID_SOURCES = {"cards", "transfers", "bill_payments"}
VALID_STATUSES = {"completed", "failed", "pending"}
VALID_TYPES = {"card_transaction", "transfer", "bill_payment"}
REQUIRED_COLS = [
    "event_id",
    "source_system",
    "source_event_id",
    "customer_id",
    "amount",
    "currency",
    "event_timestamp",
    "status",
    "payment_type",
    "schema_version",
]


def check_schema_compliance(df: pd.DataFrame) -> pd.DataFrame:
    """Run field-level validation rules and return a results DataFrame."""
    rules: list[dict] = []

    for col in REQUIRED_COLS:
        if col in df.columns:
            null_count = int(df[col].isna().sum())
            rules.append(
                {
                    "rule": f"{col}_not_null",
                    "column": col,
                    "failures": null_count,
                    "total": len(df),
                }
            )

    if "source_system" in df.columns:
        bad = int((~df["source_system"].isin(VALID_SOURCES)).sum())
        rules.append(
            {
                "rule": "source_system_valid_enum",
                "column": "source_system",
                "failures": bad,
                "total": len(df),
            }
        )

    if "status" in df.columns:
        bad = int((~df["status"].isin(VALID_STATUSES)).sum())
        rules.append(
            {
                "rule": "status_valid_enum",
                "column": "status",
                "failures": bad,
                "total": len(df),
            }
        )

    if "payment_type" in df.columns:
        bad = int((~df["payment_type"].isin(VALID_TYPES)).sum())
        rules.append(
            {
                "rule": "payment_type_valid_enum",
                "column": "payment_type",
                "failures": bad,
                "total": len(df),
            }
        )

    if "currency" in df.columns:
        valid_cur = df["currency"].dropna().str.match(r"^[A-Z]{3}$")
        bad = int((~valid_cur).sum())
        rules.append(
            {
                "rule": "currency_iso_4217",
                "column": "currency",
                "failures": bad,
                "total": len(df),
            }
        )

    if "amount" in df.columns:
        amt = pd.to_numeric(df["amount"], errors="coerce")
        bad = int((amt < 0).sum()) + int(amt.isna().sum())
        rules.append(
            {
                "rule": "amount_non_negative",
                "column": "amount",
                "failures": bad,
                "total": len(df),
            }
        )

    result = pd.DataFrame(rules)
    if len(result):
        result["pass_rate"] = (
            (result["total"] - result["failures"]) / result["total"] * 100
        ).round(2)
    return result


def compliance_by_source(df: pd.DataFrame) -> pd.DataFrame:
    """Compute overall compliance rate per source system."""
    rows = []
    for src in sorted(df["source_system"].unique()):
        subset = df[df["source_system"] == src]
        checks = check_schema_compliance(subset)
        total_checks = int(checks["total"].sum())
        total_failures = int(checks["failures"].sum())
        rate = (
            round((total_checks - total_failures) / total_checks * 100, 2)
            if total_checks
            else 100.0
        )
        rows.append(
            {
                "source_system": src,
                "total_checks": total_checks,
                "failures": total_failures,
                "compliance_pct": rate,
            }
        )
    return pd.DataFrame(rows)


def check_freshness(df: pd.DataFrame) -> pd.DataFrame:
    """Compute time since last event per source system."""
    now = datetime.now(timezone.utc)
    rows = []
    for src in sorted(df["source_system"].unique()):
        subset = df[df["source_system"] == src]
        last_ts = pd.to_datetime(subset["event_timestamp"]).max()
        if pd.isna(last_ts):
            hours_ago = float("inf")
        else:
            if last_ts.tzinfo is None:
                last_ts = last_ts.tz_localize("UTC")
            hours_ago = round((now - last_ts).total_seconds() / 3600, 1)
        if hours_ago <= 24:
            rag = "green"
        elif hours_ago <= 168:
            rag = "yellow"
        else:
            rag = "red"
        rows.append(
            {
                "source_system": src,
                "last_event": str(last_ts),
                "hours_ago": hours_ago,
                "status": rag,
            }
        )
    return pd.DataFrame(rows)


def check_volume_anomalies(
    df: pd.DataFrame, window: int = 7, z_thresh: float = 2.0
) -> pd.DataFrame:
    """Detect daily volume anomalies per source using rolling z-scores."""
    df = df.copy()
    df["event_date"] = pd.to_datetime(df["event_timestamp"]).dt.date
    daily = df.groupby(["source_system", "event_date"]).size().reset_index(name="count")
    results = []
    for src in daily["source_system"].unique():
        src_data = daily[daily["source_system"] == src].sort_values("event_date").copy()
        src_data["rolling_mean"] = (
            src_data["count"].rolling(window, min_periods=1).mean()
        )
        src_data["rolling_std"] = (
            src_data["count"].rolling(window, min_periods=1).std().fillna(0)
        )
        src_data["z_score"] = np.where(
            src_data["rolling_std"] > 0,
            (src_data["count"] - src_data["rolling_mean"]) / src_data["rolling_std"],
            0,
        )
        src_data["is_anomaly"] = src_data["z_score"].abs() > z_thresh
        results.append(src_data)
    return pd.concat(results, ignore_index=True)


def check_null_rates(df: pd.DataFrame) -> pd.DataFrame:
    """Compute null rate per column per source system."""
    rows = []
    check_cols = [c for c in df.columns if not c.startswith("_dlt_")]
    for src in sorted(df["source_system"].unique()):
        subset = df[df["source_system"] == src]
        total = len(subset)
        for col in check_cols:
            nulls = int(subset[col].isna().sum())
            rate = round(nulls / total * 100, 2) if total else 0.0
            rows.append(
                {
                    "source_system": src,
                    "column": col,
                    "null_count": nulls,
                    "total": total,
                    "null_pct": rate,
                }
            )
    return pd.DataFrame(rows)


def compute_health_score(df: pd.DataFrame) -> dict:
    """Compute weighted overall health score (0-100) with dimension breakdown."""
    compliance = compliance_by_source(df)
    freshness = check_freshness(df)
    null_rates = check_null_rates(df)
    volume = check_volume_anomalies(df)

    schema_score = compliance["compliance_pct"].mean() if len(compliance) else 100.0

    freshness_map = {"green": 100, "yellow": 60, "red": 20}
    freshness_score = (
        freshness["status"].map(freshness_map).mean() if len(freshness) else 100.0
    )

    required_nulls = null_rates[null_rates["column"].isin(REQUIRED_COLS)]
    completeness_score = (
        100.0 - required_nulls["null_pct"].mean() if len(required_nulls) else 100.0
    )

    anomaly_pct = volume["is_anomaly"].mean() * 100 if len(volume) else 0.0
    volume_score = max(0, 100.0 - anomaly_pct * 5)

    weights = {"schema": 0.30, "freshness": 0.25, "completeness": 0.25, "volume": 0.20}
    overall = (
        weights["schema"] * schema_score
        + weights["freshness"] * freshness_score
        + weights["completeness"] * completeness_score
        + weights["volume"] * volume_score
    )

    def rag(s):
        if s >= 90:
            return "green"
        return "yellow" if s >= 70 else "red"

    return {
        "overall": round(overall, 1),
        "overall_rag": rag(overall),
        "dimensions": {
            "Schema Compliance": {
                "score": round(schema_score, 1),
                "rag": rag(schema_score),
            },
            "Freshness": {
                "score": round(freshness_score, 1),
                "rag": rag(freshness_score),
            },
            "Completeness": {
                "score": round(completeness_score, 1),
                "rag": rag(completeness_score),
            },
            "Volume Stability": {
                "score": round(volume_score, 1),
                "rag": rag(volume_score),
            },
        },
    }
