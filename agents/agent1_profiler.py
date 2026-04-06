"""
DataSentinel — Agent 1: Data Profiler
Ingests a CSV dataset and produces a structured profile:
  - Schema summary
  - Null rates per column
  - Duplicate key detection
  - Value distribution anomalies
  - Outlier detection
  - Inconsistent categorical values
  - PII column detection (heuristic)

Writes to state: profile_summary, anomalies_detected, audit_log
"""

import pandas as pd
import numpy as np
from datetime import datetime
from core.state import DataSentinelState, AuditEntry

# Heuristic PII column name patterns
PII_PATTERNS = ["ssn", "social", "passport", "dob", "birth", "email",
                "phone", "mobile", "address", "zip", "postal", "aadhaar",
                "pan_number", "credit_card", "card_number", "account_no"]


def _log(agent: str, action: str, detail: str) -> AuditEntry:
    return AuditEntry(
        agent=agent,
        action=action,
        timestamp=datetime.utcnow().isoformat(),
        detail=detail
    )


def detect_pii_columns(df: pd.DataFrame) -> list[str]:
    """Heuristic PII detection based on column name patterns."""
    pii_cols = []
    for col in df.columns:
        col_lower = col.lower().replace(" ", "_")
        if any(pattern in col_lower for pattern in PII_PATTERNS):
            pii_cols.append(col)
    return pii_cols


def detect_outliers(series: pd.Series, threshold: float = 3.0) -> dict:
    """Z-score based outlier detection for numeric columns."""
    if series.dropna().empty:
        return {"count": 0, "pct": 0.0, "examples": []}
    mean = series.mean()
    std = series.std()
    if std == 0:
        return {"count": 0, "pct": 0.0, "examples": []}
    z_scores = ((series - mean) / std).abs()
    outliers = series[z_scores > threshold].dropna()
    return {
    "count": int(len(outliers)),
    "pct": float(round(len(outliers) / len(series) * 100, 2)),
    "examples": [float(round(v, 2)) for v in outliers.head(3).tolist()]
    }


def detect_inconsistent_categories(series: pd.Series, max_unique: int = 20) -> dict:
    """Detect likely inconsistent values in low-cardinality categorical columns."""
    unique_vals = series.dropna().unique()
    if len(unique_vals) > max_unique:
        return {"flagged": False}

    # Group by lowercased+stripped version to find duplicates
    normalized = {}
    for val in unique_vals:
        key = str(val).lower().strip().replace("-", "_").replace(" ", "_")
        normalized.setdefault(key, []).append(val)

    inconsistent = {k: v for k, v in normalized.items() if len(v) > 1}
    return {
        "flagged": bool(inconsistent),
        "groups": inconsistent
    }


def run_profiler(state: DataSentinelState) -> DataSentinelState:
    """Main profiler agent node."""

    audit = []
    audit.append(_log("Profiler", "START", f"Loading dataset: {state['raw_data_path']}"))

    try:
        df = pd.read_csv(state["raw_data_path"])
    except Exception as e:
        return {
            **state,
            "pipeline_status": "failed",
            "error_message": f"Could not load dataset: {str(e)}",
            "audit_log": audit
        }

    n_rows, n_cols = df.shape
    audit.append(_log("Profiler", "LOAD", f"Loaded {n_rows} rows x {n_cols} columns"))

    # ── Schema summary ────────────────────────────────────────────────────────
    schema = {}
    for col in df.columns:
        schema[col] = {
            "dtype": str(df[col].dtype),
            "null_count": int(df[col].isna().sum()),
            "null_pct": float(round(df[col].isna().sum() / n_rows * 100, 2)),
            "unique_count": int(df[col].nunique()),
            "sample_values": df[col].dropna().head(3).tolist()
        }

    # ── Duplicate key detection ───────────────────────────────────────────────
    # Try to find a likely ID column
    id_candidates = [c for c in df.columns if any(x in c.lower() for x in ["id", "key", "uuid", "ref"])]
    duplicate_findings = {}
    for col in id_candidates:
        dup_count = int(df[col].duplicated().sum())
        if dup_count > 0:
            duplicate_findings[col] = {
                "duplicate_count": dup_count,
                "pct": round(dup_count / n_rows * 100, 2)
            }

    # ── Outlier detection (numeric columns) ──────────────────────────────────
    outlier_findings = {}
    for col in df.select_dtypes(include=[np.number]).columns:
        result = detect_outliers(df[col])
        if result["count"] > 0:
            outlier_findings[col] = result

    # ── Inconsistent categories ───────────────────────────────────────────────
    category_findings = {}
    for col in df.select_dtypes(include=["object"]).columns:
        result = detect_inconsistent_categories(df[col])
        if result.get("flagged"):
            category_findings[col] = result["groups"]

    # ── PII detection ─────────────────────────────────────────────────────────
    pii_columns = detect_pii_columns(df)

    # ── Missing value patterns ────────────────────────────────────────────────
    high_null_cols = {
        col: info for col, info in schema.items()
        if info["null_pct"] > 5.0
    }

    # ── Compile profile summary ───────────────────────────────────────────────
    profile_summary = {
        "dataset_name": state["dataset_name"],
        "n_rows": n_rows,
        "n_cols": n_cols,
        "columns": list(df.columns),
        "schema": schema,
        "duplicate_keys": duplicate_findings,
        "outliers": outlier_findings,
        "inconsistent_categories": category_findings,
        "pii_columns": pii_columns,
        "high_null_columns": high_null_cols,
    }

    # ── Compile raw anomaly list for Agent 2 ─────────────────────────────────
    anomalies = []

    for col, info in duplicate_findings.items():
        anomalies.append({
            "type": "duplicate_key",
            "field": col,
            "detail": f"{info['duplicate_count']} duplicate values ({info['pct']}%)"
        })

    for col, info in outlier_findings.items():
        anomalies.append({
            "type": "outlier",
            "field": col,
            "detail": f"{info['count']} outliers ({info['pct']}%), e.g. {info['examples']}"
        })

    for col, groups in category_findings.items():
        anomalies.append({
            "type": "inconsistent_category",
            "field": col,
            "detail": f"Inconsistent labels: {groups}"
        })

    for col in pii_columns:
        anomalies.append({
            "type": "pii_exposure",
            "field": col,
            "detail": f"Column '{col}' matches PII pattern — unmasked data detected"
        })

    for col, info in high_null_cols.items():
        anomalies.append({
            "type": "high_null_rate",
            "field": col,
            "detail": f"{info['null_pct']}% null rate ({info['null_count']} rows)"
        })

    audit.append(_log("Profiler", "COMPLETE",
        f"Profiling complete. {len(anomalies)} anomalies detected across "
        f"{n_rows} rows, {n_cols} columns."))

    return {
        **state,
        "profile_summary": profile_summary,
        "anomalies_detected": anomalies,
        "current_agent": "risk_classifier",
        "pipeline_status": "running",
        "audit_log": audit
    }
