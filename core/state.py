"""
DataSentinel — Core State Schema
Defines the shared state object that flows through all LangGraph agents.
Each agent reads from and writes to this state.
"""

from typing import TypedDict, Optional, Annotated
import operator


# ── Risk severity levels ──────────────────────────────────────────────────────
SEVERITY = ["CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO"]


# ── Individual risk finding ───────────────────────────────────────────────────
class RiskFinding(TypedDict):
    finding_id: str
    dimension: str          # quality | sensitivity | lineage | regulatory
    field: str              # which column or table element triggered this
    description: str
    severity: str           # CRITICAL | HIGH | MEDIUM | LOW | INFO
    evidence: str           # raw stat or value that triggered the finding
    reviewer_override: Optional[str]   # set during HITL step


# ── Control gap entry ─────────────────────────────────────────────────────────
class ControlGap(TypedDict):
    control_id: str
    control_name: str
    risk_dimension: str
    gap_description: str
    severity: str
    recommendation: str


# ── Audit log entry ───────────────────────────────────────────────────────────
class AuditEntry(TypedDict):
    agent: str
    action: str
    timestamp: str
    detail: str


# ── Master pipeline state ─────────────────────────────────────────────────────
class DataSentinelState(TypedDict):

    # INPUT
    dataset_name: str
    dataset_description: str
    raw_data_path: str

    # AGENT 1 — Profiler output
    profile_summary: Optional[dict]         # schema info, row count, col stats
    anomalies_detected: Optional[list]      # raw anomaly flags before classification

    # AGENT 2 — Risk Classifier output
    risk_findings: Annotated[list[RiskFinding], operator.add]
    overall_risk_score: Optional[float]     # 0-100
    overall_risk_tier: Optional[str]        # CRITICAL | HIGH | MEDIUM | LOW

    # HITL checkpoint
    hitl_approved: Optional[bool]           # True = reviewer signed off
    hitl_notes: Optional[str]              # reviewer comments

    # AGENT 3 — Control Gap Assessor output
    control_gaps: Annotated[list[ControlGap], operator.add]
    control_framework_used: Optional[str]

    # AGENT 4 — Narrative Generator output
    executive_summary: Optional[str]
    risk_memo: Optional[str]               # full markdown memo
    risk_register: Optional[list]          # structured table rows

    # AUDIT
    audit_log: Annotated[list[AuditEntry], operator.add]

    # PIPELINE CONTROL
    current_agent: Optional[str]
    pipeline_status: Optional[str]         # running | awaiting_review | complete | failed
    error_message: Optional[str]
