"""
DataSentinel — Agent 2: Risk Classifier
Takes raw anomalies from the Profiler and classifies each into:
  - Dimension: quality | sensitivity | lineage | regulatory
  - Severity:  CRITICAL | HIGH | MEDIUM | LOW | INFO
  - Structured RiskFinding objects

Uses LLM (Groq/Llama) to enrich classifications with descriptions.
Falls back to rule-based classification if LLM is unavailable.

Writes to state: risk_findings, overall_risk_score, overall_risk_tier
"""

import os
import json
import uuid
from datetime import datetime
from groq import Groq
from core.state import DataSentinelState, RiskFinding, AuditEntry

# ── Severity weights for scoring ──────────────────────────────────────────────
SEVERITY_WEIGHTS = {"CRITICAL": 25, "HIGH": 15, "MEDIUM": 8, "LOW": 3, "INFO": 1}

# ── Rule-based fallback mappings ──────────────────────────────────────────────
ANOMALY_RULES = {
    "pii_exposure": {
        "dimension": "sensitivity",
        "severity_map": {
            "ssn": "CRITICAL", "social": "CRITICAL", "passport": "CRITICAL",
            "aadhaar": "CRITICAL", "pan_number": "CRITICAL",
            "email": "HIGH", "phone": "HIGH", "mobile": "HIGH",
            "address": "MEDIUM", "zip": "LOW"
        },
        "default_severity": "HIGH"
    },
    "duplicate_key": {
        "dimension": "quality",
        "default_severity": "HIGH"
    },
    "high_null_rate": {
        "dimension": "quality",
        "severity_thresholds": [(20, "HIGH"), (10, "MEDIUM"), (5, "LOW")]
    },
    "outlier": {
        "dimension": "quality",
        "default_severity": "MEDIUM"
    },
    "inconsistent_category": {
        "dimension": "lineage",
        "default_severity": "MEDIUM"
    },
    "consent_violation": {
        "dimension": "regulatory",
        "default_severity": "CRITICAL"
    }
}


def _log(action: str, detail: str) -> AuditEntry:
    return AuditEntry(
        agent="RiskClassifier",
        action=action,
        timestamp=datetime.utcnow().isoformat(),
        detail=detail
    )


def rule_based_classify(anomaly: dict) -> RiskFinding:
    """Fallback rule-based classifier when LLM is unavailable."""
    atype = anomaly["type"]
    field = anomaly["field"]
    rule = ANOMALY_RULES.get(atype, {"dimension": "quality", "default_severity": "LOW"})

    dimension = rule["dimension"]

    # Severity logic
    if atype == "pii_exposure":
        field_lower = field.lower()
        sev_map = rule.get("severity_map", {})
        severity = next((sev_map[k] for k in sev_map if k in field_lower), rule["default_severity"])

    elif atype == "high_null_rate":
        pct = float(anomaly["detail"].split("%")[0])
        severity = rule["default_severity"] if not rule.get("severity_thresholds") else "LOW"
        for threshold, sev in rule.get("severity_thresholds", []):
            if pct >= threshold:
                severity = sev
                break

    else:
        severity = rule.get("default_severity", "MEDIUM")

    return RiskFinding(
        finding_id=f"RF-{str(uuid.uuid4())[:8].upper()}",
        dimension=dimension,
        field=field,
        description=f"[Rule-based] {atype.replace('_', ' ').title()} detected in '{field}'",
        severity=severity,
        evidence=anomaly["detail"],
        reviewer_override=None
    )


def llm_classify_batch(anomalies: list[dict], dataset_context: str) -> list[RiskFinding]:
    """Use Groq/Llama to classify anomalies with richer descriptions."""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

    prompt = f"""You are a Data Risk Management expert at a financial services firm.
You are reviewing a dataset called: {dataset_context}

Below is a list of data anomalies detected by an automated profiler.
For each anomaly, classify it and return a JSON array.

Each item in the array must have:
- finding_id: unique string like "RF-XXXX"
- dimension: one of [quality, sensitivity, lineage, regulatory]
- field: the column name from the anomaly
- description: 1-2 sentence expert explanation of why this is a risk
- severity: one of [CRITICAL, HIGH, MEDIUM, LOW, INFO]
- evidence: copy the detail field from the anomaly as-is

Risk dimension guidance:
- quality: nulls, duplicates, outliers, invalid values
- sensitivity: PII, confidential data, unmasked identifiers
- lineage: inconsistent source labels, missing extraction dates, provenance gaps
- regulatory: consent violations, FCRA scope issues, UDAAP signals, data retention

Severity guidance:
- CRITICAL: immediate regulatory or legal exposure
- HIGH: significant data integrity or privacy risk
- MEDIUM: moderate risk requiring attention
- LOW: minor issue, monitor
- INFO: observation only

ANOMALIES TO CLASSIFY:
{json.dumps(anomalies, indent=2)}

Return ONLY a valid JSON array. No explanation text outside the array.
"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=3000
    )

    raw = response.choices[0].message.content.strip()

    # Strip markdown code fences if present
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    classified = json.loads(raw)

    findings = []
    for item in classified:
        findings.append(RiskFinding(
            finding_id=item.get("finding_id", f"RF-{str(uuid.uuid4())[:8].upper()}"),
            dimension=item.get("dimension", "quality"),
            field=item.get("field", "unknown"),
            description=item.get("description", ""),
            severity=item.get("severity", "MEDIUM"),
            evidence=item.get("evidence", ""),
            reviewer_override=None
        ))

    return findings


def calculate_risk_score(findings: list[RiskFinding]) -> tuple[float, str]:
    """Compute overall risk score (0-100) and tier."""
    if not findings:
        return 0.0, "INFO"

    raw_score = sum(SEVERITY_WEIGHTS.get(f["severity"], 0) for f in findings)
    normalized = min(raw_score, 100)

    if normalized >= 75:
        tier = "CRITICAL"
    elif normalized >= 50:
        tier = "HIGH"
    elif normalized >= 25:
        tier = "MEDIUM"
    else:
        tier = "LOW"

    return float(round(normalized, 1)), tier


def run_risk_classifier(state: DataSentinelState) -> DataSentinelState:
    """Main risk classifier agent node."""

    audit = [_log("START", f"Classifying {len(state['anomalies_detected'])} anomalies")]
    anomalies = state["anomalies_detected"]

    # Also check for consent violations from raw data (domain-specific rule)
    # This would be passed in from profiler in a full implementation
    # For now, check if consent-related anomaly was detected
    consent_anomalies = [a for a in anomalies if "consent" in a.get("field", "").lower()
                         or "consent" in a.get("detail", "").lower()]

    findings = []

    # Try LLM classification first
    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not set — using rule-based fallback")

        context = f"{state['dataset_name']}: {state.get('dataset_description', 'Financial services customer dataset')}"
        findings = llm_classify_batch(anomalies, context)
        audit.append(_log("LLM_CLASSIFY", f"LLM classified {len(findings)} findings"))

    except Exception as e:
        audit.append(_log("FALLBACK", f"LLM unavailable ({str(e)}) — using rule-based classifier"))
        for anomaly in anomalies:
            findings.append(rule_based_classify(anomaly))

    overall_score, overall_tier = calculate_risk_score(findings)

    audit.append(_log("SCORE", f"Overall risk score: {overall_score}/100 | Tier: {overall_tier}"))
    audit.append(_log("COMPLETE", f"Classification complete. {len(findings)} risk findings generated."))

    return {
        **state,
        "risk_findings": findings,
        "overall_risk_score": overall_score,
        "overall_risk_tier": overall_tier,
        "current_agent": "hitl_checkpoint",
        "pipeline_status": "awaiting_review",
        "audit_log": audit
    }
