"""
DataSentinel — Agent 4: Risk Narrative Generator
Synthesizes all pipeline outputs into:
  - Executive summary (3-4 sentences)
  - Full risk memo (markdown)
  - Structured risk register (table rows)

Writes to state: executive_summary, risk_memo, risk_register, pipeline_status=complete
"""

import os
import json
from datetime import datetime
from groq import Groq
from core.state import DataSentinelState, AuditEntry


def _log(action: str, detail: str) -> AuditEntry:
    return AuditEntry(
        agent="NarrativeGenerator",
        action=action,
        timestamp=datetime.utcnow().isoformat(),
        detail=detail
    )


SEVERITY_EMOJI = {
    "CRITICAL": "🔴",
    "HIGH": "🟠",
    "MEDIUM": "🟡",
    "LOW": "🟢",
    "INFO": "⚪"
}


def build_risk_register(findings: list, gaps: list) -> list[dict]:
    """Build structured risk register rows for display."""
    register = []

    for f in findings:
        register.append({
            "id": f["finding_id"],
            "type": "Risk Finding",
            "dimension": f["dimension"].title(),
            "field": f["field"],
            "severity": f["severity"],
            "emoji": SEVERITY_EMOJI.get(f["severity"], "⚪"),
            "description": f["description"],
            "evidence": f["evidence"],
            "override": f.get("reviewer_override") or "—"
        })

    for g in gaps:
        register.append({
            "id": g["control_id"],
            "type": "Control Gap",
            "dimension": g["risk_dimension"].title(),
            "field": "—",
            "severity": g["severity"],
            "emoji": SEVERITY_EMOJI.get(g["severity"], "⚪"),
            "description": g["gap_description"],
            "evidence": g["recommendation"],
            "override": "—"
        })

    # Sort by severity
    order = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3, "INFO": 4}
    register.sort(key=lambda x: order.get(x["severity"], 5))
    return register


def generate_narrative_with_llm(state: DataSentinelState) -> tuple[str, str]:
    """Use LLM to generate executive summary and risk memo."""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

    # Summarise key facts for the prompt
    findings = state["risk_findings"]
    gaps = state["control_gaps"]
    severity_counts = {}
    for f in findings:
        severity_counts[f["severity"]] = severity_counts.get(f["severity"], 0) + 1

    critical_findings = [f for f in findings if f["severity"] == "CRITICAL"]
    high_findings = [f for f in findings if f["severity"] == "HIGH"]
    critical_gaps = [g for g in gaps if g["severity"] == "CRITICAL"]

    prompt = f"""You are a Chief Data Officer writing a Data Risk Assessment memo for senior leadership.

DATASET: {state['dataset_name']}
DESCRIPTION: {state.get('dataset_description', 'Financial services customer dataset')}
OVERALL RISK SCORE: {state['overall_risk_score']}/100 ({state['overall_risk_tier']})
REVIEW DATE: {datetime.utcnow().strftime('%B %d, %Y')}
REVIEWER NOTES: {state.get('hitl_notes') or 'None provided'}

RISK FINDINGS SUMMARY:
- Total findings: {len(findings)}
- Severity breakdown: {json.dumps(severity_counts)}
- Critical findings: {json.dumps([{"field": f["field"], "description": f["description"]} for f in critical_findings])}
- High findings: {json.dumps([{"field": f["field"], "description": f["description"]} for f in high_findings[:3]])}

CONTROL GAPS SUMMARY:
- Total gaps: {len(gaps)}
- Critical gaps: {json.dumps([{"control": g["control_name"], "gap": g["gap_description"]} for g in critical_gaps])}

CONTROL FRAMEWORK: {state.get('control_framework_used', 'Internal Data Risk Control Framework')}

Please write:

1. EXECUTIVE SUMMARY (exactly 3-4 sentences, plain language, suitable for a VP/Director audience)

2. RISK MEMO (markdown format) with these sections:
   ## Assessment Overview
   ## Key Risk Findings
   ## Control Gaps Identified
   ## Immediate Actions Required
   ## Recommended Roadmap (3 bullet points max)

Keep the tone professional, factual, and action-oriented.
Do not fabricate specific numbers beyond what is provided above.

Format your response as:
EXECUTIVE_SUMMARY:
[your executive summary here]

RISK_MEMO:
[your full markdown memo here]
"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
        max_tokens=2000
    )

    raw = response.choices[0].message.content.strip()

    # Parse the two sections
    exec_summary = ""
    risk_memo = ""

    if "EXECUTIVE_SUMMARY:" in raw and "RISK_MEMO:" in raw:
        parts = raw.split("RISK_MEMO:")
        exec_part = parts[0].replace("EXECUTIVE_SUMMARY:", "").strip()
        memo_part = parts[1].strip()
        exec_summary = exec_part
        risk_memo = memo_part
    else:
        exec_summary = raw[:500]
        risk_memo = raw

    return exec_summary, risk_memo


def generate_narrative_fallback(state: DataSentinelState) -> tuple[str, str]:
    """Rule-based narrative generation when LLM is unavailable."""

    findings = state["risk_findings"]
    gaps = state["control_gaps"]
    severity_counts = {}
    for f in findings:
        severity_counts[f["severity"]] = severity_counts.get(f["severity"], 0) + 1

    critical_count = severity_counts.get("CRITICAL", 0)
    high_count = severity_counts.get("HIGH", 0)

    exec_summary = (
        f"The data risk assessment of '{state['dataset_name']}' identified {len(findings)} risk findings "
        f"with an overall risk score of {state['overall_risk_score']}/100 ({state['overall_risk_tier']} tier). "
        f"{critical_count} critical and {high_count} high severity findings require immediate attention. "
        f"Additionally, {len(gaps)} control gaps were identified against the data risk control framework."
    )

    memo_lines = [
        f"# Data Risk Assessment — {state['dataset_name']}",
        f"**Assessment Date:** {datetime.utcnow().strftime('%B %d, %Y')}",
        f"**Overall Risk Score:** {state['overall_risk_score']}/100 ({state['overall_risk_tier']})",
        f"**Control Framework:** {state.get('control_framework_used', 'Internal Framework')}",
        "",
        "## Assessment Overview",
        exec_summary,
        "",
        "## Key Risk Findings",
    ]

    for f in findings:
        emoji = SEVERITY_EMOJI.get(f["severity"], "⚪")
        memo_lines.append(f"- {emoji} **[{f['severity']}]** `{f['field']}` — {f['description']}")

    memo_lines += ["", "## Control Gaps Identified"]
    for g in gaps:
        emoji = SEVERITY_EMOJI.get(g["severity"], "⚪")
        memo_lines.append(f"- {emoji} **[{g['control_id']}]** {g['control_name']} — {g['gap_description']}")
        memo_lines.append(f"  - *Recommendation:* {g['recommendation']}")

    memo_lines += [
        "",
        "## Immediate Actions Required",
        "- Remediate all CRITICAL findings within 5 business days",
        "- Escalate HIGH findings to Data Steward for review",
        "- Initiate control gap remediation plan",
        "",
        "## Recommended Roadmap",
        "- **Week 1-2:** Address PII exposure and consent violations",
        "- **Week 3-4:** Implement missing controls for data quality dimensions",
        "- **Month 2:** Full control framework attestation and sign-off",
    ]

    return exec_summary, "\n".join(memo_lines)


def run_narrative_generator(state: DataSentinelState) -> DataSentinelState:
    """Main narrative generator agent node."""

    audit = [_log("START", "Generating risk narrative and register")]

    risk_register = build_risk_register(state["risk_findings"], state["control_gaps"])
    audit.append(_log("REGISTER", f"Risk register built: {len(risk_register)} entries"))

    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not set")

        exec_summary, risk_memo = generate_narrative_with_llm(state)
        audit.append(_log("LLM", "LLM-generated narrative complete"))

    except Exception as e:
        audit.append(_log("FALLBACK", f"Using rule-based narrative: {str(e)}"))
        exec_summary, risk_memo = generate_narrative_fallback(state)

    audit.append(_log("COMPLETE", "Pipeline complete. Risk memo and register ready."))

    return {
        **state,
        "executive_summary": exec_summary,
        "risk_memo": risk_memo,
        "risk_register": risk_register,
        "current_agent": "complete",
        "pipeline_status": "complete",
        "audit_log": audit
    }
