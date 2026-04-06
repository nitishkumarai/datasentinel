"""
DataSentinel — Agent 3: Control Gap Assessor
Maps identified risk findings against a control framework (RAG-powered).
Identifies where controls are absent, weak, or untested.

Uses FAISS + sentence-transformers to retrieve relevant controls,
then uses LLM to assess gaps.

Writes to state: control_gaps, control_framework_used
"""

import os
import json
from datetime import datetime
from groq import Groq
from core.state import DataSentinelState, ControlGap, AuditEntry

RAG_AVAILABLE = False


def _log(action: str, detail: str) -> AuditEntry:
    return AuditEntry(
        agent="ControlGapAssessor",
        action=action,
        timestamp=datetime.utcnow().isoformat(),
        detail=detail
    )


# ── Lightweight built-in control framework ────────────────────────────────────
# SR 11-7 / DAMA / BCBS 239 inspired data risk controls
CONTROL_FRAMEWORK = [
    {
        "control_id": "DQ-01",
        "control_name": "Data Completeness Monitoring",
        "dimension": "quality",
        "description": "Automated monitoring of null/missing rates per field with breach thresholds and alerts."
    },
    {
        "control_id": "DQ-02",
        "control_name": "Duplicate Key Prevention",
        "dimension": "quality",
        "description": "Primary key uniqueness constraint enforced at ingestion. Duplicate detection job runs daily."
    },
    {
        "control_id": "DQ-03",
        "control_name": "Outlier Detection and Review",
        "dimension": "quality",
        "description": "Statistical outlier detection (IQR / Z-score) on numeric fields with manual review queue for flagged records."
    },
    {
        "control_id": "DQ-04",
        "control_name": "Categorical Value Standardization",
        "dimension": "quality",
        "description": "Controlled vocabulary / reference data management for categorical fields. Ingestion rejects non-standard values."
    },
    {
        "control_id": "SE-01",
        "control_name": "PII Data Masking and Tokenization",
        "dimension": "sensitivity",
        "description": "All PII fields (SSN, email, phone) masked or tokenized before landing in analytics environments. Clear-text PII only in source systems."
    },
    {
        "control_id": "SE-02",
        "control_name": "Data Classification and Tagging",
        "dimension": "sensitivity",
        "description": "Automated data classification engine tags columns with sensitivity level (Public / Internal / Confidential / Restricted). Restricted data triggers access review."
    },
    {
        "control_id": "SE-03",
        "control_name": "Access Control and Role-Based Permissions",
        "dimension": "sensitivity",
        "description": "Role-based access control (RBAC) on all datasets. Sensitive fields require data steward approval."
    },
    {
        "control_id": "LN-01",
        "control_name": "Data Lineage Tracking",
        "dimension": "lineage",
        "description": "End-to-end lineage metadata captured for all datasets: source system, extraction timestamp, transformation steps, load timestamp."
    },
    {
        "control_id": "LN-02",
        "control_name": "Source System Standardization",
        "dimension": "lineage",
        "description": "Canonical source system registry maintained. All ingestion pipelines reference the registry — free-text source labels not permitted."
    },
    {
        "control_id": "LN-03",
        "control_name": "Data Freshness Monitoring",
        "dimension": "lineage",
        "description": "Extraction timestamp present and validated on every record. Stale or missing timestamps trigger pipeline alert."
    },
    {
        "control_id": "RG-01",
        "control_name": "Consent Management and Verification",
        "dimension": "regulatory",
        "description": "Consent flag validated before any marketing or outreach action. Records with consent_flag=N blocked from marketing workflows. Reviewed quarterly."
    },
    {
        "control_id": "RG-02",
        "control_name": "FCRA / CCPA Scope Compliance",
        "dimension": "regulatory",
        "description": "Geographic scope of data usage validated against applicable regulations. Records from territories with different regulatory treatment flagged for review."
    },
    {
        "control_id": "RG-03",
        "control_name": "Data Retention and Deletion Controls",
        "dimension": "regulatory",
        "description": "Retention schedule enforced per data type. Automated deletion jobs run at schedule. Deletion audit trail maintained."
    },
    {
        "control_id": "RG-04",
        "control_name": "Model and Analytics Governance (SR 11-7)",
        "dimension": "regulatory",
        "description": "All models using this dataset registered in Model Risk Management inventory. Data quality attestation required before model deployment."
    },
]


def build_faiss_index(controls: list[dict]) -> tuple:
    """Build FAISS index over control descriptions for RAG retrieval."""
    model = SentenceTransformer("all-MiniLM-L6-v2")
    texts = [f"{c['control_name']}: {c['description']}" for c in controls]
    embeddings = model.encode(texts, convert_to_numpy=True)

    index = faiss.IndexFlatL2(embeddings.shape[1])
    index.add(embeddings.astype(np.float32))

    return index, model, texts


def retrieve_relevant_controls(query: str, index, model, controls: list[dict], top_k: int = 3) -> list[dict]:
    """Retrieve top-k relevant controls for a given risk finding."""
    import numpy as np
    query_embedding = model.encode([query], convert_to_numpy=True).astype(np.float32)
    distances, indices = index.search(query_embedding, top_k)
    return [controls[i] for i in indices[0] if i < len(controls)]


def assess_gaps_with_llm(findings: list, relevant_controls: list[dict], dataset_name: str) -> list[ControlGap]:
    """Use LLM to assess control gaps given risk findings and relevant controls."""

    client = Groq(api_key=os.environ.get("GROQ_API_KEY"))

    findings_text = json.dumps([
        {"field": f["field"], "dimension": f["dimension"],
         "severity": f["severity"], "description": f["description"]}
        for f in findings
    ], indent=2)

    controls_text = json.dumps([
        {"id": c["control_id"], "name": c["control_name"],
         "dimension": c["dimension"], "description": c["description"]}
        for c in relevant_controls
    ], indent=2)

    prompt = f"""You are a Data Risk and Controls expert at a financial services firm.

Dataset under review: {dataset_name}

RISK FINDINGS identified:
{findings_text}

CONTROL FRAMEWORK (relevant controls):
{controls_text}

Your task: For each risk finding, assess whether the relevant control exists and is effective.
Identify control gaps — cases where a control is absent, ineffective, or only partially implemented.

Return a JSON array of control gaps. Each gap must have:
- control_id: the control ID from the framework (e.g. "DQ-01"), or "MISSING" if no control exists
- control_name: the control name
- risk_dimension: quality | sensitivity | lineage | regulatory
- gap_description: 1-2 sentences explaining the specific gap relative to the finding
- severity: CRITICAL | HIGH | MEDIUM | LOW
- recommendation: 1 concrete action to close the gap

Only include genuine gaps — do not fabricate gaps where controls are likely adequate.
Return ONLY a valid JSON array with no explanation text outside it.
"""

    response = client.chat.completions.create(
        model="llama-3.3-70b-versatile",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.1,
        max_tokens=3000
    )

    raw = response.choices[0].message.content.strip()
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
    raw = raw.strip()

    gaps_data = json.loads(raw)
    return [ControlGap(**g) for g in gaps_data]


def rule_based_gaps(findings: list, controls: list[dict]) -> list[ControlGap]:
    """Fallback: simple dimension-based gap mapping."""
    dimension_control_map = {}
    for c in controls:
        dimension_control_map.setdefault(c["dimension"], []).append(c)

    gaps = []
    seen_controls = set()

    for finding in findings:
        dim = finding["dimension"]
        relevant = dimension_control_map.get(dim, [])

        if not relevant:
            gaps.append(ControlGap(
                control_id="MISSING",
                control_name="No Control Defined",
                risk_dimension=dim,
                gap_description=f"No control framework entry found for {dim} dimension risk in '{finding['field']}'",
                severity=finding["severity"],
                recommendation=f"Define and implement a control for {dim} risk management"
            ))
        else:
            for ctrl in relevant[:1]:  # Take first matching control
                if ctrl["control_id"] not in seen_controls:
                    seen_controls.add(ctrl["control_id"])
                    gaps.append(ControlGap(
                        control_id=ctrl["control_id"],
                        control_name=ctrl["control_name"],
                        risk_dimension=dim,
                        gap_description=f"Control '{ctrl['control_name']}' may be absent or ineffective — {finding['description']}",
                        severity=finding["severity"],
                        recommendation=f"Validate effectiveness of {ctrl['control_name']} against current findings"
                    ))
    return gaps


def run_control_gap_assessor(state: DataSentinelState) -> DataSentinelState:
    """Main control gap assessor agent node."""

    audit = [_log("START", f"Assessing control gaps for {len(state['risk_findings'])} findings")]
    findings = state["risk_findings"]
    control_gaps = []

    try:
        api_key = os.environ.get("GROQ_API_KEY")
        if not api_key:
            raise ValueError("GROQ_API_KEY not set")

        if RAG_AVAILABLE:
            audit.append(_log("RAG", "Building FAISS index over control framework"))
            index, model, texts = build_faiss_index(CONTROL_FRAMEWORK)

            # Retrieve relevant controls for all findings combined
            combined_query = " ".join([
                f"{f['dimension']} {f['field']} {f['description']}" for f in findings
            ])
            relevant_controls = retrieve_relevant_controls(
                combined_query, index, model, CONTROL_FRAMEWORK, top_k=8
            )
            audit.append(_log("RAG", f"Retrieved {len(relevant_controls)} relevant controls"))
        else:
            relevant_controls = CONTROL_FRAMEWORK
            audit.append(_log("RAG", "sentence-transformers not available — using full control framework"))

        control_gaps = assess_gaps_with_llm(findings, relevant_controls, state["dataset_name"])
        audit.append(_log("LLM", f"LLM identified {len(control_gaps)} control gaps"))

    except Exception as e:
        audit.append(_log("FALLBACK", f"Using rule-based gap assessment: {str(e)}"))
        control_gaps = rule_based_gaps(findings, CONTROL_FRAMEWORK)

    audit.append(_log("COMPLETE", f"Control gap assessment complete. {len(control_gaps)} gaps identified."))

    return {
        **state,
        "control_gaps": control_gaps,
        "control_framework_used": "DataSentinel Control Framework v1.0 (SR 11-7 / DAMA / BCBS 239 inspired)",
        "current_agent": "narrative_generator",
        "pipeline_status": "running",
        "audit_log": audit
    }
