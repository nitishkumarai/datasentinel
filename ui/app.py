"""
DataSentinel — Streamlit Application
Four-tab UI:
  Tab 1: Upload & Configure
  Tab 2: Risk Dashboard (findings + profiler stats)
  Tab 3: HITL Review (approve/override before control gap assessment)
  Tab 4: Risk Memo & Audit Log
"""

import streamlit as st
import pandas as pd
import json
import os
import sys
import tempfile
from datetime import datetime

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from core.pipeline import build_pipeline, build_initial_state
from core.state import DataSentinelState

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="DataSentinel",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ── Custom CSS ────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main-header {
        font-size: 2rem;
        font-weight: 700;
        color: #e2e8f0;
    }
    .risk-critical { background-color: #e53e3e; color: #ffffff; border-left: 6px solid #9b1c1c; padding: 12px 16px; border-radius: 6px; margin: 6px 0; }
    .risk-high     { background-color: #dd6b20; color: #ffffff; border-left: 6px solid #7b341e; padding: 12px 16px; border-radius: 6px; margin: 6px 0; }
    .risk-medium   { background-color: #d69e2e; color: #ffffff; border-left: 6px solid #744210; padding: 12px 16px; border-radius: 6px; margin: 6px 0; }
    .risk-low      { background-color: #38a169; color: #ffffff; border-left: 6px solid #1c4532; padding: 12px 16px; border-radius: 6px; margin: 6px 0; }
    .metric-card   { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 16px; text-align: center; }
    .audit-entry   { font-family: monospace; font-size: 0.8rem; color: #4a5568; padding: 2px 0; }
    .stTabs [data-baseweb="tab"] { font-size: 0.95rem; font-weight: 500; }
</style>
""", unsafe_allow_html=True)


SEVERITY_EMOJI = {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢", "INFO": "⚪"}
SEVERITY_COLOR = {"CRITICAL": "risk-critical", "HIGH": "risk-high", "MEDIUM": "risk-medium", "LOW": "risk-low"}

# ── Session state init ────────────────────────────────────────────────────────
if "pipeline_state" not in st.session_state:
    st.session_state.pipeline_state = None
if "pipeline" not in st.session_state:
    st.session_state.pipeline = build_pipeline()
if "thread_id" not in st.session_state:
    st.session_state.thread_id = f"session-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 🛡️ DataSentinel")
    st.markdown("*AI-Powered Data Risk Intelligence*")
    st.divider()

    if "GROQ_API_KEY" in st.secrets:
        os.environ["GROQ_API_KEY"] = st.secrets["GROQ_API_KEY"]

    if st.session_state.pipeline_state:
        ps = st.session_state.pipeline_state
        st.markdown("### Pipeline Status")
        status = ps.get("pipeline_status", "—")
        status_icon = {"running": "🔄", "awaiting_review": "⏸️",
                       "complete": "✅", "failed": "❌"}.get(status, "—")
        st.markdown(f"{status_icon} **{status.replace('_', ' ').title()}**")

        if ps.get("overall_risk_score") is not None:
            score = ps["overall_risk_score"]
            tier = ps.get("overall_risk_tier", "")
            emoji = SEVERITY_EMOJI.get(tier, "")
            st.markdown(f"### Risk Score")
            st.markdown(f"## {emoji} {score}/100")
            st.markdown(f"**{tier}** tier")

    st.divider()
    if st.button("🔄 Reset Pipeline", use_container_width=True):
        st.session_state.pipeline_state = None
        st.session_state.thread_id = f"session-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"
        st.rerun()


# ── Main header ───────────────────────────────────────────────────────────────
st.markdown('<div class="main-header">🛡️ DataSentinel</div>', unsafe_allow_html=True)
st.markdown("*AI-Powered Data Risk Intelligence — Built on LangGraph*")
st.divider()

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs([
    "📁 Upload & Configure",
    "📊 Risk Dashboard",
    "✅ HITL Review",
    "📄 Risk Memo & Audit"
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1: Upload & Configure
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    st.markdown("### Configure Dataset for Risk Assessment")

    col1, col2 = st.columns([2, 1])

    with col1:
        dataset_name = st.text_input(
            "Dataset Name",
            value="Customer Risk Dataset Q1 2025",
            help="A descriptive name for audit and reporting purposes"
        )
        dataset_description = st.text_area(
            "Dataset Description",
            value="Financial services customer dataset containing complaints, credit scores, transaction amounts, and account data. Used for analytics and risk monitoring.",
            height=100
        )

    with col2:
        st.markdown("#### Quick Start")
        st.info("💡 Use the **synthetic dataset** to try the full pipeline with pre-seeded issues.")

        use_synthetic = st.checkbox("Use synthetic dataset", value=True)

    st.divider()

    if use_synthetic:
        st.markdown("#### Synthetic Dataset")
        st.markdown("The synthetic dataset simulates a real financial services data asset with seeded issues:")

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            st.markdown("**🔴 Quality Issues**")
            st.markdown("- 8% null credit scores\n- 3% duplicate customer IDs\n- 4% outlier transactions\n- 5% invalid status values")
        with col_b:
            st.markdown("**🟠 Sensitivity Issues**")
            st.markdown("- SSN column (unmasked)\n- Email column (unmasked)\n- No data classification")
        with col_c:
            st.markdown("**🟡 Lineage / Regulatory**")
            st.markdown("- Inconsistent source labels\n- 10% missing extraction dates\n- Consent violations (~6%)")

        # Generate the synthetic dataset
        generate_col, _ = st.columns([1, 2])
        with generate_col:
            if st.button("⚡ Generate & Run Pipeline", type="primary", use_container_width=True):
                with st.spinner("Generating synthetic dataset..."):
                    import sys
                    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
                    from data.generate_synthetic_data import generate_dataset
                    import tempfile
                    df = generate_dataset()
                    tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False)
                    df.to_csv(tmp.name, index=False)
                    data_path = tmp.name

                initial_state = build_initial_state(dataset_name, dataset_description, data_path)

                with st.spinner("Running Profiler..."):
                    from agents.agent1_profiler import run_profiler
                    state = run_profiler(initial_state)

                with st.spinner("Running Risk Classifier..."):
                    from agents.agent2_classifier import run_risk_classifier
                    state = run_risk_classifier(state)

                st.session_state.pipeline_state = state

                st.success(f"✅ Pipeline paused at HITL checkpoint. {len(state['risk_findings'])} findings detected.")
                st.info("👉 Go to **Risk Dashboard** to review findings, then **HITL Review** to approve.")

    else:
        uploaded_file = st.file_uploader("Upload CSV dataset", type=["csv"])

        if uploaded_file and st.button("▶ Run Pipeline", type="primary"):
            with tempfile.NamedTemporaryFile(mode='wb', suffix='.csv', delete=False) as tmp:
                tmp.write(uploaded_file.read())
                tmp_path = tmp.name

            initial_state = build_initial_state(dataset_name, dataset_description, tmp_path)

            with st.spinner("Running Profiler → Risk Classifier..."):
                state = st.session_state.pipeline.invoke(
                    initial_state,
                    config={"configurable": {"thread_id": st.session_state.thread_id}}
                )
                st.session_state.pipeline_state = state

            st.success(f"✅ Paused at HITL checkpoint. {len(state['risk_findings'])} findings.")
            st.info("👉 Go to **Risk Dashboard** → **HITL Review**")


# ════════════════════════════════════════════════════════════════════════════
# TAB 2: Risk Dashboard
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    ps = st.session_state.pipeline_state

    if not ps:
        st.info("Run the pipeline in **Upload & Configure** to see results here.")
    else:
        # ── KPI row ──────────────────────────────────────────────────────────
        kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)

        findings = ps.get("risk_findings", []) or []
        profile = ps.get("profile_summary", {}) or {}
        severity_counts = {}
        for f in findings:
            severity_counts[f["severity"]] = severity_counts.get(f["severity"], 0) + 1

        with kpi1:
            st.metric("Risk Score", f"{ps.get('overall_risk_score', '—')}/100",
                    delta=ps.get("overall_risk_tier", ""))
        with kpi2:
            st.metric("Total Findings", len(findings))
        with kpi3:
            st.metric("🔴 Critical", severity_counts.get("CRITICAL", 0))
        with kpi4:
            st.metric("🟠 High", severity_counts.get("HIGH", 0))
        with kpi5:
            st.metric("Dataset Rows", profile.get("n_rows", "—") if profile else "—")

        st.divider()

        col_left, col_right = st.columns([3, 2])

        with col_left:
            st.markdown("### Risk Findings")

            dim_filter = st.multiselect(
                "Filter by dimension",
                ["quality", "sensitivity", "lineage", "regulatory"],
                default=["quality", "sensitivity", "lineage", "regulatory"]
            )
            sev_filter = st.multiselect(
                "Filter by severity",
                ["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                default=["CRITICAL", "HIGH", "MEDIUM", "LOW"]
            )

            filtered = [f for f in findings
                        if f["dimension"] in dim_filter and f["severity"] in sev_filter]

            for f in filtered:
                css_class = SEVERITY_COLOR.get(f["severity"], "")
                emoji = SEVERITY_EMOJI.get(f["severity"], "")
                st.markdown(f"""
<div class="{css_class}">
  <strong>{emoji} [{f['severity']}] {f['dimension'].upper()} — {f['field']}</strong><br>
  {f['description']}<br>
  <small><em>Evidence: {f['evidence'][:120]}...</em></small>
</div>
""", unsafe_allow_html=True)

        with col_right:
            st.markdown("### Profiler Summary")

            if profile:
                st.markdown(f"**Rows:** {profile.get('n_rows', '—')} | **Columns:** {profile.get('n_cols', '—')}")

                pii = profile.get("pii_columns", [])
                if pii:
                    st.warning(f"🔐 PII Columns Detected: `{'`, `'.join(pii)}`")

                high_nulls = profile.get("high_null_columns", {})
                if high_nulls:
                    st.markdown("**High Null Rate Columns:**")
                    null_df = pd.DataFrame([
                        {"Column": col, "Null %": info["null_pct"]}
                        for col, info in high_nulls.items()
                    ])
                    st.dataframe(null_df, use_container_width=True, hide_index=True)

                dups = profile.get("duplicate_keys", {})
                if dups:
                    st.markdown("**Duplicate Keys:**")
                    for col, info in dups.items():
                        st.markdown(f"- `{col}`: {info['duplicate_count']} duplicates ({info['pct']}%)")

                outliers = profile.get("outliers", {})
                if outliers:
                    st.markdown("**Outlier Columns:**")
                    for col, info in outliers.items():
                        st.markdown(f"- `{col}`: {info['count']} outliers ({info['pct']}%)")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3: HITL Review
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    ps = st.session_state.pipeline_state

    if not ps:
        st.info("Run the pipeline first.")
    elif ps.get("pipeline_status") == "complete":
        st.success("✅ Review complete. Pipeline finished. See **Risk Memo & Audit** tab.")
    elif ps.get("pipeline_status") == "awaiting_review":
        st.markdown("### 🔍 Human-in-the-Loop Review")
        st.markdown(
            "Review the risk findings below. You may override any classification before "
            "approving and proceeding to control gap assessment."
        )
        st.warning(f"**Overall Risk:** {ps.get('overall_risk_score')}/100 — {ps.get('overall_risk_tier')}")

        findings = ps.get("risk_findings", [])
        updated_findings = []

        st.markdown("#### Finding Review")
        for i, f in enumerate(findings):
            with st.expander(
                f"{SEVERITY_EMOJI.get(f['severity'], '')} [{f['severity']}] {f['field']} — {f['dimension'].upper()}",
                expanded=(f["severity"] in ["CRITICAL", "HIGH"])
            ):
                st.markdown(f"**Description:** {f['description']}")
                st.markdown(f"**Evidence:** {f['evidence']}")

                override_options = ["— Keep as classified —", "CRITICAL", "HIGH", "MEDIUM", "LOW", "INFO", "DISMISS"]
                override = st.selectbox(
                    "Override severity?",
                    override_options,
                    key=f"override_{i}"
                )

                updated_f = dict(f)
                if override != "— Keep as classified —":
                    updated_f["reviewer_override"] = override
                updated_findings.append(updated_f)

        st.divider()
        reviewer_notes = st.text_area(
            "Reviewer Notes (optional)",
            placeholder="Add any context, exceptions, or escalation notes here..."
        )

        col_approve, col_reject = st.columns([1, 3])
        with col_approve:
            if st.button("✅ Approve & Continue", type="primary", use_container_width=True):
                updated_state = {
                    **ps,
                    "risk_findings": updated_findings,
                    "hitl_approved": True,
                    "hitl_notes": reviewer_notes or "Approved by reviewer",
                    "pipeline_status": "running"
                }

                with st.spinner("Running Control Gap Assessor..."):
                    from agents.agent3_control_gap import run_control_gap_assessor
                    final_state = run_control_gap_assessor(updated_state)

                with st.spinner("Generating Risk Memo..."):
                    from agents.agent4_narrative import run_narrative_generator
                    final_state = run_narrative_generator(final_state)

                st.session_state.pipeline_state = final_state
                st.success("✅ Pipeline complete! Go to **Risk Memo & Audit** to view the output.")
                st.rerun()
    else:
        st.info(f"Pipeline status: {ps.get('pipeline_status', '—')}")


# ════════════════════════════════════════════════════════════════════════════
# TAB 4: Risk Memo & Audit Log
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    ps = st.session_state.pipeline_state

    if not ps or ps.get("pipeline_status") != "complete":
        st.info("Complete the pipeline and HITL review to view the risk memo.")
    else:
        st.markdown("### Executive Summary")
        st.info(ps.get("executive_summary", "—"))

        st.divider()
        st.markdown("### Risk Register")

        register = ps.get("risk_register", [])
        if register:
            reg_df = pd.DataFrame([{
                "ID": r["id"],
                "Type": r["type"],
                "Dimension": r["dimension"],
                "Field": r["field"],
                "Severity": f"{r['emoji']} {r['severity']}",
                "Description": r["description"][:80] + "..." if len(r["description"]) > 80 else r["description"],
            } for r in register])
            st.dataframe(reg_df, use_container_width=True, hide_index=True)

            # Download
            st.download_button(
                "⬇️ Download Risk Register (JSON)",
                data=json.dumps(register, indent=2),
                file_name="risk_register.json",
                mime="application/json"
            )

        st.divider()
        st.markdown("### Full Risk Memo")

        memo = ps.get("risk_memo", "")
        if memo:
            st.markdown(memo)
            st.download_button(
                "⬇️ Download Risk Memo (Markdown)",
                data=memo,
                file_name="risk_memo.md",
                mime="text/markdown"
            )

        st.divider()
        st.markdown("### Audit Log")
        st.markdown("*Full decision trail across all pipeline agents*")

        audit_log = ps.get("audit_log", [])
        for entry in audit_log:
            st.markdown(
                f'<div class="audit-entry">'
                f'<strong>{entry["timestamp"][:19]}</strong> | '
                f'<strong>[{entry["agent"]}]</strong> '
                f'{entry["action"]}: {entry["detail"]}'
                f'</div>',
                unsafe_allow_html=True
            )

        st.download_button(
            "⬇️ Download Audit Log (JSON)",
            data=json.dumps(audit_log, indent=2),
            file_name="audit_log.json",
            mime="application/json"
        )
