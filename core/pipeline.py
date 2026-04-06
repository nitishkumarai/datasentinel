"""
DataSentinel — LangGraph Pipeline Orchestrator
Wires all 4 agents into a stateful LangGraph workflow with HITL checkpoint.

Graph structure:
  profiler → risk_classifier → [HITL checkpoint] → control_gap_assessor → narrative_generator

The HITL checkpoint pauses execution and waits for human review/approval
before proceeding to control gap assessment.
"""

from langgraph.graph import StateGraph, END
from langgraph.checkpoint.memory import MemorySaver

from core.state import DataSentinelState
from agents.agent1_profiler import run_profiler
from agents.agent2_classifier import run_risk_classifier
from agents.agent3_control_gap import run_control_gap_assessor
from agents.agent4_narrative import run_narrative_generator


# ── HITL node ─────────────────────────────────────────────────────────────────
def hitl_checkpoint(state: DataSentinelState) -> DataSentinelState:
    """
    Human-in-the-loop checkpoint node.
    In Streamlit mode: this node is reached, then execution pauses.
    The UI collects reviewer input, updates state, then resumes.
    In CLI mode: auto-approves with a note.
    """
    # This node just signals that we're awaiting review
    # The actual approval comes from the Streamlit UI updating the state
    return {
        **state,
        "pipeline_status": "awaiting_review",
        "current_agent": "hitl_checkpoint"
    }


def should_continue_after_hitl(state: DataSentinelState) -> str:
    """Conditional edge: proceed only if HITL approved."""
    if state.get("hitl_approved") is True:
        return "control_gap_assessor"
    return "hitl_checkpoint"  # loop back until approved


# ── Build the graph ───────────────────────────────────────────────────────────
def build_pipeline() -> StateGraph:
    """Construct and compile the DataSentinel LangGraph pipeline."""

    workflow = StateGraph(DataSentinelState)

    # Add agent nodes
    workflow.add_node("profiler", run_profiler)
    workflow.add_node("risk_classifier", run_risk_classifier)
    workflow.add_node("hitl_checkpoint", hitl_checkpoint)
    workflow.add_node("control_gap_assessor", run_control_gap_assessor)
    workflow.add_node("narrative_generator", run_narrative_generator)

    # Define edges
    workflow.set_entry_point("profiler")
    workflow.add_edge("profiler", "risk_classifier")
    workflow.add_edge("risk_classifier", "hitl_checkpoint")

    # Conditional edge after HITL
    workflow.add_conditional_edges(
        "hitl_checkpoint",
        should_continue_after_hitl,
        {
            "control_gap_assessor": "control_gap_assessor",
            "hitl_checkpoint": "hitl_checkpoint"
        }
    )

    workflow.add_edge("control_gap_assessor", "narrative_generator")
    workflow.add_edge("narrative_generator", END)

    # Memory saver enables state persistence between invocations (needed for HITL)
    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)


# ── Initial state builder ─────────────────────────────────────────────────────
def build_initial_state(
    dataset_name: str,
    dataset_description: str,
    raw_data_path: str
) -> DataSentinelState:
    """Create a clean initial pipeline state."""
    return DataSentinelState(
        dataset_name=dataset_name,
        dataset_description=dataset_description,
        raw_data_path=raw_data_path,
        profile_summary=None,
        anomalies_detected=[],
        risk_findings=[],
        overall_risk_score=None,
        overall_risk_tier=None,
        hitl_approved=None,
        hitl_notes=None,
        control_gaps=[],
        control_framework_used=None,
        executive_summary=None,
        risk_memo=None,
        risk_register=None,
        audit_log=[],
        current_agent="profiler",
        pipeline_status="running",
        error_message=None
    )


# ── CLI runner (for testing without Streamlit) ────────────────────────────────
if __name__ == "__main__":
    import json

    pipeline = build_pipeline()
    thread_config = {"configurable": {"thread_id": "cli-test-001"}}

    initial_state = build_initial_state(
        dataset_name="Customer Risk Dataset Q1 2025",
        dataset_description="Financial services customer dataset with complaints, credit, and transaction data",
        raw_data_path="data/customer_risk_dataset.csv"
    )

    print("🚀 DataSentinel Pipeline Starting...\n")

    # Run until HITL checkpoint
    print("▶ Running: Profiler → Risk Classifier → HITL Checkpoint")
    state = pipeline.invoke(initial_state, config=thread_config)

    print(f"\n⏸  HITL Checkpoint reached.")
    print(f"   Risk Score: {state['overall_risk_score']}/100 ({state['overall_risk_tier']})")
    print(f"   Findings: {len(state['risk_findings'])}")
    print(f"\n   [CLI MODE] Auto-approving and continuing...\n")

    # Simulate HITL approval
    state["hitl_approved"] = True
    state["hitl_notes"] = "CLI auto-approval for testing"

    # Resume pipeline
    print("▶ Running: Control Gap Assessor → Narrative Generator")
    final_state = pipeline.invoke(state, config=thread_config)

    print(f"\n✅ Pipeline Complete!")
    print(f"\n{'='*60}")
    print("EXECUTIVE SUMMARY")
    print('='*60)
    print(final_state["executive_summary"])
    print(f"\n   Risk Register: {len(final_state['risk_register'])} entries")
    print(f"   Audit Log: {len(final_state['audit_log'])} entries")
    print(f"\n   Full memo available in outputs/risk_memo.md")

    # Save outputs
    import os
    os.makedirs("outputs", exist_ok=True)
    with open("outputs/risk_memo.md", "w") as f:
        f.write(final_state["risk_memo"])
    with open("outputs/risk_register.json", "w") as f:
        json.dump(final_state["risk_register"], f, indent=2)
    with open("outputs/audit_log.json", "w") as f:
        json.dump(final_state["audit_log"], f, indent=2)

    print("\n📁 Outputs saved to outputs/")
