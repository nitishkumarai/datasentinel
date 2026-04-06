# 🛡️ DataSentinel

**AI-Powered Data Risk Intelligence Agent**

An agentic pipeline that monitors data assets for quality, lineage gaps, sensitivity exposure, and regulatory risk — turning raw data signals into structured risk intelligence with a human-in-the-loop governance layer.

Built with **LangGraph** · **Groq/Llama 3.3 70B** · **FAISS RAG** · **Streamlit**

---

## What It Does

DataSentinel runs a 4-agent LangGraph pipeline on any tabular dataset:

```
[Data Asset]
     │
     ▼
┌─────────────┐
│  Agent 1    │  Profiler — schema analysis, null rates, outlier detection,
│  Profiler   │  duplicate keys, PII heuristics, category inconsistencies
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Agent 2    │  Risk Classifier — maps anomalies to risk dimensions
│  Classifier │  (quality / sensitivity / lineage / regulatory)
└──────┬──────┘  scores severity (CRITICAL → INFO) using LLM
       │
       ▼
┌─────────────┐
│  ⏸ HITL    │  Human-in-the-Loop — reviewer can inspect, override, and
│  Checkpoint │  approve findings before pipeline continues
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Agent 3    │  Control Gap Assessor — RAG over SR 11-7 / DAMA / BCBS 239
│  Gap Assess │  inspired control framework; identifies missing/weak controls
└──────┬──────┘
       │
       ▼
┌─────────────┐
│  Agent 4    │  Narrative Generator — executive summary, risk memo,
│  Narrative  │  structured risk register, downloadable outputs
└─────────────┘
```

---

## Key Features

- **Multi-agent LangGraph pipeline** with stateful execution
- **Human-in-the-loop checkpoint** — reviewer approves before control gap assessment proceeds
- **LLM-powered classification** using Groq/Llama 3.3 70B with rule-based fallback
- **RAG over control framework** using FAISS + sentence-transformers
- **Full audit trail** — every agent decision is timestamped and logged
- **Synthetic dataset** with seeded issues for demo/testing
- **Streamlit UI** with 4 tabs: upload, dashboard, HITL review, risk memo

---

## Risk Dimensions

| Dimension | What It Covers |
|-----------|----------------|
| **Quality** | Nulls, duplicates, outliers, invalid/inconsistent values |
| **Sensitivity** | PII exposure, unmasked identifiers, classification gaps |
| **Lineage** | Source system inconsistency, missing extraction timestamps |
| **Regulatory** | Consent violations, FCRA scope issues, UDAAP signals |

---

## Quickstart

### 1. Clone and install

```bash
git clone https://github.com/yourusername/datasentinel.git
cd datasentinel
pip install -r requirements.txt
```

### 2. Set your Groq API key

```bash
export GROQ_API_KEY=your_key_here
# or enter it in the Streamlit sidebar
```

Get a free key at [console.groq.com](https://console.groq.com)

### 3. Run the Streamlit app

```bash
streamlit run ui/app.py
```

### 4. Or run from CLI

```bash
cd datasentinel
python data/generate_synthetic_data.py  # generate synthetic dataset
python core/pipeline.py                  # run full pipeline (CLI mode)
```

---

## Project Structure

```
datasentinel/
├── agents/
│   ├── agent1_profiler.py       # Data profiling and anomaly detection
│   ├── agent2_classifier.py     # LLM-powered risk classification
│   ├── agent3_control_gap.py    # RAG-based control gap assessment
│   └── agent4_narrative.py      # Risk memo and register generation
├── core/
│   ├── state.py                 # LangGraph state schema (TypedDict)
│   └── pipeline.py              # Graph construction and orchestration
├── data/
│   └── generate_synthetic_data.py  # Synthetic financial dataset generator
├── ui/
│   └── app.py                   # Streamlit application
├── outputs/                     # Generated memos, registers, audit logs
├── requirements.txt
└── README.md
```

---

## Synthetic Dataset

The included generator creates a 500-row financial services dataset with intentionally seeded issues:

| Issue Type | Detail |
|-----------|--------|
| Null credit scores | ~8% missing |
| Duplicate customer IDs | ~3% duplicates |
| Outlier transactions | ~4% > $50,000 |
| Invalid status values | ~5% non-standard labels |
| SSN column | Unmasked PII |
| Inconsistent source labels | 3 variants of same system |
| Missing extraction dates | ~10% null |
| Consent violations | ~6% marketing to non-consented records |

---

## Architecture Decisions

**Why LangGraph?**
State persistence between agents is essential for the HITL pattern. LangGraph's `MemorySaver` enables the pipeline to pause at the checkpoint and resume after reviewer approval without losing intermediate state.

**Why rule-based fallback?**
The pipeline is designed to work even without an API key. Every LLM call has a deterministic fallback, making it demo-safe and testable offline.

**Why RAG for control gaps?**
As the control framework grows, FAISS-based retrieval ensures only the most relevant controls are passed to the LLM — keeping prompts focused and reducing hallucination risk. This mirrors SR 11-7's expectation of documented, retrievable control evidence.

**Why an audit log?**
Every agent action is timestamped in the state. This is not just a feature — it's a design principle. In a 2LoD function, explainability and traceability of AI decisions are non-negotiable.

---

## Roadmap (Weeks 1-4)

- [x] **Week 1** — Data Profiler + Streamlit shell + synthetic dataset
- [x] **Week 2** — Risk Classifier with LLM + rule-based fallback
- [x] **Week 3** — Control Gap Assessor with FAISS RAG + HITL checkpoint
- [x] **Week 4** — Narrative Generator + audit log + UI polish + GitHub publish

---

## Use Case Narrative

> *"DataSentinel operationalises data risk management as a continuous, explainable intelligence pipeline. The architecture mirrors how a second line of defence function thinks: profile → classify → assess controls → escalate. The human-in-the-loop design reflects real governance constraints — AI surfaces the risk, humans remain accountable for the decision."*

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Agent orchestration | LangGraph 0.2+ |
| LLM | Groq / Llama 3.3 70B |
| Embeddings | sentence-transformers (all-MiniLM-L6-v2) |
| Vector search | FAISS |
| UI | Streamlit |
| Data processing | Pandas, NumPy |
| State management | TypedDict + LangGraph MemorySaver |

---

## Author

Built by Nitish | Senior Manager, Complaints Analytics & Control Management

*Portfolio project demonstrating applied AI in data risk management.*
