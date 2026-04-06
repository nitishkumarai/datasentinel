# DataSentinel Control Framework v1.0
## Data Risk Controls Reference

Inspired by SR 11-7, DAMA-DMBOK, and BCBS 239 principles.
This document serves as the RAG knowledge base for the Control Gap Assessor agent.

---

## DATA QUALITY CONTROLS

### DQ-01: Data Completeness Monitoring
**Dimension:** Quality
**Description:** Automated monitoring of null/missing rates per field with defined breach thresholds. Alerts trigger when null rate exceeds 5% for critical fields or 10% for non-critical fields. Monthly attestation by data steward required.
**Evidence of effectiveness:** Monitoring dashboard with threshold breach log, steward sign-off record.

### DQ-02: Duplicate Key Prevention
**Dimension:** Quality
**Description:** Primary key uniqueness constraint enforced at data ingestion layer. Automated duplicate detection job runs daily and blocks non-unique records from landing in downstream analytics tables. Duplicate counts tracked in data quality scorecard.
**Evidence of effectiveness:** Ingestion rejection log, daily duplicate scan results.

### DQ-03: Outlier Detection and Review
**Dimension:** Quality
**Description:** Statistical outlier detection using IQR (interquartile range) or Z-score methodology applied to all numeric fields. Records beyond 3 standard deviations are flagged and routed to a manual review queue. Reviewed within 5 business days.
**Evidence of effectiveness:** Outlier flag log, review queue completion rate.

### DQ-04: Categorical Value Standardization
**Dimension:** Quality
**Description:** Controlled vocabulary / reference data management enforced for all categorical fields. Ingestion pipelines reject non-standard values against a canonical reference table. Reference table maintained by data governance team and updated quarterly.
**Evidence of effectiveness:** Rejection log, reference table version history.

---

## DATA SENSITIVITY CONTROLS

### SE-01: PII Data Masking and Tokenization
**Dimension:** Sensitivity
**Description:** All Personally Identifiable Information (PII) fields — including SSN, email, phone number, date of birth, and financial account numbers — must be masked or tokenized before landing in analytics or reporting environments. Clear-text PII is only permitted in approved source systems with restricted access. Masking validated at ingestion by automated scanner.
**Evidence of effectiveness:** Masking validation scan results, PII inventory register.

### SE-02: Data Classification and Tagging
**Dimension:** Sensitivity
**Description:** Automated data classification engine tags all columns with sensitivity level: Public, Internal, Confidential, or Restricted. Restricted and Confidential data triggers mandatory access review. Classification taxonomy reviewed annually. Untagged datasets are blocked from production use.
**Evidence of effectiveness:** Classification tag coverage report, access review completion log.

### SE-03: Access Control and Role-Based Permissions
**Dimension:** Sensitivity
**Description:** Role-based access control (RBAC) applied to all data assets. Sensitive fields (Restricted / Confidential) require data steward approval and are subject to quarterly access review. Privileged access logged and audited. Least-privilege principle enforced.
**Evidence of effectiveness:** Access provisioning log, quarterly review sign-off, RBAC audit report.

---

## DATA LINEAGE CONTROLS

### LN-01: Data Lineage Tracking
**Dimension:** Lineage
**Description:** End-to-end lineage metadata captured for all datasets: source system identifier, extraction timestamp, transformation steps, load timestamp, and downstream consumption log. Lineage metadata stored in central catalogue and queryable. Critical datasets must have documented lineage before use in models or regulatory reporting.
**Evidence of effectiveness:** Lineage catalogue completeness report, model documentation attestation.

### LN-02: Source System Standardization
**Dimension:** Lineage
**Description:** Canonical source system registry maintained by data engineering. All ingestion pipelines must reference an approved system identifier from the registry. Free-text or ad-hoc source labels are rejected at ingestion. Registry updated via formal change management process.
**Evidence of effectiveness:** Registry coverage report, ingestion rejection log for non-standard labels.

### LN-03: Data Freshness Monitoring
**Dimension:** Lineage
**Description:** Extraction timestamp present and validated on every record at ingestion. Missing or null extraction timestamps trigger pipeline alert and block downstream load. Freshness SLA defined per dataset (e.g., daily refresh datasets must have timestamps within 26 hours). Staleness breaches tracked in operational dashboard.
**Evidence of effectiveness:** Freshness monitoring dashboard, SLA breach log.

---

## REGULATORY CONTROLS

### RG-01: Consent Management and Verification
**Dimension:** Regulatory
**Description:** Customer consent flag validated before any marketing, outreach, or data sharing action. Records with consent_flag = 'N' are automatically blocked from marketing workflows via a pre-send consent check. Consent records maintained with timestamp and source. Consent validation logic reviewed quarterly by Legal and Compliance. Violations trigger mandatory escalation.
**Evidence of effectiveness:** Consent validation run log, marketing suppression confirmation, quarterly review sign-off.

### RG-02: FCRA / CCPA / DPDP Scope Compliance
**Dimension:** Regulatory
**Description:** Geographic and jurisdictional scope of data usage validated against applicable regulations (FCRA for US credit data, CCPA for California residents, DPDP Act for India-domiciled data). Records from territories with distinct regulatory treatment flagged for review before use in credit decisions or cross-border transfers. Regulatory scope matrix maintained by Compliance.
**Evidence of effectiveness:** Scope validation run results, regulatory matrix version, cross-border transfer log.

### RG-03: Data Retention and Deletion Controls
**Dimension:** Regulatory
**Description:** Retention schedule enforced per data classification and regulatory requirement. Automated deletion jobs execute at defined intervals. Deletion audit trail maintained for 7 years. Right-to-erasure (DPDP / CCPA) requests processed within 30 days. Retention compliance attested quarterly.
**Evidence of effectiveness:** Deletion job completion log, erasure request tracker, quarterly attestation.

### RG-04: Model and Analytics Governance (SR 11-7)
**Dimension:** Regulatory
**Description:** All models and analytical tools consuming this dataset must be registered in the Model Risk Management (MRM) inventory. Data quality attestation for the input dataset is required before model development commences and before production deployment. Model documentation must reference data lineage and quality scores. MRM team reviews attestation as part of model approval.
**Evidence of effectiveness:** MRM inventory registration, data quality attestation sign-off, model approval record.
