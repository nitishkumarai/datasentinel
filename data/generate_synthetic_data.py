"""
DataSentinel — Synthetic Dataset Generator
Generates a realistic financial services customer data asset
with seeded data quality, sensitivity, lineage, and regulatory issues.

Run directly:  python generate_synthetic_data.py
"""

import pandas as pd
import numpy as np
import random
import os
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

N = 500  # rows


def random_date(start_year=2022, end_year=2024):
    start = datetime(start_year, 1, 1)
    end = datetime(end_year, 12, 31)
    return start + timedelta(days=random.randint(0, (end - start).days))


def generate_dataset():
    """
    Simulates a Customer Complaints & Account Risk dataset.
    Intentionally seeds the following issues for agents to detect:

    DATA QUALITY ISSUES
    - ~8% null rate in credit_score (missing values)
    - ~3% duplicate customer_id (duplicate keys)
    - ~5% invalid values in complaint_status (schema drift)
    - ~4% outlier transaction amounts (> $50,000)

    SENSITIVITY / PII ISSUES
    - ssn column present (high-risk PII)
    - email column present (moderate PII)
    - No masking applied

    LINEAGE ISSUES
    - source_system has 3 inconsistent labels for same system
    - data_extraction_date missing for ~10% of rows

    REGULATORY ISSUES
    - ~6% of records have consent_flag = 'N' but have active marketing
    - state column includes territories not covered by FCRA scope notes
    """

    customer_ids = [f"CUST{str(i).zfill(5)}" for i in range(1, N + 1)]

    # Seed ~3% duplicates
    dup_indices = random.sample(range(N), int(N * 0.03))
    for idx in dup_indices:
        customer_ids[idx] = customer_ids[random.randint(0, idx - 1)] if idx > 0 else customer_ids[0]

    # Credit scores with ~8% nulls
    credit_scores = [random.randint(300, 850) for _ in range(N)]
    null_indices = random.sample(range(N), int(N * 0.08))
    for idx in null_indices:
        credit_scores[idx] = None

    # Transaction amounts with ~4% outliers
    transaction_amounts = [round(random.uniform(10, 5000), 2) for _ in range(N)]
    outlier_indices = random.sample(range(N), int(N * 0.04))
    for idx in outlier_indices:
        transaction_amounts[idx] = round(random.uniform(55000, 250000), 2)

    # Complaint status with ~5% invalid values
    valid_statuses = ["Open", "Closed", "Pending", "Escalated"]
    invalid_statuses = ["OPEN", "closed_v2", "--", "TBD"]
    complaint_statuses = []
    for i in range(N):
        if i in random.sample(range(N), int(N * 0.05)):
            complaint_statuses.append(random.choice(invalid_statuses))
        else:
            complaint_statuses.append(random.choice(valid_statuses))

    # Source system with inconsistent labels
    source_systems = random.choices(
        ["CRM_v2", "CRM-V2", "crm_v2", "LOS", "ServicingPlatform", "LOS_Legacy"],
        weights=[30, 10, 10, 20, 20, 10],
        k=N
    )

    # Data extraction date with ~10% missing
    extraction_dates = [random_date().strftime("%Y-%m-%d") for _ in range(N)]
    missing_date_indices = random.sample(range(N), int(N * 0.10))
    for idx in missing_date_indices:
        extraction_dates[idx] = None

    # Consent flag with ~6% non-consented but active marketing
    consent_flags = ["Y"] * N
    non_consent_indices = random.sample(range(N), int(N * 0.06))
    for idx in non_consent_indices:
        consent_flags[idx] = "N"

    marketing_active = ["N"] * N
    for idx in range(N):
        if consent_flags[idx] == "N" and random.random() < 0.8:
            marketing_active[idx] = "Y"  # violation
        elif consent_flags[idx] == "Y":
            marketing_active[idx] = random.choice(["Y", "N"])

    # SSN — high-risk PII (should never appear in analytics datasets)
    ssns = [f"{random.randint(100,999)}-{random.randint(10,99)}-{random.randint(1000,9999)}" for _ in range(N)]

    # Email
    domains = ["gmail.com", "yahoo.com", "outlook.com", "amex.com"]
    emails = [f"user{i}@{random.choice(domains)}" for i in range(N)]

    # States
    states = random.choices(
        ["CA", "NY", "TX", "FL", "IL", "PR", "GU", "VI", "OH", "GA"],
        weights=[20, 18, 15, 12, 8, 5, 4, 3, 8, 7],
        k=N
    )

    df = pd.DataFrame({
        "customer_id": customer_ids,
        "ssn": ssns,
        "email": emails,
        "state": states,
        "credit_score": credit_scores,
        "transaction_amount_usd": transaction_amounts,
        "complaint_status": complaint_statuses,
        "consent_flag": consent_flags,
        "marketing_active": marketing_active,
        "source_system": source_systems,
        "data_extraction_date": extraction_dates,
        "account_open_date": [random_date(2018, 2023).strftime("%Y-%m-%d") for _ in range(N)],
        "product_type": random.choices(["Credit Card", "Personal Loan", "Savings", "Checking"], k=N),
        "complaint_category": random.choices(
            ["Billing", "Fraud", "Customer Service", "Rewards", "Fees", "Other"], k=N
        ),
        "resolution_days": [random.randint(1, 90) if random.random() > 0.05 else None for _ in range(N)],
    })

    return df


if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    df = generate_dataset()
    output_path = "data/customer_risk_dataset.csv"
    df.to_csv(output_path, index=False)
    print(f"✅ Synthetic dataset generated: {output_path}")
    print(f"   Rows: {len(df)} | Columns: {len(df.columns)}")
    print(f"\nColumns: {list(df.columns)}")
    print(f"\nSeeded issues:")
    print(f"  • Null credit_score: {df['credit_score'].isna().sum()} rows")
    print(f"  • Duplicate customer_id: {df['customer_id'].duplicated().sum()} rows")
    print(f"  • Outlier transactions (>$50K): {(df['transaction_amount_usd'] > 50000).sum()} rows")
    print(f"  • Consent violation (N + marketing Y): {((df['consent_flag']=='N') & (df['marketing_active']=='Y')).sum()} rows")
    print(f"  • Missing extraction dates: {df['data_extraction_date'].isna().sum()} rows")
    print(f"  • SSN column present: YES (PII exposure)")
