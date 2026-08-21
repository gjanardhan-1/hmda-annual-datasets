#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd

# Load the corrected, FDIC-merged linked file
linked = pd.read_parquet("I:/HMDA/linked_final.parquet")

# =========================================================
# 3.3 AUDIT THE MATCH — per-year match-rate table
# =========================================================
audit_table = linked.groupby("year").agg(
    n_lender_years=("lei", "count"),
    n_valid_fdic_cert=("valid_fdic_cert", "sum"),
    n_matched_financials=("matched_fdic_financials", "sum"),
).reset_index()

audit_table["pct_valid_cert"] = (audit_table["n_valid_fdic_cert"] / audit_table["n_lender_years"] * 100).round(2)
audit_table["pct_matched_financials"] = (audit_table["n_matched_financials"] / audit_table["n_lender_years"] * 100).round(2)

print(audit_table)
audit_table.to_csv("I:/HMDA/audit_match_rate_by_year.csv", index=False)

# =========================================================
# 3.4 BUILD THE ANALYSIS SAMPLE
# =========================================================
MIN_APPLICATIONS = 500

sample = linked.loc[
    linked["matched_fdic_financials"] & (linked["application_count"] >= MIN_APPLICATIONS)
].copy()

sample = sample.loc[sample["year"].between(2023, 2025)].copy()

print("\nFull linked rows:", len(linked))
print("Sample rows (matched + application_count >= 500 + years 2023-2025):", len(sample))
print(sample["year"].value_counts().sort_index())

sample.to_parquet("I:/HMDA/analysis_sample.parquet", index=False)
sample.to_csv("I:/HMDA/analysis_sample.csv", index=False)

print("\nSaved: I:/HMDA/analysis_sample.parquet")
print("Saved: I:/HMDA/analysis_sample.csv")


# In[ ]:




