#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

# =========================================================
# LOAD INPUT (output of step 3.2: full linked file)
# =========================================================
linked = pd.read_parquet("I:/HMDA/linked_final.parquet")

print("Loaded linked shape:", linked.shape)
print("Loaded linked columns:", linked.columns.tolist())

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

# =========================================================
# 3.5 DERIVED VARIABLES FOR ANALYSIS
# =========================================================

print("\nAny zero/negative assets:", (sample["asset"] <= 0).sum())
print("Any zero/negative application_count:", (sample["application_count"] <= 0).sum())

sample["log_assets"]       = np.log(sample["asset"])
sample["log_applications"] = np.log(sample["application_count"])
sample["salary_to_assets"] = 100 * sample["esal"] / sample["asset"]
sample["high_external_reliance"] = sample["external_aus_coverage"].ge(0.70)
sample["asset_quartile"] = pd.qcut(
    sample["asset"], 4,
    labels=["Q1 smallest", "Q2", "Q3", "Q4 largest"]
)

# =========================================================
# CHECK RESULTS
# =========================================================
print("\n--- Derived variable summary ---")
print(sample[[
    "log_assets", "log_applications", "salary_to_assets",
    "high_external_reliance", "asset_quartile"
]].describe(include="all"))

print("\nAny inf/NaN in log_assets:", np.isinf(sample["log_assets"]).sum(), "/", sample["log_assets"].isna().sum())
print("Any inf/NaN in log_applications:", np.isinf(sample["log_applications"]).sum(), "/", sample["log_applications"].isna().sum())
print("Any inf/NaN in salary_to_assets:", np.isinf(sample["salary_to_assets"]).sum(), "/", sample["salary_to_assets"].isna().sum())

# =========================================================
# SAVE OUTPUT
# =========================================================
sample.to_parquet("I:/HMDA/analysis_sample_derived.parquet", index=False)
sample.to_csv("I:/HMDA/analysis_sample_derived.csv", index=False)

print("\nSaved: I:/HMDA/analysis_sample_derived.parquet")
print("Saved: I:/HMDA/analysis_sample_derived.csv")

