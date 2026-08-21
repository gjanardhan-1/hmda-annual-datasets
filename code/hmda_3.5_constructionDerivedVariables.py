#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

# =========================================================
# LOAD INPUT (output of step 3.4: analysis sample)
# =========================================================
sample = pd.read_parquet("I:/HMDA/analysis_sample.parquet")

print("Loaded sample shape:", sample.shape)

# =========================================================
# 3.5 DERIVED VARIABLES FOR ANALYSIS
# =========================================================

# --- Sanity checks before log/division transforms ---
print("\nAny zero/negative assets:", (sample["asset"] <= 0).sum())
print("Any zero/negative application_count:", (sample["application_count"] <= 0).sum())

sample["log_assets"]       = np.log(sample["asset"])
sample["log_applications"] = np.log(sample["application_count"])
sample["salary_to_assets"] = 100 * sample["esal"] / sample["asset"]

# Confirm this is the primary coverage measure you intend to use
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

print("\nexternal_aus_coverage distribution:")
print(sample["external_aus_coverage"].describe())
print("high_external_reliance value counts:")
print(sample["high_external_reliance"].value_counts())

# =========================================================
# SAVE OUTPUT
# =========================================================
sample.to_parquet("I:/HMDA/analysis_sample_derived.parquet", index=False)
sample.to_csv("I:/HMDA/analysis_sample_derived.csv", index=False)

print("\nSaved: I:/HMDA/analysis_sample_derived.parquet")
print("Saved: I:/HMDA/analysis_sample_derived.csv")

