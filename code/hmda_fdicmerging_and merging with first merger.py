#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import glob
import os
import re

# Load the output from the previous step
linked = pd.read_parquet("I:/HMDA/linked_lender_year.parquet")

print("Loaded linked shape:", linked.shape)
print("Loaded linked columns:", linked.columns.tolist())
# =========================================================
# 8. LOAD & COMBINE 8 YEARLY FDIC FINANCIALS CSV FILES
# =========================================================

fdic_folder = "I:/HMDA"
csv_files = sorted(glob.glob(os.path.join(fdic_folder, "fdic_*_q4.csv")))

print(f"Found {len(csv_files)} files:")
for f in csv_files:
    print(" -", f)

fdic_list = []
for f in csv_files:
    df = pd.read_csv(f, low_memory=False)
    df.columns = df.columns.str.strip().str.lower()

    # Extract year from filename, e.g. "fdic_2018_q4.csv" -> 2018
    match = re.search(r"fdic_(\d{4})_q4", os.path.basename(f))
    if not match:
        raise ValueError(f"Could not determine year for file: {f}")
    df["year"] = int(match.group(1))

    fdic_list.append(df)

fdic_financials = pd.concat(fdic_list, ignore_index=True)

print("\nCombined FDIC financials shape:", fdic_financials.shape)
print("Columns:", fdic_financials.columns.tolist())
print("Years present:", sorted(fdic_financials["year"].unique()))

# =========================================================
# 9. NORMALIZE KEY COLUMNS
# =========================================================
fdic_financials["year"] = pd.to_numeric(fdic_financials["year"], errors="coerce").astype("Int64")
fdic_financials["cert"] = pd.to_numeric(fdic_financials["cert"], errors="coerce").astype("Int64")
linked["cert"] = pd.to_numeric(linked["cert"], errors="coerce").astype("Int64")

# =========================================================
# 10. CHECK FOR DUPLICATE (year, cert) — required for validate="many_to_one"
# =========================================================
fdic_dupes = fdic_financials[fdic_financials.duplicated(subset=["year", "cert"], keep=False)]
if len(fdic_dupes) > 0:
    print(f"\n⚠️ WARNING: {len(fdic_dupes)} duplicate (year, cert) rows in fdic_financials.")
    print(fdic_dupes.sort_values(["year", "cert"]).head(20))
    fdic_financials = fdic_financials.drop_duplicates(subset=["year", "cert"], keep="first")
    print(f"Deduplicated to {len(fdic_financials)} rows.")
    

# Load the output from the previous step
linked = pd.read_parquet("I:/HMDA/linked_lender_year.parquet")

print("Loaded linked shape:", linked.shape)
print("Loaded linked columns:", linked.columns.tolist())

# =========================================================
# 11. MERGE FDIC FINANCIALS INTO LINKED
# =========================================================
linked = linked.merge(
    fdic_financials,
    on=["year", "cert"],
    how="left",
    suffixes=("", "_fdic"),
    validate="many_to_one",   # many lender-years can map to one bank-year record
)
linked["matched_fdic_financials"] = linked["asset"].notna()

# =========================================================
# 12. SANITY CHECK
# =========================================================
print("\n--- FDIC MERGE SUMMARY ---")
print("Rows:", len(linked))
print("Match rate (FDIC financials):", round(linked["matched_fdic_financials"].mean() * 100, 2), "%")

# =========================================================
# 13. SAVE FINAL OUTPUT
# =========================================================
linked.to_parquet("I:/HMDA/linked_final.parquet", index=False)
linked.to_csv("I:/HMDA/linked_final.csv", index=False)

print("\nSaved: linked_final.parquet")
print("Saved: linked_final.csv")

