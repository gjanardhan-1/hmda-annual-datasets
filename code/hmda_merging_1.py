#!/usr/bin/env python
# coding: utf-8

# In[3]:


# REFER BELOW CODE - FOR CORRECT CODE

import pandas as pd

# =========================================================
# 1. LOAD DATA
# =========================================================
hmda_lender_year = pd.read_parquet("I:/lender_year_output/lender_year_panel.parquet")
crosswalk = pd.read_excel("I:/HMDA/hmda-2018-present.xlsx")

print("Panel shape:   ", hmda_lender_year.shape)
print("Crosswalk shape:", crosswalk.shape)
print("Panel dtypes (year, lei):", hmda_lender_year["year"].dtype, hmda_lender_year["lei"].dtype)
print("Crosswalk dtypes (year, lei):", crosswalk["year"].dtype, crosswalk["lei"].dtype)

# =========================================================
# 2. NORMALIZE KEY COLUMNS (fixes most silent merge failures)
# =========================================================
for df in (hmda_lender_year, crosswalk):
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["lei"] = df["lei"].astype(str).str.strip().str.upper()

# =========================================================
# 3. CHECK FOR DUPLICATE (year, lei) IN CROSSWALK
#    validate="one_to_one" will crash on these — better to see them first
# =========================================================
cw_dupes = crosswalk[crosswalk.duplicated(subset=["year", "lei"], keep=False)]
if len(cw_dupes) > 0:
    print(f"\n⚠️  WARNING: {len(cw_dupes)} duplicate (year, lei) rows in crosswalk.")
    print(cw_dupes.sort_values(["year", "lei"]).head(20))
    # Keep first occurrence only — adjust logic if you need smarter dedup rules
    crosswalk = crosswalk.drop_duplicates(subset=["year", "lei"], keep="first")
    print(f"Deduplicated crosswalk to {len(crosswalk)} rows.")

panel_dupes = hmda_lender_year[hmda_lender_year.duplicated(subset=["year", "lei"], keep=False)]
if len(panel_dupes) > 0:
    print(f"\n⚠️  WARNING: {len(panel_dupes)} duplicate (year, lei) rows in lender_year_panel.")
    print(panel_dupes.sort_values(["year", "lei"]).head(20))

# =========================================================
# 4. MERGE
# =========================================================
cw_cols = ["year", "lei", "cert", "rssd", "entity", "type",
           "insure", "name", "assets", "assetl", "org",
           "assorg", "namet", "state", "fore"]

# only keep columns that actually exist (in case names differ slightly)
cw_cols = [c for c in cw_cols if c in crosswalk.columns]
missing_cols = set(["year", "lei", "cert", "rssd", "entity", "type",
                     "insure", "name", "assets", "assetl", "org",
                     "assorg", "namet", "state", "fore"]) - set(cw_cols)
if missing_cols:
    print(f"\n⚠️  These expected crosswalk columns were not found: {missing_cols}")

linked = hmda_lender_year.merge(
    crosswalk[cw_cols],
    on=["year", "lei"],
    how="left",
    validate="one_to_one",   # catches silent duplication immediately
)

# =========================================================
# 5. VALID FDIC CERT FLAG
# =========================================================
linked["valid_fdic_cert"] = pd.to_numeric(linked["cert"], errors="coerce").fillna(0).gt(0)

# =========================================================
# 6. SANITY CHECKS
# =========================================================
print("\n--- MERGE SUMMARY ---")
print("Rows before merge:", len(hmda_lender_year))
print("Rows after merge: ", len(linked))
print("Match rate (valid cert):", round(linked["valid_fdic_cert"].mean() * 100, 2), "%")

unmatched = linked.loc[~linked["valid_fdic_cert"], ["year", "lei"]].drop_duplicates()
print(f"\nUnmatched (year, lei) pairs: {len(unmatched)}")
print(unmatched.head(20))


# =========================================================
# 7. SAVE OUTPUT (both formats)
# =========================================================
linked.to_parquet("linked_lender_year.parquet", index=False)
linked.to_csv("linked_lender_year.csv", index=False)

print("\nSaved: linked_lender_year.parquet")
print("Saved: linked_lender_year.csv")


# In[2]:


# REFER BELOW CODE - FOR CORRECT CODE


import pandas as pd

# =========================================================
# 1. LOAD DATA
# =========================================================
hmda_lender_year = pd.read_parquet("I:/lender_year_output/lender_year_panel.parquet")
crosswalk = pd.read_excel("I:/HMDA/hmda-2018-present.xlsx")

# Normalize column names to lowercase (crosswalk headers are ALL CAPS)
crosswalk.columns = crosswalk.columns.str.strip().str.lower()
hmda_lender_year.columns = hmda_lender_year.columns.str.strip().str.lower()

print("Panel shape:   ", hmda_lender_year.shape)
print("Crosswalk shape:", crosswalk.shape)
print("Panel columns:", hmda_lender_year.columns.tolist())
print("Crosswalk columns:", crosswalk.columns.tolist())
print("Panel dtypes (year, lei):", hmda_lender_year["year"].dtype, hmda_lender_year["lei"].dtype)
print("Crosswalk dtypes (year, lei):", crosswalk["year"].dtype, crosswalk["lei"].dtype)

# =========================================================
# 2. NORMALIZE KEY VALUES (fixes most silent merge failures)
# =========================================================
for df in (hmda_lender_year, crosswalk):
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["lei"] = df["lei"].astype(str).str.strip().str.upper()

# =========================================================
# 3. CHECK FOR DUPLICATE (year, lei) IN CROSSWALK
# =========================================================
cw_dupes = crosswalk[crosswalk.duplicated(subset=["year", "lei"], keep=False)]
if len(cw_dupes) > 0:
    print(f"\n⚠️  WARNING: {len(cw_dupes)} duplicate (year, lei) rows in crosswalk.")
    print(cw_dupes.sort_values(["year", "lei"]).head(20))
    crosswalk = crosswalk.drop_duplicates(subset=["year", "lei"], keep="first")
    print(f"Deduplicated crosswalk to {len(crosswalk)} rows.")

panel_dupes = hmda_lender_year[hmda_lender_year.duplicated(subset=["year", "lei"], keep=False)]
if len(panel_dupes) > 0:
    print(f"\n⚠️  WARNING: {len(panel_dupes)} duplicate (year, lei) rows in lender_year_panel.")
    print(panel_dupes.sort_values(["year", "lei"]).head(20))

# =========================================================
# 4. MERGE
# =========================================================
cw_cols_wanted = ["year", "lei", "cert", "rssd", "entity", "type",
                   "insure", "name", "assets", "assetl", "org",
                   "assorg", "namet", "state", "fore"]

cw_cols = [c for c in cw_cols_wanted if c in crosswalk.columns]
missing_cols = set(cw_cols_wanted) - set(cw_cols)
if missing_cols:
    print(f"\n⚠️  These expected crosswalk columns were not found: {missing_cols}")

linked = hmda_lender_year.merge(
    crosswalk[cw_cols],
    on=["year", "lei"],
    how="left",
    validate="one_to_one",   # catches silent duplication immediately
)

# =========================================================
# 5. VALID FDIC CERT FLAG
# =========================================================
linked["valid_fdic_cert"] = pd.to_numeric(linked["cert"], errors="coerce").fillna(0).gt(0)

# =========================================================
# 6. SANITY CHECKS
# =========================================================
print("\n--- MERGE SUMMARY ---")
print("Rows before merge:", len(hmda_lender_year))
print("Rows after merge: ", len(linked))
print("Match rate (valid cert):", round(linked["valid_fdic_cert"].mean() * 100, 2), "%")

unmatched = linked.loc[~linked["valid_fdic_cert"], ["year", "lei"]].drop_duplicates()
print(f"\nUnmatched (year, lei) pairs: {len(unmatched)}")
print(unmatched.head(20))

# =========================================================
# 7. SAVE OUTPUT (both formats)
# =========================================================
linked.to_parquet("I:/HMDA/linked_lender_year.parquet", index=False)
linked.to_csv("I:/HMDA/linked_lender_year.csv", index=False)

print("\nSaved: linked_lender_year.parquet")
print("Saved: linked_lender_year.csv")


# In[7]:


import pandas as pd

hmda_lender_year = pd.read_parquet("I:/lender_year_output/lender_year_panel.parquet")
crosswalk = pd.read_excel("I:/HMDA/hmda-2018-present.xlsx")

crosswalk.columns = crosswalk.columns.str.strip().str.lower()
hmda_lender_year.columns = hmda_lender_year.columns.str.strip().str.lower()

crosswalk["year"] = pd.to_numeric(crosswalk["year"], errors="coerce").astype("int64")
hmda_lender_year["year"] = pd.to_numeric(hmda_lender_year["year"], errors="coerce").astype("int64")
crosswalk["lei"] = crosswalk["lei"].astype(str).str.strip().str.upper()
hmda_lender_year["lei"] = hmda_lender_year["lei"].astype(str).str.strip().str.upper()

cw_dupes = crosswalk[crosswalk.duplicated(subset=["year", "lei"], keep=False)]
if len(cw_dupes) > 0:
    print(f"WARNING: {len(cw_dupes)} duplicate (year, lei) rows in crosswalk.")
    crosswalk = crosswalk.drop_duplicates(subset=["year", "lei"], keep="first")

cw_cols = ["year", "lei", "cert", "rssd", "entity", "type",
           "insure", "name", "assets", "assetl", "org",
           "assorg", "namet", "state", "fore"]
cw_cols = [c for c in cw_cols if c in crosswalk.columns]

linked = hmda_lender_year.merge(
    crosswalk[cw_cols],
    on=["year", "lei"],
    how="left",
    validate="one_to_one",
)
linked["valid_fdic_cert"] = linked["cert"].fillna(0).gt(0)

print("Rows:", len(linked))
print("Match rate (valid FDIC cert):", round(linked["valid_fdic_cert"].mean() * 100, 2), "%")

# Save both formats
linked.to_parquet("I:/HMDA/linked_lender_year.parquet", index=False)
linked.to_csv("I:/HMDA/linked_lender_year.csv", index=False)

print("Saved: I:/HMDA/linked_lender_year.parquet")
print("Saved: I:/HMDA/linked_lender_year.csv")

