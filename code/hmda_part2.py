#!/usr/bin/env python
# coding: utf-8

# In[2]:


"""
lender_year_panel.py

Builds the lender-year panel from the HARMONIZED APPLICATION-LEVEL dataset
produced by hmda_pipeline.py (application_level.parquet / .csv). Does NOT
re-read any raw HMDA ZIP/CSV files.

IMPORTANT CAVEAT (please read):
This script assumes (LEI, activity_year) is the correct aggregation unit
because the user explicitly specified it. The underlying replication paper
was never provided to this pipeline, so that assumption -- and the list of
"additional lender-year variables" in Step 3 -- could NOT be verified
against the paper's actual methodology. Anywhere this matters, it is
flagged explicitly in comments and in the written reports rather than
silently assumed. If you have the paper's methodology section, compare it
against DATA_DICTIONARY / ADDITIONAL_VARIABLE_CANDIDATES below and adjust.

Usage:
    python lender_year_panel.py "I:\hmda_output"
    (the folder that contains application_level.parquet / .csv, i.e. the
    output directory from hmda_pipeline.py; or run with no argument and
    you will be prompted)

Only pandas + standard library are required for CSV input/output;
pyarrow (or fastparquet) is required only if you want Parquet output --
exactly like hmda_pipeline.py, this script degrades gracefully if it's
missing.
"""

import sys
import csv
import json
import hashlib
import logging
import platform
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd


# ======================================================================
# SECTION 0: CONFIG
# ======================================================================

OUTPUT_DIRNAME = "lender_year_output"

# ----------------------------------------------------------------------
# Step 5 outlier-detection thresholds (Step 4 quality checks).
# These are DOCUMENTED CHOICES, not values from any external source --
# flagged as such in the QC report. Adjust freely to match your paper's
# actual thresholds if/when you have them.
# ----------------------------------------------------------------------
MIN_APPLICATIONS_FOR_STABLE_RATE = 5   # lender-years below this are flagged
                                        # as "thin" -- rates computed on very
                                        # few applications are noisy, not
                                        # necessarily wrong
IQR_OUTLIER_MULTIPLIER = 1.5           # standard Tukey fence multiplier

FLOAT_TOLERANCE = 1e-6                 # tolerance for share-sum validation


# ----------------------------------------------------------------------
# SECTION 0a: VARIABLE DEFINITIONS (Step 2) -- documented for every
# constructed lender-year variable: formula, numerator, denominator,
# missing-value treatment, and interpretation. This dict IS the
# machine-readable "variable definitions" deliverable from Step 5.
# ----------------------------------------------------------------------
LENDER_YEAR_VARIABLE_DEFINITIONS = {
    "application_count": {
        "formula": "COUNT(*) per (lei, activity_year)",
        "numerator": "All retained applications for this lender-year "
                      "(already restricted upstream to action_taken in {1,2,3} "
                      "and every other Step-4 inclusion criterion)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "Rows with missing lei or missing activity_year are "
                              "EXCLUDED before grouping (see Step 4 quality checks) "
                              "and reported separately, never silently dropped.",
        "interpretation": "Total mortgage applications originated, or "
                           "approved-not-accepted, or denied by this lender in this year, "
                           "within the harmonized sample scope.",
    },
    "approval_count": {
        "formula": "COUNT(*) WHERE action_taken IN {1,2} per (lei, activity_year)",
        "numerator": "Applications with action_taken = 1 (Originated) OR 2 "
                      "(Approved but not accepted)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "action_taken is a REQUIRED column (validated upstream); "
                              "no missing values are possible in the harmonized input.",
        "interpretation": "Count of applications resulting in a favorable credit decision.",
    },
    "denial_count": {
        "formula": "COUNT(*) WHERE action_taken == 3 per (lei, activity_year)",
        "numerator": "Applications with action_taken = 3 (Denied)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "Same as approval_count.",
        "interpretation": "Count of applications formally denied.",
    },
    "approval_rate": {
        "formula": "approval_count / application_count",
        "numerator": "approval_count",
        "denominator": "application_count",
        "missing_treatment": "application_count is never 0 for a group that exists "
                              "(a group only exists because >=1 row aggregated into it); "
                              "no division-by-zero is possible.",
        "interpretation": "Share of applications approved (originated or approved-not-accepted).",
    },
    "denial_rate": {
        "formula": "denial_count / application_count",
        "numerator": "denial_count",
        "denominator": "application_count",
        "missing_treatment": "Same as approval_rate.",
        "interpretation": "Share of applications denied. NOTE: because the harmonized "
                           "input is restricted to action_taken in {1,2,3} only, "
                           "approval_rate + denial_rate == 1.0 exactly for every "
                           "lender-year (validated in Step 4).",
    },
    "du_share": {
        "formula": "COUNT(primary_AUS == 'Desktop Underwriter (DU)') / application_count",
        "numerator": "Applications whose primary_AUS (derived from aus-1) is DU",
        "denominator": "application_count",
        "missing_treatment": "Applications with primary_AUS == 'Missing' contribute 0 "
                              "to the numerator and ARE included in the denominator "
                              "(i.e., shares are of ALL applications, not just those "
                              "with a reported AUS). See external_aus_coverage for the "
                              "companion 'how much of the denominator has any AUS at all' metric.",
        "interpretation": "Share of this lender's applications underwritten via DU.",
    },
    "lpa_share": {
        "formula": "COUNT(primary_AUS == 'Loan Product Advisor (LPA)') / application_count",
        "numerator": "Applications whose primary_AUS is LPA",
        "denominator": "application_count",
        "missing_treatment": "Same as du_share.",
        "interpretation": "Share of this lender's applications underwritten via LPA.",
    },
    "internal_aus_share": {
        "formula": "COUNT(primary_AUS == 'Internal/Proprietary') / application_count",
        "numerator": "Applications whose primary_AUS is an internal/proprietary system",
        "denominator": "application_count",
        "missing_treatment": "Same as du_share.",
        "interpretation": "Share of this lender's applications underwritten via an "
                           "in-house/proprietary AUS rather than a GSE system.",
    },
    "other_aus_share": {
        "formula": "COUNT(primary_AUS IN {'Other','Missing'}) / application_count",
        "numerator": "Applications whose primary_AUS is 'Other' (TOTAL Scorecard, GUS, "
                      "or code 5) OR 'Missing' (not applicable/exempt/blank)",
        "denominator": "application_count",
        "missing_treatment": "This bucket is defined to ABSORB missing/not-applicable "
                              "AUS values, specifically so that "
                              "du_share + lpa_share + internal_aus_share + other_aus_share == 1 "
                              "exactly for every lender-year (this is the Step-4 'share "
                              "validation' check).",
        "interpretation": "Share of applications using a non-DU/non-LPA/non-internal AUS, "
                           "OR with no AUS information reported at all.",
    },
    "external_aus_coverage": {
        "formula": "(du_share + lpa_share)",
        "numerator": "Applications using DU or LPA -- i.e., a GSE-operated external AUS",
        "denominator": "application_count",
        "missing_treatment": "N/A -- derived from du_share/lpa_share directly.",
        "interpretation": "ASSUMPTION FLAGGED: interpreted as 'share of applications "
                           "underwritten via an external (GSE) AUS' as opposed to internal/"
                           "proprietary or no AUS. An alternative, equally defensible reading "
                           "of 'coverage' is 'share of applications with ANY AUS reported at "
                           "all' (i.e., 1 - missing_aus_share). Both are provided -- see "
                           "external_aus_coverage_alt_any_aus_reported below -- pick whichever "
                           "matches the paper's actual definition once available.",
    },
    "external_aus_coverage_alt_any_aus_reported": {
        "formula": "1 - (COUNT(primary_AUS == 'Missing') / application_count)",
        "numerator": "Applications with a non-missing primary_AUS value",
        "denominator": "application_count",
        "missing_treatment": "N/A -- this metric measures missingness directly.",
        "interpretation": "ALTERNATIVE definition of 'AUS coverage': share of applications "
                           "for which ANY automated underwriting system was reported, "
                           "regardless of which one. Provided for comparison; not the "
                           "primary external_aus_coverage metric above.",
    },
}

# ----------------------------------------------------------------------
# SECTION 0b: STEP 3 -- CANDIDATE additional lender-year variables.
# UNVERIFIED against the paper (not provided). These are common variables
# in HMDA lender-year fair-lending studies, listed here as candidates only.
# Each notes exactly which application-level column(s) it would be built
# from, so adding any of them later is a one-line change.
# ----------------------------------------------------------------------
ADDITIONAL_VARIABLE_CANDIDATES = {
    "avg_loan_amount": "MEAN(loan_amount) per (lei, activity_year). Source column: loan_amount.",
    "avg_combined_ltv": "MEAN(combined_loan_to_value_ratio) per (lei, activity_year). "
                          "Source column: combined_loan_to_value_ratio.",
    "avg_interest_rate": "MEAN(interest_rate) per (lei, activity_year), applicants with "
                          "action_taken==1 only (rate is only meaningful for originated loans). "
                          "Source column: interest_rate.",
    "minority_applicant_share": "Share of applications where derived_race/derived_ethnicity "
                                 "indicates a minority applicant, per (lei, activity_year). "
                                 "Source columns: derived_race, derived_ethnicity. Requires an "
                                 "explicit minority-classification rule not specified here.",
    "female_applicant_share": "Share of applications with applicant_sex == 'Female'. "
                               "Source column: applicant_sex.",
    "avg_applicant_income": "MEAN(income) per (lei, activity_year). Source column: income.",
    "market_hhi": "Herfindahl-Hirschman Index of application volume across lenders within "
                   "a geography-year (e.g., county-year), NOT a lender-year variable itself "
                   "but often merged onto the lender-year panel. Source columns: lei, "
                   "county_code, activity_year, application_count.",
}


# ======================================================================
# SECTION 1: LOGGING
# ======================================================================

def setup_logging(output_dir: Path) -> logging.Logger:
    logger = logging.getLogger("lender_year_panel")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(output_dir / "aggregation_log.txt", mode="w", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


# ======================================================================
# SECTION 2: INPUT FOLDER + LOADING application_level.{parquet,csv}
# ======================================================================

def get_input_directory() -> Path:
    """
    Determine the folder containing application_level.parquet/.csv
    (the OUTPUT of hmda_pipeline.py). Robust to Jupyter/IPython
    kernel-launcher argv artifacts, same as hmda_pipeline.py.
    """
    candidate = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        if not arg.startswith("-") and not arg.lower().endswith(".json"):
            candidate = arg

    if candidate:
        raw_input_path = candidate
    else:
        raw_input_path = input(
            "Enter the folder containing application_level.parquet/.csv "
            "(the hmda_output folder from Prompt 1): "
        ).strip()

    raw_input_path = raw_input_path.strip('"').strip("'")
    folder = Path(raw_input_path).expanduser().resolve()

    if not folder.is_dir():
        print(f"ERROR: '{folder}' is not a valid directory.")
        sys.exit(1)

    return folder


def get_output_directory(input_dir: Path) -> Path:
    out_dir = input_dir.parent / OUTPUT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def load_application_level_data(input_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    """
    Loads the harmonized application-level dataset produced by
    hmda_pipeline.py. Does NOT touch any raw HMDA ZIP/CSV file.

    Prefers Parquet (faster, preserves dtypes); falls back to CSV if the
    Parquet file isn't present (e.g. because pyarrow wasn't installed
    when Prompt 1's script ran) or if reading it fails for any reason.
    """
    parquet_path = input_dir / "application_level.parquet"
    csv_path = input_dir / "application_level.csv"

    if parquet_path.exists():
        try:
            logger.info(f"Loading {parquet_path}")
            df = pd.read_parquet(parquet_path)
            logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns from Parquet.")
            return df
        except ImportError:
            logger.warning(
                "application_level.parquet exists but no Parquet engine is installed "
                "(pip install pyarrow). Falling back to CSV."
            )
        except Exception as exc:
            logger.warning(f"Could not read {parquet_path} ({exc}). Falling back to CSV.")

    if csv_path.exists():
        logger.info(f"Loading {csv_path}")
        # Read core columns as string to avoid pandas silently coercing codes
        # (e.g. lei as a number, activity_year as float). We only need a
        # handful of columns for this panel -- read everything as string,
        # then explicitly cast the numeric covariates we actually average.
        df = pd.read_csv(csv_path, dtype=str, keep_default_na=True, na_values=["", "NA"])
        logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns from CSV.")
        return df

    logger.error(
        f"Neither application_level.parquet nor application_level.csv found in {input_dir}. "
        f"Run hmda_pipeline.py (Prompt 1) first."
    )
    sys.exit(1)


# ======================================================================
# SECTION 3: STEP 4 (part 1) -- PRE-AGGREGATION VALIDATION
# ======================================================================

def validate_before_aggregation(df: pd.DataFrame, logger: logging.Logger) -> (pd.DataFrame, dict):
    """
    Detect and EXCLUDE (never silently drop without logging) rows with a
    missing lei or missing activity_year, since these cannot be assigned
    to any (LEI, Year) group. Returns the cleaned dataframe plus a dict of
    counts for the aggregation log / QC report.
    """
    issues = {}
    n_start = len(df)

    missing_lei_mask = df["lei"].isna() | (df["lei"].astype(str).str.strip() == "")
    n_missing_lei = int(missing_lei_mask.sum())
    issues["missing_lei_rows_excluded"] = n_missing_lei
    if n_missing_lei > 0:
        logger.warning(f"Excluding {n_missing_lei:,} rows with missing/blank lei.")

    df = df.loc[~missing_lei_mask]

    missing_year_mask = df["activity_year"].isna() | (df["activity_year"].astype(str).str.strip() == "")
    n_missing_year = int(missing_year_mask.sum())
    issues["missing_year_rows_excluded"] = n_missing_year
    if n_missing_year > 0:
        logger.warning(f"Excluding {n_missing_year:,} rows with missing/blank activity_year.")

    df = df.loc[~missing_year_mask]

    n_end = len(df)
    issues["rows_before_cleaning"] = n_start
    issues["rows_after_cleaning"] = n_end
    logger.info(f"Pre-aggregation cleaning: {n_start:,} -> {n_end:,} rows "
                f"({n_start - n_end:,} excluded for missing lei/year).")

    return df, issues


# ======================================================================
# SECTION 4: STEP 1 + STEP 2 -- BUILD THE (LEI, YEAR) PANEL
# ======================================================================

AUS_CATEGORIES = ["Desktop Underwriter (DU)", "Loan Product Advisor (LPA)",
                   "Internal/Proprietary", "Other", "Missing"]


def build_lender_year_panel(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Aggregate the harmonized application-level dataframe to exactly one
    row per (lei, activity_year), computing every Step-2 variable.

    Everything here operates on the ALREADY-FILTERED harmonized sample
    from Prompt 1 (owner-occupied, site-built, 1-4 family, home purchase,
    first lien, conventional, conforming, action_taken in {1,2,3}).
    Consequently application_count == approval_count + denial_count
    exactly, for every group -- this identity is checked in Step 4.
    """
    df = df.copy()

    # action_taken and primary_AUS may have been read as string (from CSV
    # fallback) or already string (from Parquet, since hmda_pipeline.py
    # wrote them as string dtype) -- normalize defensively either way.
    df["action_taken"] = df["action_taken"].astype(str).str.strip()
    if "primary_AUS" not in df.columns:
        raise RuntimeError(
            "Input is missing 'primary_AUS' -- this column is expected to already exist "
            "in application_level.parquet/.csv from hmda_pipeline.py (Prompt 1). "
            "Re-run Prompt 1 if it's missing."
        )
    df["primary_AUS"] = df["primary_AUS"].astype(str).str.strip()

    df["is_approval"] = df["action_taken"].isin(["1", "2"])
    df["is_denial"] = df["action_taken"] == "3"

    for category in AUS_CATEGORIES:
        col_name = f"_aus_flag_{category}"
        df[col_name] = (df["primary_AUS"] == category)

    grouped = df.groupby(["lei", "activity_year"], dropna=False)

    panel = grouped.agg(
        application_count=("action_taken", "size"),
        approval_count=("is_approval", "sum"),
        denial_count=("is_denial", "sum"),
        _du_count=(f"_aus_flag_{AUS_CATEGORIES[0]}", "sum"),
        _lpa_count=(f"_aus_flag_{AUS_CATEGORIES[1]}", "sum"),
        _internal_count=(f"_aus_flag_{AUS_CATEGORIES[2]}", "sum"),
        _other_count=(f"_aus_flag_{AUS_CATEGORIES[3]}", "sum"),
        _missing_aus_count=(f"_aus_flag_{AUS_CATEGORIES[4]}", "sum"),
    ).reset_index()

    panel = panel.rename(columns={"activity_year": "year"})

    # --- Step 2 rate/share variables (see LENDER_YEAR_VARIABLE_DEFINITIONS) ---
    panel["approval_rate"] = panel["approval_count"] / panel["application_count"]
    panel["denial_rate"] = panel["denial_count"] / panel["application_count"]

    panel["du_share"] = panel["_du_count"] / panel["application_count"]
    panel["lpa_share"] = panel["_lpa_count"] / panel["application_count"]
    panel["internal_aus_share"] = panel["_internal_count"] / panel["application_count"]
    panel["other_aus_share"] = (panel["_other_count"] + panel["_missing_aus_count"]) / panel["application_count"]

    panel["external_aus_coverage"] = panel["du_share"] + panel["lpa_share"]
    panel["external_aus_coverage_alt_any_aus_reported"] = 1 - (
        panel["_missing_aus_count"] / panel["application_count"]
    )

    # Drop internal helper count columns -- not part of the deliverable schema,
    # but keep them available for debugging via a separate diagnostic export.
    diagnostic_cols = ["_du_count", "_lpa_count", "_internal_count", "_other_count", "_missing_aus_count"]
    panel_diagnostic = panel[["lei", "year"] + diagnostic_cols].copy()
    panel = panel.drop(columns=diagnostic_cols)

    final_columns = [
        "lei", "year", "application_count", "approval_count", "denial_count",
        "approval_rate", "denial_rate",
        "du_share", "lpa_share", "internal_aus_share", "other_aus_share",
        "external_aus_coverage", "external_aus_coverage_alt_any_aus_reported",
    ]
    panel = panel[final_columns]

    logger.info(f"Built lender-year panel: {len(panel):,} (LEI, Year) rows "
                f"from {len(df):,} application-level rows "
                f"({df['lei'].nunique():,} unique LEIs, "
                f"{df['activity_year'].nunique()} years).")

    return panel, panel_diagnostic


# ======================================================================
# SECTION 5: STEP 4 (part 2) -- POST-AGGREGATION QUALITY CHECKS
# ======================================================================

def run_quality_checks(panel: pd.DataFrame, pre_agg_issues: dict, logger: logging.Logger) -> dict:
    """
    Runs every Step-4 quality check and returns a structured results dict
    (used both for the printed/logged summary and the written QC report).
    Checks are DIAGNOSTIC -- they flag issues but do not silently modify
    or drop rows from the panel itself (any row genuinely too broken to
    keep would already have been excluded upstream in validate_before_aggregation).
    """
    results = {"pre_aggregation": pre_agg_issues}

    # ---- Duplicate LEI-year detection ----
    dup_mask = panel.duplicated(subset=["lei", "year"], keep=False)
    n_dup = int(dup_mask.sum())
    results["duplicate_lei_year_rows"] = n_dup
    if n_dup > 0:
        logger.error(f"QC FAILURE: {n_dup:,} duplicate (lei, year) rows found in the panel "
                      f"-- aggregation should make this impossible; investigate groupby logic.")
    else:
        logger.info("QC PASSED: no duplicate (lei, year) rows.")

    # ---- Missing LEI / missing year detection (within the panel itself; should be zero
    #      since validate_before_aggregation already excluded these upstream) ----
    n_missing_lei_panel = int(panel["lei"].isna().sum())
    n_missing_year_panel = int(panel["year"].isna().sum())
    results["panel_missing_lei"] = n_missing_lei_panel
    results["panel_missing_year"] = n_missing_year_panel
    if n_missing_lei_panel or n_missing_year_panel:
        logger.error(f"QC FAILURE: panel contains missing lei ({n_missing_lei_panel}) "
                      f"or missing year ({n_missing_year_panel}) after aggregation.")
    else:
        logger.info("QC PASSED: no missing lei/year in the panel.")

    # ---- Approval-rate validation ----
    bad_rate_mask = (panel["approval_rate"] < 0) | (panel["approval_rate"] > 1) | panel["approval_rate"].isna()
    n_bad_rate = int(bad_rate_mask.sum())
    results["invalid_approval_rate_rows"] = n_bad_rate

    identity_diff = (panel["approval_rate"] + panel["denial_rate"] - 1.0).abs()
    n_identity_violation = int((identity_diff > FLOAT_TOLERANCE).sum())
    results["approval_denial_identity_violations"] = n_identity_violation

    if n_bad_rate or n_identity_violation:
        logger.error(f"QC FAILURE: {n_bad_rate:,} rows with approval_rate outside [0,1]; "
                      f"{n_identity_violation:,} rows where approval_rate + denial_rate != 1.")
    else:
        logger.info("QC PASSED: approval_rate in [0,1] and approval_rate + denial_rate == 1 for all rows.")

    # ---- Share validation (DU + LPA + Internal + Other == 1) ----
    share_sum = panel["du_share"] + panel["lpa_share"] + panel["internal_aus_share"] + panel["other_aus_share"]
    share_diff = (share_sum - 1.0).abs()
    n_share_violation = int((share_diff > FLOAT_TOLERANCE).sum())
    results["aus_share_sum_violations"] = n_share_violation
    if n_share_violation > 0:
        logger.error(f"QC FAILURE: {n_share_violation:,} rows where "
                      f"du_share+lpa_share+internal_aus_share+other_aus_share != 1.")
    else:
        logger.info("QC PASSED: AUS shares (DU+LPA+Internal+Other) sum to 1 for every row.")

    # ---- Outlier detection ----
    # (a) "thin" lender-years: rates computed on very few applications.
    n_thin = int((panel["application_count"] < MIN_APPLICATIONS_FOR_STABLE_RATE).sum())
    results["thin_lender_years_flagged"] = n_thin
    results["thin_threshold_applications"] = MIN_APPLICATIONS_FOR_STABLE_RATE
    logger.info(f"Flagged {n_thin:,} lender-years with < {MIN_APPLICATIONS_FOR_STABLE_RATE} "
                f"applications (rates for these are statistically noisy, not necessarily wrong).")

    # (b) IQR-based outliers on application_count, computed WITHIN each year
    #     (lender size distributions differ by year; a global IQR would just
    #     flag "large banks" every year rather than true anomalies).
    outlier_flags = []
    for year, group in panel.groupby("year"):
        q1, q3 = group["application_count"].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower = q1 - IQR_OUTLIER_MULTIPLIER * iqr
        upper = q3 + IQR_OUTLIER_MULTIPLIER * iqr
        flags = (group["application_count"] < lower) | (group["application_count"] > upper)
        outlier_flags.append(flags)
    application_count_outlier_mask = pd.concat(outlier_flags).sort_index()
    n_outliers = int(application_count_outlier_mask.sum())
    results["application_count_outliers_flagged"] = n_outliers
    logger.info(f"Flagged {n_outliers:,} lender-years as application_count outliers "
                f"(Tukey IQR method, computed within each year).")

    panel["_flag_thin"] = panel["application_count"] < MIN_APPLICATIONS_FOR_STABLE_RATE
    panel["_flag_application_count_outlier"] = application_count_outlier_mask.values

    return results


# ======================================================================
# SECTION 6: OUTPUT WRITERS (Step 5)
# ======================================================================

def write_variable_definitions(output_dir: Path) -> Path:
    path = output_dir / "variable_definitions.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "formula", "numerator", "denominator",
                          "missing_value_treatment", "interpretation"])
        for var, meta in LENDER_YEAR_VARIABLE_DEFINITIONS.items():
            writer.writerow([var, meta["formula"], meta["numerator"], meta["denominator"],
                              meta["missing_treatment"], meta["interpretation"]])
        writer.writerow([])
        writer.writerow(["--- CANDIDATE ADDITIONAL VARIABLES (Step 3, UNVERIFIED against paper) ---"])
        for var, note in ADDITIONAL_VARIABLE_CANDIDATES.items():
            writer.writerow([var, note])
    return path


def write_qc_report(qc_results: dict, output_dir: Path) -> Path:
    path = output_dir / "quality_control_report.txt"
    lines = ["LENDER-YEAR PANEL - QUALITY CONTROL REPORT",
             f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]
    lines.append(json.dumps(qc_results, indent=2))
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_summary_statistics(panel: pd.DataFrame, output_dir: Path) -> Path:
    path = output_dir / "summary_statistics.csv"
    numeric_cols = ["application_count", "approval_count", "denial_count",
                     "approval_rate", "denial_rate", "du_share", "lpa_share",
                     "internal_aus_share", "other_aus_share", "external_aus_coverage"]
    summary = panel[numeric_cols].describe().T
    summary.to_csv(path)
    return path


def sha256_of_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def write_checksums(file_paths: list, output_dir: Path) -> Path:
    path = output_dir / "SHA256SUMS.txt"
    lines = [f"{sha256_of_file(p)}  {p.name}" for p in file_paths if p.exists()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ======================================================================
# SECTION 7: MAIN ORCHESTRATION
# ======================================================================

def main():
    input_dir = get_input_directory()
    output_dir = get_output_directory(input_dir)
    logger = setup_logging(output_dir)

    logger.info(f"Detected OS: {platform.system()} {platform.release()}")
    logger.info(f"Input directory (hmda_output from Prompt 1): {input_dir}")
    logger.info(f"Output directory: {output_dir}")

    # ---- Load application-level data (Prompt 1 output ONLY -- no raw HMDA files) ----
    app_df = load_application_level_data(input_dir, logger)

    # ---- Step 4 (pre-aggregation): missing lei / missing year detection + exclusion ----
    app_df, pre_agg_issues = validate_before_aggregation(app_df, logger)

    # ---- Steps 1-2: build the (LEI, Year) panel ----
    panel, panel_diagnostic = build_lender_year_panel(app_df, logger)

    # ---- Step 4 (post-aggregation): full quality-control suite ----
    qc_results = run_quality_checks(panel, pre_agg_issues, logger)

    # Drop internal QC flag columns from the DELIVERABLE panel (they're
    # diagnostic, not part of the requested variable set) but keep them
    # in a side file for transparency.
    qc_flag_cols = ["_flag_thin", "_flag_application_count_outlier"]
    panel_with_flags = panel.copy()
    panel = panel.drop(columns=qc_flag_cols)

    # ---- Step 5: write all outputs ----
    parquet_path = output_dir / "lender_year_panel.parquet"
    csv_path = output_dir / "lender_year_panel.csv"

    parquet_written = False
    try:
        panel.to_parquet(parquet_path, index=False)
        logger.info(f"Wrote {parquet_path}")
        parquet_written = True
    except ImportError:
        logger.warning(
            "Could not write Parquet output: no Parquet engine is installed. "
            "Run 'pip install pyarrow' (or 'conda install pyarrow') and re-run "
            "this script to also get lender_year_panel.parquet. "
            "Continuing with all other outputs."
        )

    panel.to_csv(csv_path, index=False)
    logger.info(f"Wrote {csv_path}")

    # Diagnostic side-files (not in Step 5's required list, but written for
    # transparency/debugging -- clearly named so they aren't mistaken for
    # the primary deliverable).
    panel_with_flags.to_csv(output_dir / "lender_year_panel_with_qc_flags.csv", index=False)
    panel_diagnostic.to_csv(output_dir / "aus_count_diagnostics.csv", index=False)

    var_def_path = write_variable_definitions(output_dir)
    qc_path = write_qc_report(qc_results, output_dir)
    summary_path = write_summary_statistics(panel, output_dir)

    logger.info(f"Wrote {var_def_path}")
    logger.info(f"Wrote {qc_path}")
    logger.info(f"Wrote {summary_path}")

    checksummed_files = [csv_path, var_def_path, qc_path, summary_path]
    if parquet_written:
        checksummed_files.insert(0, parquet_path)
    checksums_path = write_checksums(checksummed_files, output_dir)
    logger.info(f"Wrote {checksums_path}")

    # ---- Final summary ----
    logger.info("=" * 60)
    logger.info("LENDER-YEAR PANEL COMPLETE")
    logger.info(f"Panel rows (unique LEI-year combinations): {len(panel):,}")
    logger.info(f"Unique lenders (LEIs): {panel['lei'].nunique():,}")
    logger.info(f"Years covered: {sorted(panel['year'].unique())}")
    logger.info(f"QC issues found: duplicates={qc_results['duplicate_lei_year_rows']}, "
                f"identity_violations={qc_results['approval_denial_identity_violations']}, "
                f"share_violations={qc_results['aus_share_sum_violations']}")
    logger.info(f"All outputs written to: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()


# In[3]:


"""
lender_year_panel.py

Builds the lender-year panel from the HARMONIZED APPLICATION-LEVEL dataset
produced by hmda_pipeline.py (application_level.parquet / .csv). Does NOT
re-read any raw HMDA ZIP/CSV files.

IMPORTANT CAVEAT (please read):
This script assumes (LEI, activity_year) is the correct aggregation unit
because the user explicitly specified it. The underlying replication paper
was never provided to this pipeline, so that assumption -- and the list of
"additional lender-year variables" in Step 3 -- could NOT be verified
against the paper's actual methodology. Anywhere this matters, it is
flagged explicitly in comments and in the written reports rather than
silently assumed. If you have the paper's methodology section, compare it
against DATA_DICTIONARY / ADDITIONAL_VARIABLE_CANDIDATES below and adjust.

Usage:
    python lender_year_panel.py "I:\\hmda_output"
    (the folder that contains application_level.parquet / .csv, i.e. the
    output directory from hmda_pipeline.py; or run with no argument and
    you will be prompted)

Only pandas + standard library are required for CSV input/output;
pyarrow (or fastparquet) is required only if you want Parquet output --
exactly like hmda_pipeline.py, this script degrades gracefully if it's
missing.
"""

import sys
import csv
import json
import hashlib
import logging
import platform
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd


# ======================================================================
# SECTION 0: CONFIG
# ======================================================================

OUTPUT_DIRNAME = "lender_year_output"

# Only these columns are needed to build the lender-year panel. Loading
# only these (instead of all ~50 columns in application_level.parquet)
# avoids MemoryError on large national files (20M+ rows as object dtype
# is too large to hold entirely in memory on most machines otherwise).
COLUMNS_NEEDED_FOR_PANEL = ["lei", "activity_year", "action_taken", "primary_AUS"]

# ----------------------------------------------------------------------
# Step 5 outlier-detection thresholds (Step 4 quality checks).
# These are DOCUMENTED CHOICES, not values from any external source --
# flagged as such in the QC report. Adjust freely to match your paper's
# actual thresholds if/when you have them.
# ----------------------------------------------------------------------
MIN_APPLICATIONS_FOR_STABLE_RATE = 5   # lender-years below this are flagged
                                        # as "thin" -- rates computed on very
                                        # few applications are noisy, not
                                        # necessarily wrong
IQR_OUTLIER_MULTIPLIER = 1.5           # standard Tukey fence multiplier

FLOAT_TOLERANCE = 1e-6                 # tolerance for share-sum validation


# ----------------------------------------------------------------------
# SECTION 0a: VARIABLE DEFINITIONS (Step 2) -- documented for every
# constructed lender-year variable: formula, numerator, denominator,
# missing-value treatment, and interpretation. This dict IS the
# machine-readable "variable definitions" deliverable from Step 5.
# ----------------------------------------------------------------------
LENDER_YEAR_VARIABLE_DEFINITIONS = {
    "application_count": {
        "formula": "COUNT(*) per (lei, activity_year)",
        "numerator": "All retained applications for this lender-year "
                      "(already restricted upstream to action_taken in {1,2,3} "
                      "and every other Step-4 inclusion criterion)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "Rows with missing lei or missing activity_year are "
                              "EXCLUDED before grouping (see Step 4 quality checks) "
                              "and reported separately, never silently dropped.",
        "interpretation": "Total mortgage applications originated, or "
                           "approved-not-accepted, or denied by this lender in this year, "
                           "within the harmonized sample scope.",
    },
    "approval_count": {
        "formula": "COUNT(*) WHERE action_taken IN {1,2} per (lei, activity_year)",
        "numerator": "Applications with action_taken = 1 (Originated) OR 2 "
                      "(Approved but not accepted)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "action_taken is a REQUIRED column (validated upstream); "
                              "no missing values are possible in the harmonized input.",
        "interpretation": "Count of applications resulting in a favorable credit decision.",
    },
    "denial_count": {
        "formula": "COUNT(*) WHERE action_taken == 3 per (lei, activity_year)",
        "numerator": "Applications with action_taken = 3 (Denied)",
        "denominator": "N/A (raw count)",
        "missing_treatment": "Same as approval_count.",
        "interpretation": "Count of applications formally denied.",
    },
    "approval_rate": {
        "formula": "approval_count / application_count",
        "numerator": "approval_count",
        "denominator": "application_count",
        "missing_treatment": "application_count is never 0 for a group that exists "
                              "(a group only exists because >=1 row aggregated into it); "
                              "no division-by-zero is possible.",
        "interpretation": "Share of applications approved (originated or approved-not-accepted).",
    },
    "denial_rate": {
        "formula": "denial_count / application_count",
        "numerator": "denial_count",
        "denominator": "application_count",
        "missing_treatment": "Same as approval_rate.",
        "interpretation": "Share of applications denied. NOTE: because the harmonized "
                           "input is restricted to action_taken in {1,2,3} only, "
                           "approval_rate + denial_rate == 1.0 exactly for every "
                           "lender-year (validated in Step 4).",
    },
    "du_share": {
        "formula": "COUNT(primary_AUS == 'Desktop Underwriter (DU)') / application_count",
        "numerator": "Applications whose primary_AUS (derived from aus-1) is DU",
        "denominator": "application_count",
        "missing_treatment": "Applications with primary_AUS == 'Missing' contribute 0 "
                              "to the numerator and ARE included in the denominator "
                              "(i.e., shares are of ALL applications, not just those "
                              "with a reported AUS). See external_aus_coverage for the "
                              "companion 'how much of the denominator has any AUS at all' metric.",
        "interpretation": "Share of this lender's applications underwritten via DU.",
    },
    "lpa_share": {
        "formula": "COUNT(primary_AUS == 'Loan Product Advisor (LPA)') / application_count",
        "numerator": "Applications whose primary_AUS is LPA",
        "denominator": "application_count",
        "missing_treatment": "Same as du_share.",
        "interpretation": "Share of this lender's applications underwritten via LPA.",
    },
    "internal_aus_share": {
        "formula": "COUNT(primary_AUS == 'Internal/Proprietary') / application_count",
        "numerator": "Applications whose primary_AUS is an internal/proprietary system",
        "denominator": "application_count",
        "missing_treatment": "Same as du_share.",
        "interpretation": "Share of this lender's applications underwritten via an "
                           "in-house/proprietary AUS rather than a GSE system.",
    },
    "other_aus_share": {
        "formula": "COUNT(primary_AUS IN {'Other','Missing'}) / application_count",
        "numerator": "Applications whose primary_AUS is 'Other' (TOTAL Scorecard, GUS, "
                      "or code 5) OR 'Missing' (not applicable/exempt/blank)",
        "denominator": "application_count",
        "missing_treatment": "This bucket is defined to ABSORB missing/not-applicable "
                              "AUS values, specifically so that "
                              "du_share + lpa_share + internal_aus_share + other_aus_share == 1 "
                              "exactly for every lender-year (this is the Step-4 'share "
                              "validation' check).",
        "interpretation": "Share of applications using a non-DU/non-LPA/non-internal AUS, "
                           "OR with no AUS information reported at all.",
    },
    "external_aus_coverage": {
        "formula": "(du_share + lpa_share)",
        "numerator": "Applications using DU or LPA -- i.e., a GSE-operated external AUS",
        "denominator": "application_count",
        "missing_treatment": "N/A -- derived from du_share/lpa_share directly.",
        "interpretation": "ASSUMPTION FLAGGED: interpreted as 'share of applications "
                           "underwritten via an external (GSE) AUS' as opposed to internal/"
                           "proprietary or no AUS. An alternative, equally defensible reading "
                           "of 'coverage' is 'share of applications with ANY AUS reported at "
                           "all' (i.e., 1 - missing_aus_share). Both are provided -- see "
                           "external_aus_coverage_alt_any_aus_reported below -- pick whichever "
                           "matches the paper's actual definition once available.",
    },
    "external_aus_coverage_alt_any_aus_reported": {
        "formula": "1 - (COUNT(primary_AUS == 'Missing') / application_count)",
        "numerator": "Applications with a non-missing primary_AUS value",
        "denominator": "application_count",
        "missing_treatment": "N/A -- this metric measures missingness directly.",
        "interpretation": "ALTERNATIVE definition of 'AUS coverage': share of applications "
                           "for which ANY automated underwriting system was reported, "
                           "regardless of which one. Provided for comparison; not the "
                           "primary external_aus_coverage metric above.",
    },
}

# ----------------------------------------------------------------------
# SECTION 0b: STEP 3 -- CANDIDATE additional lender-year variables.
# UNVERIFIED against the paper (not provided). These are common variables
# in HMDA lender-year fair-lending studies, listed here as candidates only.
# Each notes exactly which application-level column(s) it would be built
# from, so adding any of them later is a one-line change.
#
# NOTE: none of these are currently computed by this script, since
# COLUMNS_NEEDED_FOR_PANEL only loads the 4 columns required for the
# CONFIRMED variable set above (to keep memory usage bounded on large
# national files). If you want any of these, add the source column(s)
# to COLUMNS_NEEDED_FOR_PANEL and extend build_lender_year_panel()
# accordingly.
# ----------------------------------------------------------------------
ADDITIONAL_VARIABLE_CANDIDATES = {
    "avg_loan_amount": "MEAN(loan_amount) per (lei, activity_year). Source column: loan_amount.",
    "avg_combined_ltv": "MEAN(combined_loan_to_value_ratio) per (lei, activity_year). "
                          "Source column: combined_loan_to_value_ratio.",
    "avg_interest_rate": "MEAN(interest_rate) per (lei, activity_year), applicants with "
                          "action_taken==1 only (rate is only meaningful for originated loans). "
                          "Source column: interest_rate.",
    "minority_applicant_share": "Share of applications where derived_race/derived_ethnicity "
                                 "indicates a minority applicant, per (lei, activity_year). "
                                 "Source columns: derived_race, derived_ethnicity. Requires an "
                                 "explicit minority-classification rule not specified here.",
    "female_applicant_share": "Share of applications with applicant_sex == 'Female'. "
                               "Source column: applicant_sex.",
    "avg_applicant_income": "MEAN(income) per (lei, activity_year). Source column: income.",
    "market_hhi": "Herfindahl-Hirschman Index of application volume across lenders within "
                   "a geography-year (e.g., county-year), NOT a lender-year variable itself "
                   "but often merged onto the lender-year panel. Source columns: lei, "
                   "county_code, activity_year, application_count.",
}


# ======================================================================
# SECTION 1: LOGGING
# ======================================================================

def setup_logging(output_dir: Path) -> logging.Logger:
    logger = logging.getLogger("lender_year_panel")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    file_handler = logging.FileHandler(output_dir / "aggregation_log.txt", mode="w", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


# ======================================================================
# SECTION 2: INPUT FOLDER + LOADING application_level.{parquet,csv}
# ======================================================================

def get_input_directory() -> Path:
    """
    Determine the folder containing application_level.parquet/.csv
    (the OUTPUT of hmda_pipeline.py). Robust to Jupyter/IPython
    kernel-launcher argv artifacts, same as hmda_pipeline.py.
    """
    candidate = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        if not arg.startswith("-") and not arg.lower().endswith(".json"):
            candidate = arg

    if candidate:
        raw_input_path = candidate
    else:
        raw_input_path = input(
            "Enter the folder containing application_level.parquet/.csv "
            "(the hmda_output folder from Prompt 1): "
        ).strip()

    raw_input_path = raw_input_path.strip('"').strip("'")
    folder = Path(raw_input_path).expanduser().resolve()

    if not folder.is_dir():
        print(f"ERROR: '{folder}' is not a valid directory.")
        sys.exit(1)

    return folder


def get_output_directory(input_dir: Path) -> Path:
    out_dir = input_dir.parent / OUTPUT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


def load_application_level_data(input_dir: Path, logger: logging.Logger) -> pd.DataFrame:
    """
    Loads the harmonized application-level dataset produced by
    hmda_pipeline.py. Does NOT touch any raw HMDA ZIP/CSV file.

    Only loads COLUMNS_NEEDED_FOR_PANEL (lei, activity_year, action_taken,
    primary_AUS) instead of all ~50 columns, to avoid MemoryError on large
    national files (e.g. ~20M rows x 50 columns as object dtype can exceed
    available RAM on typical machines; 4 columns is a small fraction of
    that footprint).

    Prefers Parquet (faster, preserves dtypes, and supports column
    pruning at the file-format level so unneeded columns are never even
    read off disk); falls back to CSV if the Parquet file isn't present
    or if reading it fails for any reason.
    """
    parquet_path = input_dir / "application_level.parquet"
    csv_path = input_dir / "application_level.csv"

    if parquet_path.exists():
        try:
            logger.info(f"Loading {parquet_path} (columns-limited: {COLUMNS_NEEDED_FOR_PANEL})")
            df = pd.read_parquet(parquet_path, columns=COLUMNS_NEEDED_FOR_PANEL)
            logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns from Parquet.")
            return df
        except ImportError:
            logger.warning(
                "application_level.parquet exists but no Parquet engine is installed "
                "(pip install pyarrow). Falling back to CSV."
            )
        except Exception as exc:
            logger.warning(f"Could not read {parquet_path} ({exc}). Falling back to CSV.")

    if csv_path.exists():
        logger.info(f"Loading {csv_path} (columns-limited: {COLUMNS_NEEDED_FOR_PANEL})")
        # Read only the needed columns as string to avoid pandas silently
        # coercing codes (e.g. lei as a number, activity_year as float).
        df = pd.read_csv(
            csv_path, usecols=COLUMNS_NEEDED_FOR_PANEL, dtype=str,
            keep_default_na=True, na_values=["", "NA"],
        )
        logger.info(f"Loaded {len(df):,} rows, {len(df.columns)} columns from CSV.")
        return df

    logger.error(
        f"Neither application_level.parquet nor application_level.csv found in {input_dir}. "
        f"Run hmda_pipeline.py (Prompt 1) first."
    )
    sys.exit(1)


# ======================================================================
# SECTION 3: STEP 4 (part 1) -- PRE-AGGREGATION VALIDATION
# ======================================================================

def validate_before_aggregation(df: pd.DataFrame, logger: logging.Logger) -> (pd.DataFrame, dict):
    """
    Detect and EXCLUDE (never silently drop without logging) rows with a
    missing lei or missing activity_year, since these cannot be assigned
    to any (LEI, Year) group. Returns the cleaned dataframe plus a dict of
    counts for the aggregation log / QC report.
    """
    issues = {}
    n_start = len(df)

    missing_lei_mask = df["lei"].isna() | (df["lei"].astype(str).str.strip() == "")
    n_missing_lei = int(missing_lei_mask.sum())
    issues["missing_lei_rows_excluded"] = n_missing_lei
    if n_missing_lei > 0:
        logger.warning(f"Excluding {n_missing_lei:,} rows with missing/blank lei.")

    df = df.loc[~missing_lei_mask]

    missing_year_mask = df["activity_year"].isna() | (df["activity_year"].astype(str).str.strip() == "")
    n_missing_year = int(missing_year_mask.sum())
    issues["missing_year_rows_excluded"] = n_missing_year
    if n_missing_year > 0:
        logger.warning(f"Excluding {n_missing_year:,} rows with missing/blank activity_year.")

    df = df.loc[~missing_year_mask]

    n_end = len(df)
    issues["rows_before_cleaning"] = n_start
    issues["rows_after_cleaning"] = n_end
    logger.info(f"Pre-aggregation cleaning: {n_start:,} -> {n_end:,} rows "
                f"({n_start - n_end:,} excluded for missing lei/year).")

    return df, issues


# ======================================================================
# SECTION 4: STEP 1 + STEP 2 -- BUILD THE (LEI, YEAR) PANEL
# ======================================================================

AUS_CATEGORIES = ["Desktop Underwriter (DU)", "Loan Product Advisor (LPA)",
                   "Internal/Proprietary", "Other", "Missing"]


def build_lender_year_panel(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """
    Aggregate the harmonized application-level dataframe to exactly one
    row per (lei, activity_year), computing every Step-2 variable.

    Everything here operates on the ALREADY-FILTERED harmonized sample
    from Prompt 1 (owner-occupied, site-built, 1-4 family, home purchase,
    first lien, conventional, conforming, action_taken in {1,2,3}).
    Consequently application_count == approval_count + denial_count
    exactly, for every group -- this identity is checked in Step 4.
    """
    df = df.copy()

    # action_taken and primary_AUS may have been read as string (from CSV
    # fallback) or already string (from Parquet, since hmda_pipeline.py
    # wrote them as string dtype) -- normalize defensively either way.
    df["action_taken"] = df["action_taken"].astype(str).str.strip()
    if "primary_AUS" not in df.columns:
        raise RuntimeError(
            "Input is missing 'primary_AUS' -- this column is expected to already exist "
            "in application_level.parquet/.csv from hmda_pipeline.py (Prompt 1). "
            "Re-run Prompt 1 if it's missing."
        )
    df["primary_AUS"] = df["primary_AUS"].astype(str).str.strip()

    df["is_approval"] = df["action_taken"].isin(["1", "2"])
    df["is_denial"] = df["action_taken"] == "3"

    for category in AUS_CATEGORIES:
        col_name = f"_aus_flag_{category}"
        df[col_name] = (df["primary_AUS"] == category)

    grouped = df.groupby(["lei", "activity_year"], dropna=False)

    panel = grouped.agg(
        application_count=("action_taken", "size"),
        approval_count=("is_approval", "sum"),
        denial_count=("is_denial", "sum"),
        _du_count=(f"_aus_flag_{AUS_CATEGORIES[0]}", "sum"),
        _lpa_count=(f"_aus_flag_{AUS_CATEGORIES[1]}", "sum"),
        _internal_count=(f"_aus_flag_{AUS_CATEGORIES[2]}", "sum"),
        _other_count=(f"_aus_flag_{AUS_CATEGORIES[3]}", "sum"),
        _missing_aus_count=(f"_aus_flag_{AUS_CATEGORIES[4]}", "sum"),
    ).reset_index()

    panel = panel.rename(columns={"activity_year": "year"})

    # --- Step 2 rate/share variables (see LENDER_YEAR_VARIABLE_DEFINITIONS) ---
    panel["approval_rate"] = panel["approval_count"] / panel["application_count"]
    panel["denial_rate"] = panel["denial_count"] / panel["application_count"]

    panel["du_share"] = panel["_du_count"] / panel["application_count"]
    panel["lpa_share"] = panel["_lpa_count"] / panel["application_count"]
    panel["internal_aus_share"] = panel["_internal_count"] / panel["application_count"]
    panel["other_aus_share"] = (panel["_other_count"] + panel["_missing_aus_count"]) / panel["application_count"]

    panel["external_aus_coverage"] = panel["du_share"] + panel["lpa_share"]
    panel["external_aus_coverage_alt_any_aus_reported"] = 1 - (
        panel["_missing_aus_count"] / panel["application_count"]
    )

    # Drop internal helper count columns -- not part of the deliverable schema,
    # but keep them available for debugging via a separate diagnostic export.
    diagnostic_cols = ["_du_count", "_lpa_count", "_internal_count", "_other_count", "_missing_aus_count"]
    panel_diagnostic = panel[["lei", "year"] + diagnostic_cols].copy()
    panel = panel.drop(columns=diagnostic_cols)

    final_columns = [
        "lei", "year", "application_count", "approval_count", "denial_count",
        "approval_rate", "denial_rate",
        "du_share", "lpa_share", "internal_aus_share", "other_aus_share",
        "external_aus_coverage", "external_aus_coverage_alt_any_aus_reported",
    ]
    panel = panel[final_columns]

    logger.info(f"Built lender-year panel: {len(panel):,} (LEI, Year) rows "
                f"from {len(df):,} application-level rows "
                f"({df['lei'].nunique():,} unique LEIs, "
                f"{df['activity_year'].nunique()} years).")

    return panel, panel_diagnostic


# ======================================================================
# SECTION 5: STEP 4 (part 2) -- POST-AGGREGATION QUALITY CHECKS
# ======================================================================

def run_quality_checks(panel: pd.DataFrame, pre_agg_issues: dict, logger: logging.Logger) -> dict:
    """
    Runs every Step-4 quality check and returns a structured results dict
    (used both for the printed/logged summary and the written QC report).
    Checks are DIAGNOSTIC -- they flag issues but do not silently modify
    or drop rows from the panel itself (any row genuinely too broken to
    keep would already have been excluded upstream in validate_before_aggregation).
    """
    results = {"pre_aggregation": pre_agg_issues}

    # ---- Duplicate LEI-year detection ----
    dup_mask = panel.duplicated(subset=["lei", "year"], keep=False)
    n_dup = int(dup_mask.sum())
    results["duplicate_lei_year_rows"] = n_dup
    if n_dup > 0:
        logger.error(f"QC FAILURE: {n_dup:,} duplicate (lei, year) rows found in the panel "
                      f"-- aggregation should make this impossible; investigate groupby logic.")
    else:
        logger.info("QC PASSED: no duplicate (lei, year) rows.")

    # ---- Missing LEI / missing year detection (within the panel itself; should be zero
    #      since validate_before_aggregation already excluded these upstream) ----
    n_missing_lei_panel = int(panel["lei"].isna().sum())
    n_missing_year_panel = int(panel["year"].isna().sum())
    results["panel_missing_lei"] = n_missing_lei_panel
    results["panel_missing_year"] = n_missing_year_panel
    if n_missing_lei_panel or n_missing_year_panel:
        logger.error(f"QC FAILURE: panel contains missing lei ({n_missing_lei_panel}) "
                      f"or missing year ({n_missing_year_panel}) after aggregation.")
    else:
        logger.info("QC PASSED: no missing lei/year in the panel.")

    # ---- Approval-rate validation ----
    bad_rate_mask = (panel["approval_rate"] < 0) | (panel["approval_rate"] > 1) | panel["approval_rate"].isna()
    n_bad_rate = int(bad_rate_mask.sum())
    results["invalid_approval_rate_rows"] = n_bad_rate

    identity_diff = (panel["approval_rate"] + panel["denial_rate"] - 1.0).abs()
    n_identity_violation = int((identity_diff > FLOAT_TOLERANCE).sum())
    results["approval_denial_identity_violations"] = n_identity_violation

    if n_bad_rate or n_identity_violation:
        logger.error(f"QC FAILURE: {n_bad_rate:,} rows with approval_rate outside [0,1]; "
                      f"{n_identity_violation:,} rows where approval_rate + denial_rate != 1.")
    else:
        logger.info("QC PASSED: approval_rate in [0,1] and approval_rate + denial_rate == 1 for all rows.")

    # ---- Share validation (DU + LPA + Internal + Other == 1) ----
    share_sum = panel["du_share"] + panel["lpa_share"] + panel["internal_aus_share"] + panel["other_aus_share"]
    share_diff = (share_sum - 1.0).abs()
    n_share_violation = int((share_diff > FLOAT_TOLERANCE).sum())
    results["aus_share_sum_violations"] = n_share_violation
    if n_share_violation > 0:
        logger.error(f"QC FAILURE: {n_share_violation:,} rows where "
                      f"du_share+lpa_share+internal_aus_share+other_aus_share != 1.")
    else:
        logger.info("QC PASSED: AUS shares (DU+LPA+Internal+Other) sum to 1 for every row.")

    # ---- Outlier detection ----
    # (a) "thin" lender-years: rates computed on very few applications.
    n_thin = int((panel["application_count"] < MIN_APPLICATIONS_FOR_STABLE_RATE).sum())
    results["thin_lender_years_flagged"] = n_thin
    results["thin_threshold_applications"] = MIN_APPLICATIONS_FOR_STABLE_RATE
    logger.info(f"Flagged {n_thin:,} lender-years with < {MIN_APPLICATIONS_FOR_STABLE_RATE} "
                f"applications (rates for these are statistically noisy, not necessarily wrong).")

    # (b) IQR-based outliers on application_count, computed WITHIN each year
    #     (lender size distributions differ by year; a global IQR would just
    #     flag "large banks" every year rather than true anomalies).
    outlier_flags = []
    for year, group in panel.groupby("year"):
        q1, q3 = group["application_count"].quantile([0.25, 0.75])
        iqr = q3 - q1
        lower = q1 - IQR_OUTLIER_MULTIPLIER * iqr
        upper = q3 + IQR_OUTLIER_MULTIPLIER * iqr
        flags = (group["application_count"] < lower) | (group["application_count"] > upper)
        outlier_flags.append(flags)
    application_count_outlier_mask = pd.concat(outlier_flags).sort_index()
    n_outliers = int(application_count_outlier_mask.sum())
    results["application_count_outliers_flagged"] = n_outliers
    logger.info(f"Flagged {n_outliers:,} lender-years as application_count outliers "
                f"(Tukey IQR method, computed within each year).")

    panel["_flag_thin"] = panel["application_count"] < MIN_APPLICATIONS_FOR_STABLE_RATE
    panel["_flag_application_count_outlier"] = application_count_outlier_mask.values

    return results


# ======================================================================
# SECTION 6: OUTPUT WRITERS (Step 5)
# ======================================================================

def write_variable_definitions(output_dir: Path) -> Path:
    path = output_dir / "variable_definitions.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "formula", "numerator", "denominator",
                          "missing_value_treatment", "interpretation"])
        for var, meta in LENDER_YEAR_VARIABLE_DEFINITIONS.items():
            writer.writerow([var, meta["formula"], meta["numerator"], meta["denominator"],
                              meta["missing_treatment"], meta["interpretation"]])
        writer.writerow([])
        writer.writerow(["--- CANDIDATE ADDITIONAL VARIABLES (Step 3, UNVERIFIED against paper) ---"])
        for var, note in ADDITIONAL_VARIABLE_CANDIDATES.items():
            writer.writerow([var, note])
    return path


def write_qc_report(qc_results: dict, output_dir: Path) -> Path:
    path = output_dir / "quality_control_report.txt"
    lines = ["LENDER-YEAR PANEL - QUALITY CONTROL REPORT",
             f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]
    lines.append(json.dumps(qc_results, indent=2))
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def write_summary_statistics(panel: pd.DataFrame, output_dir: Path) -> Path:
    path = output_dir / "summary_statistics.csv"
    numeric_cols = ["application_count", "approval_count", "denial_count",
                     "approval_rate", "denial_rate", "du_share", "lpa_share",
                     "internal_aus_share", "other_aus_share", "external_aus_coverage"]
    summary = panel[numeric_cols].describe().T
    summary.to_csv(path)
    return path


def sha256_of_file(path: Path, chunk_size: int = 8 * 1024 * 1024) -> str:
    sha256 = hashlib.sha256()
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha256.update(chunk)
    return sha256.hexdigest()


def write_checksums(file_paths: list, output_dir: Path) -> Path:
    path = output_dir / "SHA256SUMS.txt"
    lines = [f"{sha256_of_file(p)}  {p.name}" for p in file_paths if p.exists()]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ======================================================================
# SECTION 7: MAIN ORCHESTRATION
# ======================================================================

def main():
    input_dir = get_input_directory()
    output_dir = get_output_directory(input_dir)
    logger = setup_logging(output_dir)

    logger.info(f"Detected OS: {platform.system()} {platform.release()}")
    logger.info(f"Input directory (hmda_output from Prompt 1): {input_dir}")
    logger.info(f"Output directory: {output_dir}")

    # ---- Load application-level data (Prompt 1 output ONLY -- no raw HMDA files) ----
    app_df = load_application_level_data(input_dir, logger)

    # ---- Step 4 (pre-aggregation): missing lei / missing year detection + exclusion ----
    app_df, pre_agg_issues = validate_before_aggregation(app_df, logger)

    # ---- Steps 1-2: build the (LEI, Year) panel ----
    panel, panel_diagnostic = build_lender_year_panel(app_df, logger)

    # ---- Step 4 (post-aggregation): full quality-control suite ----
    qc_results = run_quality_checks(panel, pre_agg_issues, logger)

    # Drop internal QC flag columns from the DELIVERABLE panel (they're
    # diagnostic, not part of the requested variable set) but keep them
    # in a side file for transparency.
    qc_flag_cols = ["_flag_thin", "_flag_application_count_outlier"]
    panel_with_flags = panel.copy()
    panel = panel.drop(columns=qc_flag_cols)

    # ---- Step 5: write all outputs ----
    parquet_path = output_dir / "lender_year_panel.parquet"
    csv_path = output_dir / "lender_year_panel.csv"

    parquet_written = False
    try:
        panel.to_parquet(parquet_path, index=False)
        logger.info(f"Wrote {parquet_path}")
        parquet_written = True
    except ImportError:
        logger.warning(
            "Could not write Parquet output: no Parquet engine is installed. "
            "Run 'pip install pyarrow' (or 'conda install pyarrow') and re-run "
            "this script to also get lender_year_panel.parquet. "
            "Continuing with all other outputs."
        )

    panel.to_csv(csv_path, index=False)
    logger.info(f"Wrote {csv_path}")

    # Diagnostic side-files (not in Step 5's required list, but written for
    # transparency/debugging -- clearly named so they aren't mistaken for
    # the primary deliverable).
    panel_with_flags.to_csv(output_dir / "lender_year_panel_with_qc_flags.csv", index=False)
    panel_diagnostic.to_csv(output_dir / "aus_count_diagnostics.csv", index=False)

    var_def_path = write_variable_definitions(output_dir)
    qc_path = write_qc_report(qc_results, output_dir)
    summary_path = write_summary_statistics(panel, output_dir)

    logger.info(f"Wrote {var_def_path}")
    logger.info(f"Wrote {qc_path}")
    logger.info(f"Wrote {summary_path}")

    checksummed_files = [csv_path, var_def_path, qc_path, summary_path]
    if parquet_written:
        checksummed_files.insert(0, parquet_path)
    checksums_path = write_checksums(checksummed_files, output_dir)
    logger.info(f"Wrote {checksums_path}")

    # ---- Final summary ----
    logger.info("=" * 60)
    logger.info("LENDER-YEAR PANEL COMPLETE")
    logger.info(f"Panel rows (unique LEI-year combinations): {len(panel):,}")
    logger.info(f"Unique lenders (LEIs): {panel['lei'].nunique():,}")
    logger.info(f"Years covered: {sorted(panel['year'].unique())}")
    logger.info(f"QC issues found: duplicates={qc_results['duplicate_lei_year_rows']}, "
                f"identity_violations={qc_results['approval_denial_identity_violations']}, "
                f"share_violations={qc_results['aus_share_sum_violations']}")
    logger.info(f"All outputs written to: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()


# In[ ]:




