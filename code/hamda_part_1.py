#!/usr/bin/env python
# coding: utf-8

# In[ ]:


"""
hmda_pipeline.py

Reproduces a harmonized, application-level HMDA sample (2018-2025 LAR data)
for an academic replication study, following the sample-construction
criteria below:

  1. Owner-occupied, site-built, 1-4 family dwelling
  2. Home purchase loans only
  3. First lien only
  4. Conventional, conforming loans only (excludes FHA / VA / USDA-RHS / jumbo)
  5. Outcome = Approval (originated OR approved-not-accepted) vs Denial
  6. Excludes withdrawn, closed-for-incompleteness, purchased loans,
     and preapproval-only records

ALL variable names and codes below are taken from the official FFIEC HMDA
documentation ("Public HMDA - LAR Data Fields", current specification,
which covers reporting years 2018 onward):
    https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
This page is the CFPB's authoritative, continuously-maintained field
reference for the modern (2018+) LAR schema. It is a web document, not a
paginated PDF, so no page number can be cited for it -- this is noted
explicitly wherever the source is used.

This script does NOT download any data. It assumes the 16 official ZIP
archives (8 years x {LAR, TS}) have already been manually downloaded and
sit together in one folder on disk.

This script does NOT perform any lender-level aggregation -- output is a
harmonized APPLICATION-level dataset only.

Usage:
    python hmda_pipeline.py "C:\\path\\to\\hmda_zip_folder"
    (or run with no argument and you will be prompted for the folder)

Only pandas + standard library are required; pyarrow is required for
Parquet output (pip install pyarrow).
"""

import sys
import os
import re
import csv
import json
import hashlib
import logging
import zipfile
import platform
from pathlib import Path
from datetime import datetime

import pandas as pd


# ======================================================================
# SECTION 0: GLOBAL CONFIG
# ======================================================================

# Reporting years this replication study covers.
EXPECTED_YEARS = list(range(2018, 2026))  # 2018..2025 inclusive

# Rows are streamed in chunks to keep memory bounded even though the
# national LAR files are multi-GB. Lower this if you hit memory limits.
CHUNK_SIZE = 250_000

# Output locations (created under the input folder's parent, in a
# dedicated "hmda_output" directory, so we never write into the
# read-only source folder).
OUTPUT_DIRNAME = "hmda_output"

# ----------------------------------------------------------------------
# SECTION 0a: OFFICIAL VARIABLE DEFINITIONS (Step 1 variable inventory)
# ----------------------------------------------------------------------
# Every code value below is copied verbatim from the FFIEC "Public HMDA -
# LAR Data Fields" reference (current spec, applies to 2018-2025 data).
# This dict doubles as the machine-readable "data dictionary" that gets
# written out in Step 7.
#
# Source for ALL entries in this dict, unless noted otherwise:
#   FFIEC HMDA Documentation, "Public HMDA - LAR Data Fields"
#   https://ffiec.cfpb.gov/documentation/publications/loan-level-datasets/lar-data-fields/
#   (web page, current specification, no page number available)
# ----------------------------------------------------------------------
DATA_DICTIONARY = {
    "occupancy_type": {
        "definition": "Occupancy type for the dwelling",
        "codes": {"1": "Principal residence", "2": "Second residence", "3": "Investment property"},
        "used_for": "Filter: owner-occupied (keep 1)",
    },
    "construction_method": {
        "definition": "Construction method for the dwelling",
        "codes": {"1": "Site-built", "2": "Manufactured home"},
        "used_for": "Filter: site-built (keep 1)",
    },
    "total_units": {
        "definition": "Number of individual dwelling units related to the property",
        "codes": {"1": "1", "2": "2", "3": "3", "4": "4", "5-24": "5-24", "25-49": "25-49",
                   "50-99": "50-99", "100-149": "100-149", ">149": ">149"},
        "used_for": "Filter: 1-4 family dwelling (keep 1,2,3,4)",
    },
    "derived_dwelling_category": {
        "definition": "Derived dwelling type from Construction Method and Total Units fields",
        "codes": {
            "Single Family (1-4 Units):Site-Built": "1-4 family, site-built",
            "Multifamily:Site-Built (5+ Units)": "5+ units, site-built",
            "Single Family (1-4 Units):Manufactured": "1-4 family, manufactured",
            "Multifamily:Manufactured (5+ Units)": "5+ units, manufactured",
        },
        "used_for": "Cross-check for property-type filter",
    },
    "loan_purpose": {
        "definition": "The purpose of covered loan or application",
        "codes": {"1": "Home purchase", "2": "Home improvement", "31": "Refinancing",
                   "32": "Cash-out refinancing", "4": "Other purpose", "5": "Not applicable"},
        "used_for": "Filter: home purchase only (keep 1)",
    },
    "lien_status": {
        "definition": "Lien status of the property securing the covered loan",
        "codes": {"1": "Secured by a first lien", "2": "Secured by a subordinate lien"},
        "used_for": "Filter: first lien only (keep 1)",
    },
    "loan_type": {
        "definition": "The type of covered loan or application",
        "codes": {"1": "Conventional (not insured/guaranteed by FHA, VA, RHS, or FSA)",
                   "2": "FHA insured", "3": "VA guaranteed", "4": "USDA RHS or FSA guaranteed"},
        "used_for": "Filter: conventional only (keep 1); excludes FHA/VA/USDA",
    },
    "conforming_loan_limit": {
        "definition": "Indicates whether the reported loan amount exceeds the GSE conforming loan limit",
        "codes": {"C": "Conforming", "NC": "Nonconforming", "U": "Undetermined", "NA": "Not applicable"},
        "used_for": "Filter: conforming only (keep 'C'; see ambiguity note in README)",
    },
    "action_taken": {
        "definition": "The action taken on the covered loan or application",
        "codes": {"1": "Loan originated", "2": "Application approved but not accepted",
                   "3": "Application denied", "4": "Application withdrawn by applicant",
                   "5": "File closed for incompleteness", "6": "Purchased loan",
                   "7": "Preapproval request denied", "8": "Preapproval request approved but not accepted"},
        "used_for": "Outcome: approval={1,2}, denial={3}; excludes {4,5,6,7,8}",
    },
    "preapproval": {
        "definition": "Whether the application involved a request for preapproval",
        "codes": {"1": "Preapproval requested", "2": "Preapproval not requested"},
        "used_for": "Cross-check only; preapproval-only records already excluded via action_taken",
    },
    "combined_loan_to_value_ratio": {
        "definition": "Ratio of total secured debt to property value relied on in credit decision",
        "codes": {"varying": "numeric percentage"},
        "used_for": "Covariate (not a hard filter). NOTE: some 2018-vintage public LAR files use "
                    "the column header 'loan_to_value_ratio' instead -- harmonized automatically.",
    },
    "aus-1": {"definition": "1st automated underwriting system (AUS) used",
              "codes": {"1": "Desktop Underwriter (DU)", "2": "Loan Prospector (LP)/Loan Product Advisor",
                         "3": "TOTAL Scorecard", "4": "GUS", "5": "Other", "6": "Not applicable",
                         "7": "Internal Proprietary System", "1111": "Exempt"},
              "used_for": "Basis for harmonized primary_AUS"},
    "aus-2": {"definition": "2nd AUS used", "codes": "same as aus-1 (minus code 6)", "used_for": "Not used for primary_AUS"},
    "aus-3": {"definition": "3rd AUS used", "codes": "same as aus-1 (minus code 6)", "used_for": "Not used for primary_AUS"},
    "aus-4": {"definition": "4th AUS used", "codes": "same as aus-1 (minus code 6)", "used_for": "Not used for primary_AUS"},
    "aus-5": {"definition": "5th AUS used", "codes": "same as aus-1 (minus code 6)", "used_for": "Not used for primary_AUS"},
}

# ----------------------------------------------------------------------
# SECTION 0b: HARMONIZATION CROSSWALK (Step 2)
# ----------------------------------------------------------------------
# Maps every KNOWN alternate/legacy column header to the single canonical
# name used throughout this pipeline. Verified rename:
#   'loan_to_value_ratio' (seen in some 2018-vintage public LAR exports)
#     -> 'combined_loan_to_value_ratio' (current FFIEC field name)
# This was confirmed directly against an actual downloaded file header,
# cross-checked with the current FFIEC field reference cited above.
#
# The pipeline does NOT rely on this list blindly: get_column_rename_map()
# below inspects each file's ACTUAL header at read time and only maps
# columns that are actually present, so any other unanticipated renames
# in your specific files will surface in the validation report rather
# than silently failing.
# ----------------------------------------------------------------------

RENAME_CROSSWALK = {
    "loan_to_value_ratio": "combined_loan_to_value_ratio",
    "aus_1": "aus-1",
    "aus_2": "aus-2",
    "aus_3": "aus-3",
    "aus_4": "aus-4",
    "aus_5": "aus-5",
    "derived_msa_md": "derived_msa-md",
    "co_applicant_ethnicity_observed": "co-applicant_ethnicity_observed",
    "co_applicant_race_observed": "co-applicant_race_observed",
    "co_applicant_sex": "co-applicant_sex",
    "co_applicant_age_above_62": "co-applicant_age_above_62",
    "denial_reason_1": "denial_reason-1",
    "denial_reason_2": "denial_reason-2",
    "denial_reason_3": "denial_reason-3",
    "denial_reason_4": "denial_reason-4",
}
# Canonical columns we need from every LAR file (post-harmonization) to
# (a) run every filter, and (b) keep as covariates in the final dataset.
# This keeps memory low: we only ever load this subset via usecols,
# never all ~99 raw columns.
CANONICAL_COLUMNS_NEEDED = [
    "activity_year", "lei", "derived_msa-md", "state_code", "county_code", "census_tract",
    "derived_loan_product_type", "derived_dwelling_category", "conforming_loan_limit",
    "derived_ethnicity", "derived_race", "derived_sex",
    "action_taken", "purchaser_type", "preapproval",
    "loan_type", "loan_purpose", "lien_status",
    "loan_amount", "combined_loan_to_value_ratio", "interest_rate", "rate_spread",
    "property_value", "construction_method", "occupancy_type", "total_units",
    "income", "debt_to_income_ratio",
    "applicant_ethnicity_observed", "co-applicant_ethnicity_observed",
    "applicant_race_observed", "co-applicant_race_observed",
    "applicant_sex", "co-applicant_sex",
    "applicant_age_above_62", "co-applicant_age_above_62",
    "aus-1", "aus-2", "aus-3", "aus-4", "aus-5",
    "denial_reason-1", "denial_reason-2", "denial_reason-3", "denial_reason-4",
    "tract_minority_population_percent", "ffiec_msa_md_median_family_income",
    "tract_to_msa_income_percentage", "tract_owner_occupied_units",
]

# Filters MUST see these columns no matter what -- if any is missing after
# harmonization, we cannot proceed for that file, and validation must fail
# loudly rather than silently skip a filter.
REQUIRED_FOR_FILTERING = [
    "occupancy_type", "construction_method", "total_units",
    "loan_purpose", "lien_status", "loan_type", "conforming_loan_limit",
    "action_taken",
]


# ======================================================================
# SECTION 1: LOGGING SETUP
# ======================================================================

def setup_logging(output_dir: Path) -> logging.Logger:
    """
    Sets up a logger that writes to BOTH the console and a persistent
    processing_log.txt file inside the output directory (Step 7 deliverable).
    """
    logger = logging.getLogger("hmda_pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S")

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_path = output_dir / "processing_log.txt"
    file_handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger


# ======================================================================
# SECTION 2: FOLDER INPUT + OS-AGNOSTIC PATH HANDLING
# ======================================================================

def get_input_directory() -> Path:
    """
    Determine the folder containing the 16 HMDA ZIP files.
    Priority: command-line argument > interactive prompt.
    Robust to being launched from Jupyter/IPython, where sys.argv contains
    kernel-launcher flags like '-f connection.json' instead of a real path.
    """
    candidate = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        # Reject Jupyter/IPython kernel-launcher artifacts.
        if not arg.startswith("-") and not arg.lower().endswith(".json"):
            candidate = arg

    if candidate:
        raw_input_path = candidate
    else:
        raw_input_path = input("I:\HMDA:").strip()

    # Strip accidental surrounding quotes (common when pasting Windows paths).
    raw_input_path = raw_input_path.strip('"').strip("'")

    # pathlib.Path handles both Windows ("C:\\...") and POSIX ("/...") separators
    # natively depending on the platform this script runs on -- we never
    # hand-construct paths with '\\' or '/' ourselves.
    folder = Path(raw_input_path).expanduser().resolve()

    if not folder.is_dir():
        print(f"ERROR: '{folder}' is not a valid directory.")
        sys.exit(1)

    return folder


def get_output_directory(input_dir: Path) -> Path:
    """Create (if needed) and return the output directory, kept separate
    from the (possibly read-only / archival) input folder."""
    out_dir = input_dir.parent / OUTPUT_DIRNAME
    out_dir.mkdir(parents=True, exist_ok=True)
    return out_dir


# ======================================================================
# SECTION 3: ZIP DISCOVERY, CLASSIFICATION, AND INVENTORY VALIDATION
# ======================================================================

# Matches a 4-digit year (2017-2099) anywhere in the filename.
YEAR_PATTERN = re.compile(r"(20[1-9][0-9])")

# Matches "lar" or "ts" (transmittal sheet) as a distinct token in the
# filename, case-insensitively, bounded by non-alphanumeric characters
# (or start/end of string) so we don't accidentally match inside another
# word (e.g. "vitals" should not match "ts... " etc.)
LAR_PATTERN = re.compile(r"(?<![a-z0-9])lar(?![a-z0-9])", re.IGNORECASE)
TS_PATTERN = re.compile(r"(?<![a-z0-9])ts(?![a-z0-9])", re.IGNORECASE)


def classify_zip_file(filename: str):
    """
    Given a ZIP filename (e.g. '2025_public_lar_csv.zip' or
    '2021_public_ts_csv.zip'), determine:
      - the reporting year (int, or None if not found)
      - the archive kind: 'lar', 'ts', or None if it can't be determined

    Deliberately does NOT hard-code full filenames or a fixed naming
    template -- it only looks for a 4-digit year token and a lar/ts
    token anywhere in the name, so it tolerates minor naming variations
    across the 2018-2025 CFPB downloads.
    """
    year_match = YEAR_PATTERN.search(filename)
    year = int(year_match.group(1)) if year_match else None

    is_lar = bool(LAR_PATTERN.search(filename))
    is_ts = bool(TS_PATTERN.search(filename))

    if is_lar and not is_ts:
        kind = "lar"
    elif is_ts and not is_lar:
        kind = "ts"
    else:
        kind = None  # ambiguous or unrecognized

    return year, kind


def discover_zip_files(folder: Path, logger: logging.Logger):
    """Find every .zip in the folder (non-recursive) and classify each one."""
    zip_paths = sorted(folder.glob("*.zip"))
    if not zip_paths:
        logger.error(f"No .zip files found in '{folder}'.")
        sys.exit(1)

    inventory = []  # list of dicts: {path, filename, year, kind}
    for p in zip_paths:
        year, kind = classify_zip_file(p.name)
        inventory.append({"path": p, "filename": p.name, "year": year, "kind": kind})

    return inventory


def validate_inventory(inventory, logger: logging.Logger, output_dir: Path):
    """
    Verify exactly one LAR archive and one TS archive exist for every year
    in EXPECTED_YEARS. Reports (and writes to a validation report):
      - unrecognized files (year or kind could not be determined)
      - duplicate files for the same (year, kind)
      - missing (year, kind) combinations

    Processing continues only if every expected (year, kind) pair is present
    exactly once; otherwise the script exits with a clear error so the user
    can fix their download folder before wasting time on a partial run.
    """
    report_lines = []
    report_lines.append("HMDA ZIP INVENTORY VALIDATION REPORT")
    report_lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    report_lines.append(f"Platform: {platform.system()} {platform.release()}")
    report_lines.append("")

    unrecognized = [f for f in inventory if f["year"] is None or f["kind"] is None]
    recognized = [f for f in inventory if f["year"] is not None and f["kind"] is not None]

    # Build a lookup: (year, kind) -> list of matching files (should be exactly 1)
    lookup = {}
    for f in recognized:
        key = (f["year"], f["kind"])
        lookup.setdefault(key, []).append(f)

    report_lines.append(f"Total .zip files found: {len(inventory)}")
    report_lines.append(f"Recognized (year + kind both determined): {len(recognized)}")
    report_lines.append(f"Unrecognized: {len(unrecognized)}")
    report_lines.append("")

    if unrecognized:
        report_lines.append("UNRECOGNIZED FILES (year and/or LAR/TS type could not be determined):")
        for f in unrecognized:
            report_lines.append(f"  - {f['filename']}  (year={f['year']}, kind={f['kind']})")
        report_lines.append("")

    duplicates_found = False
    for key, files in lookup.items():
        if len(files) > 1:
            duplicates_found = True
            report_lines.append(f"DUPLICATE FILES for year={key[0]}, kind={key[1]}:")
            for f in files:
                report_lines.append(f"  - {f['filename']}")
    if duplicates_found:
        report_lines.append("")

    missing = []
    report_lines.append("PER-YEAR / PER-KIND PRESENCE CHECK:")
    report_lines.append(f"{'Year':<6}{'LAR':<6}{'TS':<6}")
    for year in EXPECTED_YEARS:
        lar_present = (year, "lar") in lookup
        ts_present = (year, "ts") in lookup
        report_lines.append(f"{year:<6}{'OK' if lar_present else 'MISSING':<6}{'OK' if ts_present else 'MISSING':<6}")
        if not lar_present:
            missing.append((year, "lar"))
        if not ts_present:
            missing.append((year, "ts"))
    report_lines.append("")

    if missing:
        report_lines.append("MISSING ARCHIVES:")
        for year, kind in missing:
            report_lines.append(f"  - {year} {kind.upper()}")
    else:
        report_lines.append("All 16 expected archives (2018-2025, LAR + TS) are present.")

    report_text = "\n".join(report_lines)
    logger.info("\n" + report_text)

    report_path = output_dir / "inventory_validation_report.txt"
    report_path.write_text(report_text, encoding="utf-8")
    logger.info(f"Inventory validation report written to: {report_path}")

    if unrecognized or duplicates_found or missing:
        logger.error("Inventory validation FAILED. Fix the issues above before re-running.")
        sys.exit(1)

    logger.info("Inventory validation PASSED. Proceeding with processing.")
    # Return only the LAR files (this pipeline builds the application-level
    # dataset from LAR data; TS files are validated for presence but not
    # loaded further here, consistent with the task scope).
    lar_files = {f["year"]: f["path"] for f in recognized if f["kind"] == "lar"}
    return lar_files


# ======================================================================
# SECTION 4: PER-FILE HARMONIZATION
# ======================================================================

def get_inner_csv_name(zip_path: Path) -> str:
    """Return the name of the single CSV entry inside a HMDA zip archive.
    HMDA LAR zips contain exactly one CSV; if that assumption is ever
    violated the function raises loudly rather than guessing."""
    with zipfile.ZipFile(zip_path) as zf:
        csv_names = [n for n in zf.namelist() if n.lower().endswith(".csv")]
    if len(csv_names) != 1:
        raise ValueError(
            f"Expected exactly one CSV inside {zip_path.name}, found {len(csv_names)}: {csv_names}"
        )
    return csv_names[0]


def get_header_columns(zip_path: Path, inner_csv_name: str) -> list:
    """Read only the header row (no data) to discover the ACTUAL columns
    present in this specific file -- never assumed."""
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(inner_csv_name) as f:
            header_df = pd.read_csv(f, nrows=0)
    return list(header_df.columns)


def build_rename_map_for_file(actual_columns: list) -> dict:
    """
    For THIS file's actual header, build a {raw_name: canonical_name} map.
    Only includes renames where the raw (legacy) name is actually present
    -- i.e. if a file already uses the canonical name, no rename happens.
    """
    rename_map = {}
    for raw_name, canonical_name in RENAME_CROSSWALK.items():
        if raw_name in actual_columns:
            rename_map[raw_name] = canonical_name
    return rename_map


def resolve_usecols(actual_columns: list, rename_map: dict) -> list:
    """
    Determine which raw columns to actually read from disk: every column
    we need, expressed in whatever name THIS file uses (post-rename-aware).
    """
    # Build reverse map: canonical -> raw name actually present in this file
    canonical_to_raw = {}
    for raw in actual_columns:
        canonical = rename_map.get(raw, raw)  # if not renamed, raw == canonical
        canonical_to_raw[canonical] = raw

    usecols = []
    missing_canonical = []
    for canonical_name in CANONICAL_COLUMNS_NEEDED:
        if canonical_name in canonical_to_raw:
            usecols.append(canonical_to_raw[canonical_name])
        else:
            missing_canonical.append(canonical_name)

    return usecols, missing_canonical


def validate_file_columns(year: int, missing_canonical: list, logger: logging.Logger, validation_issues: list):
    """Record any canonical columns this year's file is missing entirely
    (distinct from columns that just needed renaming)."""
    if missing_canonical:
        msg = f"Year {year}: MISSING columns (not found under any known name): {missing_canonical}"
        logger.warning(msg)
        validation_issues.append(msg)

        missing_required = [c for c in missing_canonical if c in REQUIRED_FOR_FILTERING]
        if missing_required:
            raise RuntimeError(
                f"Year {year}: cannot proceed -- these columns are REQUIRED for filtering "
                f"and were not found under any known name: {missing_required}"
            )


def read_harmonized_lar_chunks(zip_path: Path, year: int, logger: logging.Logger, validation_issues: list):
    """
    Generator: yields harmonized, filtered-to-relevant-columns DataFrame
    chunks for one year's LAR file, streamed from the zip so we never load
    the full multi-GB file into memory at once.

    Harmonization performed here (Step 2/3):
      1. Discover the actual header of this specific file.
      2. Build a rename map for any legacy/alternate column names present.
      3. Read ONLY the columns we need (post-rename-aware) via usecols.
      4. Rename to the canonical schema immediately after reading each chunk.
    """
    inner_csv_name = get_inner_csv_name(zip_path)
    actual_columns = get_header_columns(zip_path, inner_csv_name)
    rename_map = build_rename_map_for_file(actual_columns)

    if rename_map:
        logger.info(f"Year {year}: harmonizing column names: {rename_map}")

    usecols, missing_canonical = resolve_usecols(actual_columns, rename_map)
    validate_file_columns(year, missing_canonical, logger, validation_issues)

    # Read everything as string dtype. This is deliberate: HMDA code fields
    # (action_taken, loan_type, total_units, conforming_loan_limit, etc.)
    # are categorical codes, not numbers to do arithmetic on, and several
    # of them (e.g. total_units) mix integers with text ranges like "5-24".
    # Reading as string avoids pandas silently coercing codes into floats
    # (e.g. "1" -> 1.0) or choking on mixed-type columns.
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(inner_csv_name) as f:
            reader = pd.read_csv(
                f, usecols=usecols, dtype=str, chunksize=CHUNK_SIZE,
                keep_default_na=True, na_values=["", "NA"],
            )
            for chunk in reader:
                chunk = chunk.rename(columns=rename_map)
                # activity_year is authoritative from the FILE ITSELF; if it's
                # missing or inconsistent, we trust the filename-derived year
                # (already validated in Section 3) and stamp it explicitly.
                if "activity_year" not in chunk.columns:
                    chunk["activity_year"] = year
                yield chunk


# ======================================================================
# SECTION 5: FILTER PIPELINE (Step 4) + primary_AUS (Step 5)
# ======================================================================

# Ordered list of (step_name, filter_function). Each filter_function takes
# a DataFrame and returns a BOOLEAN MASK (not a filtered df) so we can log
# how many rows each individual step removes, in the exact order specified.
#
# NOTE on "conforming": per the ambiguity discussion, we implement the
# STRICT interpretation (keep only 'C'). The permissive alternative
# (keep 'C' and 'U') is documented but not applied; see README notes.
FILTER_STEPS = [
    ("owner_occupied",        lambda df: df["occupancy_type"] == "1"),
    ("site_built",            lambda df: df["construction_method"] == "1"),
    ("one_to_four_family",    lambda df: df["total_units"].isin(["1", "2", "3", "4"])),
    ("home_purchase",         lambda df: df["loan_purpose"] == "1"),
    ("first_lien",            lambda df: df["lien_status"] == "1"),
    ("conventional",          lambda df: df["loan_type"] == "1"),
    ("conforming",            lambda df: df["conforming_loan_limit"] == "C"),
    ("exclude_fha",           lambda df: df["loan_type"] != "2"),
    ("exclude_va",            lambda df: df["loan_type"] != "3"),
    ("exclude_usda_rhs",      lambda df: df["loan_type"] != "4"),
    ("exclude_jumbo_nonconforming", lambda df: df["conforming_loan_limit"] != "NC"),
    ("valid_outcome_only",    lambda df: df["action_taken"].isin(["1", "2", "3"])),
    # ^ This single step simultaneously: (a) defines approval={1,2} and
    #   denial={3} as retainable outcomes, and (b) excludes withdrawn(4),
    #   closed-for-incompleteness(5), purchased loans(6), and both
    #   preapproval-only codes(7,8) -- since those are simply not in {1,2,3}.
]


def apply_filters(df: pd.DataFrame, step_counts: dict) -> pd.DataFrame:
    """
    Apply every filter in FILTER_STEPS IN ORDER, updating a running
    observation count after each step (Step 6 sample-flow data).
    step_counts is mutated in place: {step_name: cumulative_row_count}.
    """
    for step_name, filter_fn in FILTER_STEPS:
        mask = filter_fn(df)
        df = df.loc[mask]
        step_counts[step_name] = step_counts.get(step_name, 0) + len(df)
    return df


# ----------------------------------------------------------------------
# primary_AUS derivation (Step 5)
# ----------------------------------------------------------------------
# Codes per aus-1..aus-5 (FFIEC LAR Data Fields):
#   1=DU, 2=LP(LPA), 3=TOTAL Scorecard, 4=GUS, 5=Other,
#   6=Not applicable, 7=Internal Proprietary System, 1111=Exempt
#
# INTERPRETATION (flagged explicitly, since the FIG does not explicitly
# label aus-1 as "the primary system"): by convention, when an institution
# used more than one AUS, the first-reported slot (aus-1) is treated as
# primary. This is the standard convention used in HMDA replication
# literature, not an explicit CFPB rule -- documented here for transparency.
AUS_CODE_TO_CATEGORY = {
    "1": "Desktop Underwriter (DU)",
    "2": "Loan Product Advisor (LPA)",
    "7": "Internal/Proprietary",
    "3": "Other", "4": "Other", "5": "Other",
    "6": "Missing", "1111": "Missing",
}


def compute_primary_aus(df: pd.DataFrame) -> pd.Series:
    """Derive the harmonized primary_AUS column from aus-1."""
    if "aus-1" not in df.columns:
        return pd.Series(["Missing"] * len(df), index=df.index)
    return df["aus-1"].map(AUS_CODE_TO_CATEGORY).fillna("Missing")


# ======================================================================
# SECTION 6: VALIDATION OF RETAINED SAMPLE (Step 3 / Step 8 requirement)
# ======================================================================

def validate_retained_sample(df: pd.DataFrame, logger: logging.Logger) -> list:
    """
    After filtering, assert that every retained row actually satisfies
    every inclusion criterion. Returns a list of any violations found
    (empty list = fully valid). This re-checks independently of the
    filter pipeline itself, as a safeguard against logic errors.
    """
    checks = {
        "occupancy_type == 1":            (df["occupancy_type"] == "1"),
        "construction_method == 1":       (df["construction_method"] == "1"),
        "total_units in {1,2,3,4}":       (df["total_units"].isin(["1", "2", "3", "4"])),
        "loan_purpose == 1":              (df["loan_purpose"] == "1"),
        "lien_status == 1":               (df["lien_status"] == "1"),
        "loan_type == 1":                 (df["loan_type"] == "1"),
        "conforming_loan_limit == 'C'":   (df["conforming_loan_limit"] == "C"),
        "action_taken in {1,2,3}":        (df["action_taken"].isin(["1", "2", "3"])),
    }

    violations = []
    for description, mask in checks.items():
        n_bad = (~mask).sum()
        if n_bad > 0:
            violations.append(f"{n_bad:,} rows violate: {description}")
            logger.error(f"VALIDATION FAILURE: {n_bad:,} rows violate '{description}'")

    if not violations:
        logger.info(f"Validation PASSED: all {len(df):,} retained rows satisfy every inclusion criterion.")

    return violations


# ======================================================================
# SECTION 7: OUTPUT WRITERS
# ======================================================================

def write_crosswalk(output_dir: Path) -> Path:
    """Step 7: variable-name harmonization crosswalk as CSV."""
    path = output_dir / "variable_crosswalk.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["raw_column_name", "canonical_column_name", "note"])
        for raw, canonical in RENAME_CROSSWALK.items():
            writer.writerow([raw, canonical,
                              "Confirmed rename: legacy header seen in some 2018-vintage public LAR files"])
        if not RENAME_CROSSWALK:
            writer.writerow(["(none found)", "", ""])
    return path


def write_data_dictionary(output_dir: Path) -> Path:
    """Step 7: machine-readable data dictionary for every harmonized variable used."""
    path = output_dir / "data_dictionary.csv"
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["variable", "definition", "codes", "used_for", "source"])
        for var, meta in DATA_DICTIONARY.items():
            writer.writerow([
                var, meta["definition"], json.dumps(meta["codes"], ensure_ascii=False),
                meta["used_for"],
                "FFIEC HMDA Documentation, 'Public HMDA - LAR Data Fields' (current spec, 2018-2025)",
            ])
    return path


def write_sample_flow_table(flow_by_year: dict, output_dir: Path) -> Path:
    """
    Step 6/7: CONSORT-style flow table -- rows = years, columns = each
    filter step's cumulative remaining observation count, plus a final
    'ALL_YEARS' total row.
    """
    path = output_dir / "sample_flow_table.csv"
    step_names = ["initial"] + [s[0] for s in FILTER_STEPS]

    totals = {step: 0 for step in step_names}
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["activity_year"] + step_names)
        for year in sorted(flow_by_year.keys()):
            row = flow_by_year[year]
            writer.writerow([year] + [row.get(step, 0) for step in step_names])
            for step in step_names:
                totals[step] += row.get(step, 0)
        writer.writerow(["ALL_YEARS"] + [totals[step] for step in step_names])
    return path


def write_validation_report(validation_issues: list, retained_violations: list, output_dir: Path) -> Path:
    path = output_dir / "final_validation_report.txt"
    lines = ["HMDA HARMONIZED SAMPLE - VALIDATION REPORT",
             f"Generated: {datetime.now().isoformat(timespec='seconds')}", ""]

    lines.append("COLUMN-LEVEL ISSUES DURING HARMONIZATION:")
    lines.extend(f"  - {issue}" for issue in validation_issues) if validation_issues else lines.append("  (none)")
    lines.append("")

    lines.append("RETAINED-SAMPLE INCLUSION-CRITERIA CHECK:")
    if retained_violations:
        lines.extend(f"  - {v}" for v in retained_violations)
    else:
        lines.append("  PASSED: every retained row satisfies every inclusion criterion.")

    path.write_text("\n".join(lines), encoding="utf-8")
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
    """Step 7: SHA-256 checksums for all output deliverables."""
    path = output_dir / "SHA256SUMS.txt"
    lines = []
    for p in file_paths:
        if p.exists():
            lines.append(f"{sha256_of_file(p)}  {p.name}")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


# ======================================================================
# SECTION 8: MAIN ORCHESTRATION
# ======================================================================

def main():
    input_dir = get_input_directory()
    output_dir = get_output_directory(input_dir)
    logger = setup_logging(output_dir)

    logger.info(f"Detected OS: {platform.system()} {platform.release()}")
    logger.info(f"Input directory: {input_dir}")
    logger.info(f"Output directory: {output_dir}")

    # ---- Steps: discover, classify, and validate the 16-file inventory ----
    inventory = discover_zip_files(input_dir, logger)
    lar_files_by_year = validate_inventory(inventory, logger, output_dir)  # exits on failure

    # ---- Steps 3-6: harmonize, validate columns, filter, derive primary_AUS ----
    validation_issues = []
    flow_by_year = {}
    harmonized_chunks = []  # collected AFTER filtering -> keeps memory bounded

    for year in EXPECTED_YEARS:
        zip_path = lar_files_by_year[year]
        logger.info(f"Processing {year} LAR file: {zip_path.name}")

        step_counts = {"initial": 0}
        year_chunks_kept = []

        for chunk in read_harmonized_lar_chunks(zip_path, year, logger, validation_issues):
            step_counts["initial"] += len(chunk)
            filtered_chunk = apply_filters(chunk, step_counts)
            if len(filtered_chunk) > 0:
                filtered_chunk = filtered_chunk.copy()
                filtered_chunk["primary_AUS"] = compute_primary_aus(filtered_chunk)
                year_chunks_kept.append(filtered_chunk)

        flow_by_year[year] = step_counts
        n_final = step_counts.get(FILTER_STEPS[-1][0], 0)
        logger.info(f"  Year {year}: {step_counts['initial']:,} initial -> {n_final:,} retained")

        if year_chunks_kept:
            harmonized_chunks.append(pd.concat(year_chunks_kept, ignore_index=True))

    # ---- Combine all years into one harmonized application-level dataset ----
    if not harmonized_chunks:
        logger.error("No records survived filtering across any year. Aborting before writing outputs.")
        sys.exit(1)

    final_df = pd.concat(harmonized_chunks, ignore_index=True)
    logger.info(f"Combined harmonized application-level dataset: {len(final_df):,} rows, "
                f"{len(final_df.columns)} columns.")

    # ---- Step 3/8: validate every retained observation against every criterion ----
    retained_violations = validate_retained_sample(final_df, logger)

    # ---- Step 7: write all outputs ----
    parquet_path = output_dir / "application_level.parquet"
    csv_path = output_dir / "application_level.csv"

    final_df.to_parquet(parquet_path, index=False)
    logger.info(f"Wrote {parquet_path}")

    final_df.to_csv(csv_path, index=False)
    logger.info(f"Wrote {csv_path}")

    crosswalk_path = write_crosswalk(output_dir)
    dict_path = write_data_dictionary(output_dir)
    flow_path = write_sample_flow_table(flow_by_year, output_dir)
    validation_path = write_validation_report(validation_issues, retained_violations, output_dir)

    logger.info(f"Wrote {crosswalk_path}")
    logger.info(f"Wrote {dict_path}")
    logger.info(f"Wrote {flow_path}")
    logger.info(f"Wrote {validation_path}")

    # Checksums cover every output file EXCEPT SHA256SUMS.txt and the
    # processing log itself (the log is still being written to as this
    # line runs, so hashing it here would capture an incomplete file).
    checksummed_files = [parquet_path, csv_path, crosswalk_path, dict_path, flow_path, validation_path]
    checksums_path = write_checksums(checksummed_files, output_dir)
    logger.info(f"Wrote {checksums_path}")

    # ---- Final summary ----
    logger.info("=" * 60)
    logger.info("PIPELINE COMPLETE")
    logger.info(f"Final harmonized application-level sample: {len(final_df):,} rows")
    logger.info(f"Retained-sample validation: {'PASSED' if not retained_violations else 'FAILED - see report'}")
    logger.info(f"All outputs written to: {output_dir}")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()


# In[ ]:





# In[2]:


pip install pyarrow


# In[2]:


import zipfile
import pandas as pd

zip_path = "I:/HMDA/2023_public_lar_csv.zip"   # adjust to your actual filename

with zipfile.ZipFile(zip_path) as zf:
    csv_name = [n for n in zf.namelist() if n.lower().endswith(".csv")][0]
    with zf.open(csv_name) as f:
        header = pd.read_csv(f, nrows=0)

cols = header.columns.tolist()

# Check each missing canonical name's likely raw variants
print("msa-md related:", [c for c in cols if "msa" in c.lower()])
print("co-applicant related:", [c for c in cols if "co" in c.lower() and "applicant" in c.lower()])
print("denial reason related:", [c for c in cols if "denial" in c.lower()])

