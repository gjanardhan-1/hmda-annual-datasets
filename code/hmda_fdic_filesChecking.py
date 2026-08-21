#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
check_fdic_column_consistency.py

Checks whether all 8 downloaded FDIC financials CSVs (fdic_2018_q4.csv ...
fdic_2025_q4.csv) share the EXACT same set of columns, in case the API
silently dropped/reordered a field in any given year's response.

For each file, this script records:
  - the exact column names present (and their order)
  - whether every column matches a "baseline" set (first file found)
  - which columns are MISSING relative to the baseline (present in some
    years, absent in others)
  - which columns are EXTRA relative to the baseline
  - for the numeric fields, what fraction of values in each file
    actually parse as numbers (a field can technically "exist" as a
    column but be entirely blank/non-numeric in a given year, which
    the presence/absence check alone would not catch)

Usage:
    python check_fdic_column_consistency.py "C:\\path\\to\\fdic_csv_folder"

Outputs (written into the same folder as the CSVs):
    column_consistency_report.csv   (one row per column x year: present? position? %-numeric?)
    column_consistency_summary.txt  (human-readable pass/fail summary)

Upload BOTH files back for review once you've run this against your
real downloaded data.
"""

import sys
import re
import csv
from pathlib import Path
from datetime import datetime

import pandas as pd

EXPECTED_YEARS = list(range(2018, 2026))
FILENAME_PATTERN = re.compile(r"fdic_(20[1-9][0-9])_q4\.csv", re.IGNORECASE)

# Fields you specifically requested from the API -- used to flag numeric
# parse-rate issues (a field can technically exist as a column but be
# entirely blank in a given year's response).
NUMERIC_FIELDS = ["ASSET", "ESAL", "NONIXAY", "ASTEMPM", "ROA", "RBC1AAJ", "NCLNLSR", "NUMEMP"]


def get_input_directory() -> Path:
    candidate = None
    if len(sys.argv) >= 2:
        arg = sys.argv[1]
        if not arg.startswith("-") and not arg.lower().endswith(".json"):
            candidate = arg
    if candidate:
        raw = candidate
    else:
        raw = input("Enter the folder containing the fdic_YYYY_q4.csv files: ").strip()
    raw = raw.strip('"').strip("'")
    folder = Path(raw).expanduser().resolve()
    if not folder.is_dir():
        print(f"ERROR: '{folder}' is not a valid directory.")
        sys.exit(1)
    return folder


def main():
    folder = get_input_directory()

    files_by_year = {}
    for f in sorted(folder.glob("fdic_*_q4.csv")):
        m = FILENAME_PATTERN.match(f.name)
        if m:
            files_by_year[int(m.group(1))] = f

    missing_years = [y for y in EXPECTED_YEARS if y not in files_by_year]
    if missing_years:
        print(f"ERROR: missing files for years: {missing_years}. "
              f"All 8 files (2018-2025) must be present to run this check.")
        sys.exit(1)

    # ------------------------------------------------------------------
    # Pass 1: read headers only (fast) to build the full column inventory
    # across all 8 files, and pick a baseline (union of ALL columns seen,
    # NOT just the first file -- this way a column that's merely absent
    # from year 1 but present elsewhere is still tracked, not missed).
    # ------------------------------------------------------------------
    columns_by_year = {}
    for year, path in files_by_year.items():
        header_df = pd.read_csv(path, nrows=0)
        columns_by_year[year] = list(header_df.columns)

    all_columns_seen = set()
    for cols in columns_by_year.values():
        all_columns_seen.update(cols)
    all_columns_seen = sorted(all_columns_seen)

    baseline_year = EXPECTED_YEARS[0]
    baseline_columns = set(columns_by_year[baseline_year])

    # ------------------------------------------------------------------
    # Pass 2: for each numeric field, compute % of non-null values that
    # successfully parse as a number, per year (full read required here).
    # ------------------------------------------------------------------
    numeric_parse_rates = {}  # {(field, year): pct_numeric_of_nonblank}
    blank_rates = {}          # {(field, year): pct_blank_of_all_rows}
    row_counts = {}
    for year, path in files_by_year.items():
        df = pd.read_csv(path, dtype=str)
        row_counts[year] = len(df)
        for field in NUMERIC_FIELDS:
            if field not in df.columns:
                numeric_parse_rates[(field, year)] = None  # column doesn't exist at all
                blank_rates[(field, year)] = None
                continue
            series = df[field]
            non_blank = series.notna() & (series.str.strip() != "")
            n_non_blank = non_blank.sum()
            pct_blank = round(100 * (1 - n_non_blank / len(series)), 2) if len(series) > 0 else None
            blank_rates[(field, year)] = pct_blank
            if n_non_blank == 0:
                numeric_parse_rates[(field, year)] = None  # column exists but 100% blank
                continue
            parsed = pd.to_numeric(series[non_blank], errors="coerce")
            pct_numeric = round(100 * parsed.notna().mean(), 2)
            numeric_parse_rates[(field, year)] = pct_numeric

    # ------------------------------------------------------------------
    # Write column_consistency_report.csv: one row per (column, year)
    # ------------------------------------------------------------------
    report_csv_path = folder / "column_consistency_report.csv"
    with open(report_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["column", "year", "present", "position_in_file",
                          "in_baseline_set", "pct_blank_of_all_rows", "pct_numeric_of_nonblank_values"])
        for column in all_columns_seen:
            in_baseline = column in baseline_columns
            for year in EXPECTED_YEARS:
                cols_this_year = columns_by_year[year]
                present = column in cols_this_year
                position = cols_this_year.index(column) if present else ""
                pct_blank = blank_rates.get((column, year), "")
                pct_numeric = numeric_parse_rates.get((column, year), "")
                writer.writerow([column, year, present, position, in_baseline, pct_blank, pct_numeric])

    # ------------------------------------------------------------------
    # Write column_consistency_summary.txt: human-readable pass/fail
    # ------------------------------------------------------------------
    lines = [
        "FDIC FINANCIALS - COLUMN CONSISTENCY SUMMARY",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        f"Baseline year used for comparison: {baseline_year}",
        "",
        "ROW COUNTS PER FILE:",
    ]
    for year in EXPECTED_YEARS:
        lines.append(f"  {year}: {row_counts[year]:,} rows, {len(columns_by_year[year])} columns")

    lines.append("")
    lines.append(f"TOTAL DISTINCT COLUMN NAMES SEEN ACROSS ALL 8 FILES: {len(all_columns_seen)}")
    lines.append(f"COLUMNS IN BASELINE ({baseline_year}): {len(baseline_columns)}")
    lines.append("")

    any_mismatch = False
    lines.append("PER-YEAR COLUMN-SET COMPARISON AGAINST BASELINE:")
    for year in EXPECTED_YEARS:
        cols_this_year = set(columns_by_year[year])
        missing_vs_baseline = baseline_columns - cols_this_year
        extra_vs_baseline = cols_this_year - baseline_columns

        if not missing_vs_baseline and not extra_vs_baseline:
            lines.append(f"  {year}: MATCH -- identical column set to baseline ({baseline_year})")
        else:
            any_mismatch = True
            lines.append(f"  {year}: MISMATCH")
            if missing_vs_baseline:
                lines.append(f"      Missing (in baseline, not in {year}): {sorted(missing_vs_baseline)}")
            if extra_vs_baseline:
                lines.append(f"      Extra (in {year}, not in baseline): {sorted(extra_vs_baseline)}")

    lines.append("")
    lines.append("COLUMN ORDER CHECK (informational -- order differences don't break pandas "
                  "column-name-based code, but flagged in case you have any position-based logic):")
    for year in EXPECTED_YEARS:
        if columns_by_year[year] != columns_by_year[baseline_year] and            set(columns_by_year[year]) == set(columns_by_year[baseline_year]):
            lines.append(f"  {year}: same columns as baseline but DIFFERENT ORDER")

    lines.append("")
    lines.append("NUMERIC FIELD COMPLETENESS:")
    lines.append("  ('% blank' = share of ALL rows with no value; '% numeric' = share of the")
    lines.append("   NON-blank values that successfully parse as a number -- these are independent checks.)")
    for field in NUMERIC_FIELDS:
        lines.append(f"  {field}:")
        for year in EXPECTED_YEARS:
            rate = numeric_parse_rates.get((field, year))
            pct_blank = blank_rates.get((field, year))
            if rate is None and pct_blank is None:
                lines.append(f"    {year}: column missing entirely")
            elif rate is None:
                lines.append(f"    {year}: column present but 100% blank")
            else:
                flag = ""
                if pct_blank and pct_blank > 0:
                    flag += f" [{pct_blank}% BLANK]"
                if rate < 100:
                    flag += f" [{rate}% OF NON-BLANK VALUES FAIL TO PARSE AS NUMERIC]"
                lines.append(f"    {year}: {rate}% of non-blank values numeric{flag}" if flag
                             else f"    {year}: 100% populated and numeric")

    lines.append("")
    lines.append(f"OVERALL RESULT: {'MISMATCHES FOUND -- see above' if any_mismatch else 'ALL 8 FILES HAVE IDENTICAL COLUMN SETS'}")

    summary_text = "\n".join(lines)
    print(summary_text)

    summary_path = folder / "column_consistency_summary.txt"
    summary_path.write_text(summary_text, encoding="utf-8")

    print(f"\nWrote: {report_csv_path}")
    print(f"Wrote: {summary_path}")
    print("\nUpload both files back for review.")


if __name__ == "__main__":
    main()

