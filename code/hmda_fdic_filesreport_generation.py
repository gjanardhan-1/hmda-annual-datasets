#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
validate_fdic_financials.py

Validates the 8 downloaded FDIC financials CSVs (fdic_2018_q4.csv ...
fdic_2025_q4.csv) before any further processing:
  1. Confirms all 8 expected files are present.
  2. Validates uniqueness on (year, CERT) WITHIN each file -- a duplicate
     CERT in a single report-date pull means overlapping report types
     were accidentally included (e.g. both a bank record and a
     holding-company record) and the query needs a tighter filter.
  3. Reports row counts and flags any file that fails validation.

Usage:
    python validate_fdic_financials.py "C:\\path\\to\\fdic_csv_folder"
"""

import sys
import re
from pathlib import Path
from datetime import datetime

import pandas as pd

EXPECTED_YEARS = list(range(2018, 2026))
FILENAME_PATTERN = re.compile(r"fdic_(20[1-9][0-9])_q4\.csv", re.IGNORECASE)


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
    csv_files = sorted(folder.glob("fdic_*_q4.csv"))

    files_by_year = {}
    for f in csv_files:
        m = FILENAME_PATTERN.match(f.name)
        if m:
            files_by_year[int(m.group(1))] = f

    report_lines = [
        "FDIC FINANCIALS - VALIDATION REPORT",
        f"Generated: {datetime.now().isoformat(timespec='seconds')}",
        "",
        "FILE PRESENCE CHECK:",
    ]

    missing_years = [y for y in EXPECTED_YEARS if y not in files_by_year]
    for year in EXPECTED_YEARS:
        status = "OK" if year in files_by_year else "MISSING"
        report_lines.append(f"  {year}: {status}")

    if missing_years:
        report_lines.append(f"\nMISSING YEARS: {missing_years}")
        print("\n".join(report_lines))
        print("\nCannot proceed with validation until all 8 files are present.")
        sys.exit(1)

    report_lines.append("\nAll 8 expected files (2018-2025) present.\n")
    report_lines.append("PER-FILE UNIQUENESS CHECK ON (year, CERT):")

    any_failures = False
    total_rows = 0
    for year in EXPECTED_YEARS:
        path = files_by_year[year]
        df = pd.read_csv(path, dtype=str)

        if "CERT" not in df.columns:
            report_lines.append(f"  {year}: ERROR -- no 'CERT' column found in {path.name}")
            any_failures = True
            continue

        n_rows = len(df)
        total_rows += n_rows
        n_unique_cert = df["CERT"].nunique()
        n_duplicates = n_rows - n_unique_cert

        if n_duplicates > 0:
            any_failures = True
            dup_certs = df[df.duplicated(subset=["CERT"], keep=False)]["CERT"].unique()
            report_lines.append(
                f"  {year}: FAIL -- {n_rows:,} rows, {n_unique_cert:,} unique CERT "
                f"({n_duplicates:,} duplicate rows across {len(dup_certs):,} CERTs). "
                f"Example duplicated CERTs: {list(dup_certs[:5])}"
            )
        else:
            report_lines.append(f"  {year}: PASS -- {n_rows:,} rows, all CERT values unique")

    report_lines.append(f"\nTotal rows across all 8 files: {total_rows:,}")
    report_lines.append(f"\nOVERALL RESULT: {'FAILED -- see duplicates above' if any_failures else 'PASSED'}")

    report_text = "\n".join(report_lines)
    print(report_text)

    report_path = folder / "fdic_financials_validation_report.txt"
    report_path.write_text(report_text, encoding="utf-8")
    print(f"\nReport written to: {report_path}")

    if any_failures:
        sys.exit(1)


if __name__ == "__main__":
    main()


# In[ ]:




