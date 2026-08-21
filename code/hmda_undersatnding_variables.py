#!/usr/bin/env python
# coding: utf-8

# In[1]:


"""
inspect_large_csv.py

Inspect a large CSV file (e.g. > 4 GB) WITHOUT loading it fully into memory.

It will tell you:
  1. The full list of variables (columns) and their inferred dtypes
  2. Whether the file contains a date/time column
  3. Whether the data is available at MONTHLY granularity or only ANNUAL
     (i.e. one row per year vs. multiple rows per year / a month field)
  4. Basic row count and file size

Usage:
    python inspect_large_csv.py "I:\\H\\your_file.csv"

Requires: pandas  (pip install pandas)
"""

import sys
import os
import pandas as pd
from collections import Counter

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
INPUT_CSV_PATH = r"I:\HMDA\year_2024.csv"   # <-- EDIT THIS to your real file path

CHUNK_SIZE = 200_000          # rows per chunk while scanning
SAMPLE_ROWS_FOR_DTYPE = 5000  # rows used just to peek at dtypes/values quickly
DATE_COL_CANDIDATES = [
    "date", "month", "year", "period", "yyyymm", "yyyy_mm",
    "obs_date", "timestamp", "time", "year_month", "date_time"
]

def human_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def find_date_like_columns(columns):
    """Heuristic match of column names that look like date/time fields."""
    matches = []
    for c in columns:
        c_lower = str(c).strip().lower()
        for cand in DATE_COL_CANDIDATES:
            if cand in c_lower:
                matches.append(c)
                break
    return matches


def main(csv_path):
    if not os.path.isfile(csv_path):
        print(f"ERROR: File not found: {csv_path}")
        sys.exit(1)

    file_size = os.path.getsize(csv_path)
    print("=" * 70)
    print(f"File: {csv_path}")
    print(f"Size: {human_size(file_size)}")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Read ONLY the header to get column names (cheap, instant)
    # ------------------------------------------------------------------
    header_df = pd.read_csv(csv_path, nrows=0)
    columns = list(header_df.columns)
    print(f"\nNumber of columns/variables: {len(columns)}")
    print("\nVariable list:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:3d}. {col}")

    # ------------------------------------------------------------------
    # 2. Peek at a small sample to guess dtypes and find date-like columns
    # ------------------------------------------------------------------
    sample = pd.read_csv(csv_path, nrows=SAMPLE_ROWS_FOR_DTYPE)
    print("\nInferred dtypes (from first {} rows):".format(SAMPLE_ROWS_FOR_DTYPE))
    for col in columns:
        print(f"  {col:30s} -> {sample[col].dtype}")

    date_like_cols = find_date_like_columns(columns)
    if not date_like_cols:
        print("\nNo obviously date/month/year-named column found by name.")
        print("Trying to auto-detect a parseable date column from the sample...")
        for col in columns:
            try:
                parsed = pd.to_datetime(sample[col], errors="coerce")
                if parsed.notna().mean() > 0.8:  # mostly parseable
                    date_like_cols.append(col)
            except Exception:
                pass

    if not date_like_cols:
        print("\n>>> Could not identify any date/time column. "
              "Please inspect column names manually above.")
        return

    print(f"\nCandidate date/time column(s): {date_like_cols}")

    # ------------------------------------------------------------------
    # 3. Stream through the FULL file in chunks to determine granularity
    #    - collect unique (year, month) pairs seen
    #    - collect unique years seen
    #    - count total rows
    # ------------------------------------------------------------------
    print("\nScanning full file in chunks to determine time granularity "
          "(this may take a while for large files)...")

    years_seen = set()
    year_months_seen = set()
    total_rows = 0
    unparseable = 0

    # Use the first date-like column found
    date_col = date_like_cols[0]

    for chunk_num, chunk in enumerate(
        pd.read_csv(csv_path, usecols=[date_col], chunksize=CHUNK_SIZE), start=1
    ):
        total_rows += len(chunk)
        parsed = pd.to_datetime(chunk[date_col], errors="coerce")
        unparseable += parsed.isna().sum()

        valid = parsed.dropna()
        years_seen.update(valid.dt.year.unique().tolist())
        year_months_seen.update(
            list(zip(valid.dt.year, valid.dt.month))
        )

        if chunk_num % 20 == 0:
            print(f"  ... processed {total_rows:,} rows so far")

    print(f"\nTotal rows scanned: {total_rows:,}")
    print(f"Rows where '{date_col}' could not be parsed as a date: {unparseable:,}")
    print(f"Distinct years found: {sorted(years_seen)}")
    print(f"Distinct (year, month) combinations found: {len(year_months_seen)}")

    # ------------------------------------------------------------------
    # 4. Decide: monthly or annual
    # ------------------------------------------------------------------
    avg_months_per_year = (
        len(year_months_seen) / len(years_seen) if years_seen else 0
    )

    print("\n" + "=" * 70)
    print("CONCLUSION")
    print("=" * 70)
    if avg_months_per_year >= 2:
        print(f"Data appears to be MONTHLY (or sub-annual): "
              f"~{avg_months_per_year:.1f} distinct months per year on average.")
    elif avg_months_per_year > 0:
        print(f"Data appears to be ANNUAL: only ~{avg_months_per_year:.1f} "
              f"month(s) represented per year (i.e., roughly one observation "
              f"per year).")
    else:
        print("Could not determine granularity — check the date column parsing above.")


if __name__ == "__main__":
    # Priority: command-line argument (if given) > INPUT_CSV_PATH set above
    if len(sys.argv) == 2:
        path_to_use = sys.argv[1]
    else:
        path_to_use = INPUT_CSV_PATH
    main(path_to_use)


# In[2]:


"""
inspect_large_csv.py

Inspect a large CSV file (e.g. > 4 GB) WITHOUT loading it fully into memory.

It will tell you:
  1. The full list of variables (columns) and their inferred dtypes
  2. Whether the file contains a date/time column
  3. Whether the data is available at MONTHLY granularity or only ANNUAL
     (i.e. one row per year vs. multiple rows per year / a month field)
  4. Basic row count and file size

Usage:
    python inspect_large_csv.py "I:\\H\\your_file.csv"

Requires: pandas  (pip install pandas)
"""

import sys
import os
import pandas as pd
from collections import Counter

# ----------------------------------------------------------------------
# CONFIG
# ----------------------------------------------------------------------
INPUT_CSV_PATH = r"I:\HMDA\year_2024.csv"    # <-- EDIT THIS to your real file path

CHUNK_SIZE = 200_000          # rows per chunk while scanning
SAMPLE_ROWS_FOR_DTYPE = 5000  # rows used just to peek at dtypes/values quickly
DATE_COL_CANDIDATES = [
    "date", "month", "year", "period", "yyyymm", "yyyy_mm",
    "obs_date", "timestamp", "time", "year_month", "date_time"
]

def human_size(num_bytes):
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if num_bytes < 1024:
            return f"{num_bytes:.2f} {unit}"
        num_bytes /= 1024
    return f"{num_bytes:.2f} PB"


def find_date_like_columns(columns):
    """Heuristic match of column names that look like date/time fields."""
    matches = []
    for c in columns:
        c_lower = str(c).strip().lower()
        for cand in DATE_COL_CANDIDATES:
            if cand in c_lower:
                matches.append(c)
                break
    return matches


def main(csv_path):
    if not os.path.isfile(csv_path):
        print(f"ERROR: File not found: {csv_path}")
        sys.exit(1)

    file_size = os.path.getsize(csv_path)
    print("=" * 70)
    print(f"File: {csv_path}")
    print(f"Size: {human_size(file_size)}")
    print("=" * 70)

    # ------------------------------------------------------------------
    # 1. Read ONLY the header to get column names (cheap, instant)
    # ------------------------------------------------------------------
    header_df = pd.read_csv(csv_path, nrows=0)
    columns = list(header_df.columns)
    print(f"\nNumber of columns/variables: {len(columns)}")
    print("\nVariable list:")
    for i, col in enumerate(columns, 1):
        print(f"  {i:3d}. {col}")

    # ------------------------------------------------------------------
    # 2. Peek at a small sample to guess dtypes and find date-like columns
    # ------------------------------------------------------------------
    sample = pd.read_csv(csv_path, nrows=SAMPLE_ROWS_FOR_DTYPE)
    print("\nInferred dtypes (from first {} rows):".format(SAMPLE_ROWS_FOR_DTYPE))
    for col in columns:
        print(f"  {col:30s} -> {sample[col].dtype}")

    date_like_cols = find_date_like_columns(columns)
    if not date_like_cols:
        print("\nNo obviously date/month/year-named column found by name.")
        print("Trying to auto-detect a parseable date column from the sample...")
        for col in columns:
            try:
                parsed = pd.to_datetime(sample[col], errors="coerce")
                if parsed.notna().mean() > 0.8:  # mostly parseable
                    date_like_cols.append(col)
            except Exception:
                pass

    if not date_like_cols:
        print("\n>>> Could not identify any date/time column. "
              "Please inspect column names manually above.")
        return

    print(f"\nCandidate date/time column(s): {date_like_cols}")

    # ------------------------------------------------------------------
    # 3. Pick the actual year column to use.
    #    Many datasets (e.g. HMDA) store a plain 4-digit YEAR integer
    #    (e.g. 2023) rather than a real date. Feeding a bare integer like
    #    2023 into pd.to_datetime() is WRONG -- pandas will interpret it
    #    as nanoseconds-since-1970-epoch and silently give you 1970.
    #    So: only treat a column as a real calendar date if it actually
    #    LOOKS like a date string/number (e.g. "2023-05-01" or 20230501),
    #    not a bare year like 2023.
    # ------------------------------------------------------------------
    def looks_like_bare_year_column(series):
        vals = pd.to_numeric(series, errors="coerce").dropna()
        if vals.empty:
            return False
        # bare years are small integers roughly in range 1900-2100
        return vals.between(1900, 2100).mean() > 0.95

    date_col = date_like_cols[0]
    is_bare_year = looks_like_bare_year_column(sample[date_col])

    total_rows = 0

    if is_bare_year:
        # -------------------------------------------------------------
        # Case A: this is just a YEAR field (no month/day info exists
        # anywhere in the file). Just count rows per year.
        # -------------------------------------------------------------
        print(f"\n'{date_col}' looks like a plain YEAR field (e.g. 2018, 2019, "
              f"2023...), not a full date. There is no month/day information "
              f"to extract from this column alone.")

        year_counts = Counter()
        for chunk_num, chunk in enumerate(
            pd.read_csv(csv_path, usecols=[date_col], chunksize=CHUNK_SIZE), start=1
        ):
            total_rows += len(chunk)
            yrs = pd.to_numeric(chunk[date_col], errors="coerce").dropna().astype(int)
            year_counts.update(yrs.tolist())
            if chunk_num % 20 == 0:
                print(f"  ... processed {total_rows:,} rows so far")

        print(f"\nTotal rows scanned: {total_rows:,}")
        print("\nRow count per year:")
        for yr in sorted(year_counts):
            print(f"  {yr}: {year_counts[yr]:,} rows")

        print("\n" + "=" * 70)
        print("CONCLUSION")
        print("=" * 70)
        print(f"'{date_col}' only records a YEAR (no month/day field exists "
              f"anywhere in this file's columns). This means the dataset is "
              f"published/aggregated at ANNUAL granularity by design -- there "
              f"is no way to check 'are all 12 months present' because month "
              f"was never recorded. Each row is one record (e.g. one loan "
              f"application) tagged with the year it occurred in, not a "
              f"specific month.")
        print(f"\nYears present in the file: {sorted(year_counts.keys())}")

    else:
        # -------------------------------------------------------------
        # Case B: this looks like a real date/datetime column.
        # Check whether all 12 months are present for each year.
        # -------------------------------------------------------------
        print(f"\nScanning full file in chunks to check monthly coverage "
              f"per year (this may take a while for large files)...")

        year_months_seen = {}  # {year: set(months)}
        unparseable = 0

        for chunk_num, chunk in enumerate(
            pd.read_csv(csv_path, usecols=[date_col], chunksize=CHUNK_SIZE), start=1
        ):
            total_rows += len(chunk)
            parsed = pd.to_datetime(chunk[date_col], errors="coerce")
            unparseable += parsed.isna().sum()

            valid = parsed.dropna()
            for yr, mo in zip(valid.dt.year, valid.dt.month):
                year_months_seen.setdefault(yr, set()).add(mo)

            if chunk_num % 20 == 0:
                print(f"  ... processed {total_rows:,} rows so far")

        print(f"\nTotal rows scanned: {total_rows:,}")
        print(f"Rows where '{date_col}' could not be parsed as a date: {unparseable:,}")

        print("\n" + "=" * 70)
        print("MONTHLY COVERAGE PER YEAR")
        print("=" * 70)
        all_months = set(range(1, 13))
        for yr in sorted(year_months_seen):
            months_present = year_months_seen[yr]
            missing = sorted(all_months - months_present)
            status = "COMPLETE (all 12 months)" if not missing else f"INCOMPLETE (missing months: {missing})"
            print(f"  {yr}: {len(months_present)}/12 months present -> {status}")


if __name__ == "__main__":
    # Priority: command-line argument (if given) > INPUT_CSV_PATH set above
    if len(sys.argv) == 2:
        path_to_use = sys.argv[1]
    else:
        path_to_use = INPUT_CSV_PATH
    main(path_to_use)

