# HMDA National Loan-Level Source Archives (2018–2025)

## Research purpose

This repository supports the replication package for **[PAPER TITLE PLACEHOLDER]**. It provides the public HMDA (Home Mortgage Disclosure Act) national loan-level data used in the analysis, along with code and documentation needed to reproduce the results.

## Data source

The raw annual files (`year_2018.csv` through `year_2025.csv`) are unmodified downloads from the **https://ffiec.cfpb.gov/data-browser/**:

- Source: https://ffiec.cfpb.gov/data-browser/ (or the exact URL used — update this line)
- Reporting years: 2018–2025
- File status: Unchanged official public snapshots, as downloaded

## Repository contents

| File | Description |
|---|---|
| `README.md` | This file |
| `source_manifest.csv` | Per-year record of original filename, source URL, download date, size, and checksum |
| `SHA256SUMS.txt` | SHA-256 checksums of the original raw CSV files |
| `SHA256SUMS_archives.txt` | SHA-256 checksums of the compressed `.7z` Release assets (for download integrity) |
| `code/` | Processing and analysis scripts |

The large raw data files are **not** stored in this Git repository. They are distributed as compressed archives attached to a [GitHub Release](../../releases) (see below).

## Where the raw data lives

Each year's raw CSV is compressed into a single `.7z` archive and uploaded as a Release asset:

| Year | Original file | Release archive |
|---|---|---|
| 2018 | `year_2018.csv` | `hmda_2018.7z.001` |
| 2019 | `year_2019.csv` | `hmda_2019.7z.001` |
| 2020 | `year_2020.csv` | `hmda_2020.7z.001` |
| 2021 | `year_2021.csv` | `hmda_2021.7z.001` |
| 2022 | `year_2022.csv` | `hmda_2022.7z.001` |
| 2023 | `year_2023.csv` | `hmda_2023.7z.001` |
| 2024 | `year_2024.csv` | `hmda_2024.7z.001` |
| 2025 | `year_2025.csv` | `hmda_2025.7z.001` |

Each archive is a complete, single-volume `.7z` file (the `.001` suffix is retained from the splitting workflow, but no further parts exist — there is no `.002`).

Download the Release: **https://github.com/gjanardhan-1/hmda-annual-datasets**

## How to reconstruct and verify the data

1. Download the `.7z.001` file for the year you need from the Release page.
2. Extract it with [7-Zip](https://www.7-zip.org/): right-click the file → **7-Zip → Extract Here**. This produces the original `year_20XX.csv`.
3. Verify integrity by hashing the extracted file and comparing it against `SHA256SUMS.txt`:

   **Windows (PowerShell):**
   ```powershell
   Get-FileHash .\year_2018.csv -Algorithm SHA256
   ```

   **macOS/Linux:**
   ```bash
   shasum -a 256 year_2018.csv
   ```

4. The resulting hash must exactly match the corresponding line in `SHA256SUMS.txt`. If it does not, re-download the archive — the file may be corrupted or incomplete.

(Optional) You can also verify the downloaded archive itself, before extraction, against `SHA256SUMS_archives.txt`.

## Software required

- [7-Zip](https://www.7-zip.org/) (or any `.7z`-compatible extraction tool) to unpack the archives
- [PAPER-SPECIFIC: list analysis software/language/packages here, e.g. R, Python, Stata]

## Derived data and code

- `code/` — [describe processing pipeline here]
- Public FDIC data and the lender crosswalk used for institution-level linkage are [describe location/source here]

## License / data use note

The HMDA data distributed here are unmodified public releases from the CFPB/FFIEC and are subject to their original terms of use. Code in this repository is **licensed under [LICENSE PLACEHOLDER]**.

## Contact

Questions about this replication package can be directed to **[]**.
