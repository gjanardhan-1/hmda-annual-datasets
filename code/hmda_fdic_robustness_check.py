"""
Empirical test of Proposition 3 / Falsifiable Prediction 5, updated to
consume the LOCAL FDIC financials files (fdic_data/fdic_YYYY_q4.csv) instead
of live FDIC API calls, since those files are already on disk.

Everything else in this script still requires normal internet access,
since these pieces have no local equivalent on disk:
  - FDIC `institutions` endpoint   (CERT -> NAME, FED_RSSD)   [small pull]
  - Philly Fed HMDA Lender File    (RSSD -> LEI crosswalk)    [one file]
  - HMDA Data Browser CSV endpoint (LEI -> AUS usage)         [per lender-year]

This script is a robustness/validation check, independent of the main
offline HMDA pipeline (hmda_pipeline.py + lender_year_panel.py). It
recomputes AUS reliance for a small, hand-picked sample of banks directly
from the live HMDA Data Browser, so its results don't share any bug that
might exist in the main pipeline's locally-derived primary_AUS column.

Run this on your own machine with internet access -- it cannot run inside
a network-sandboxed environment.
"""

import glob
import io
import re
import sys
import time

import numpy as np
import pandas as pd
import requests

# ----------------------------------------------------------------------
# 0. CONFIG
# ----------------------------------------------------------------------

FDIC_LOCAL_DIR = "fdic_data"                    # unzipped fdic_data.zip
FDIC_FILE_PATTERN = "fdic_*_q4.csv"

HMDA_LENDER_FILE_URL = (
    "https://www.philadelphiafed.org/-/media/frbp/assets/consumer-finance/"
    "data/hmda/hmda_lender_file.csv"             # VERIFY current path
)
FDIC_INSTITUTIONS_ENDPOINT = "https://banks.data.fdic.gov/api/institutions"
HMDA_CSV_ENDPOINT = "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv"

SLEEP_BETWEEN_CALLS = 0.5

# Sanity-check benchmarks (approximate, real-world, $ actual dollars).
# FDIC ASSET field convention is typically thousands of dollars -- the
# checker tries both interpretations and reports which one (if either)
# looks plausible, rather than assuming.
KNOWN_MIN_BIGBANK_ASSETS_USD = 500e9      # at least a few banks should exceed this
KNOWN_SYSTEM_TOTAL_ASSETS_USD_LOW = 15e12  # loose bounds on US banking system total
KNOWN_SYSTEM_TOTAL_ASSETS_USD_HIGH = 30e12


# ----------------------------------------------------------------------
# 1. Load local FDIC financials files + sanity check
# ----------------------------------------------------------------------

def load_local_fdic_financials(directory: str = FDIC_LOCAL_DIR) -> pd.DataFrame:
    paths = sorted(glob.glob(f"{directory}/{FDIC_FILE_PATTERN}"))
    if not paths:
        raise FileNotFoundError(
            f"No files matching {FDIC_FILE_PATTERN} in {directory}/ "
            "-- unzip fdic_data.zip first, or point FDIC_LOCAL_DIR at your "
            "existing fdic_XXXX_q4.csv folder (e.g. 'I:/HMDA')."
        )
    frames = []
    for p in paths:
        m = re.search(r"(\d{4})_q4", p)
        year = int(m.group(1)) if m else None
        df = pd.read_csv(p)
        df.columns = [c.strip().upper() for c in df.columns]
        df["year"] = year
        frames.append(df)
    panel = pd.concat(frames, ignore_index=True)
    return panel


def sanity_check_assets(df: pd.DataFrame, asset_col: str = "ASSET") -> str:
    """
    Checks whether ASSET looks like real, complete FDIC data under either
    a $-thousands or $-actual convention, and returns which (if any) is
    plausible. Raises if neither is, instead of letting a bad file feed
    a regression silently -- this is the check that caught the problem
    with fdic_data.zip.
    """
    latest_year = df["year"].max()
    latest = df[df["year"] == latest_year]

    max_val = latest[asset_col].max()
    total_val = latest[asset_col].sum()

    interpretations = {
        "actual_dollars": (max_val, total_val),
        "thousands_of_dollars": (max_val * 1_000, total_val * 1_000),
    }

    verdicts = {}
    for label, (max_usd, total_usd) in interpretations.items():
        has_big_bank = max_usd >= KNOWN_MIN_BIGBANK_ASSETS_USD
        total_in_range = (
            KNOWN_SYSTEM_TOTAL_ASSETS_USD_LOW
            <= total_usd
            <= KNOWN_SYSTEM_TOTAL_ASSETS_USD_HIGH
        )
        verdicts[label] = has_big_bank and total_in_range
        print(
            f"[sanity check | {label}] max bank = ${max_usd:,.0f}, "
            f"system total = ${total_usd:,.0f}  "
            f"-> plausible: {verdicts[label]}"
        )

    plausible_labels = [k for k, v in verdicts.items() if v]

    if not plausible_labels:
        raise ValueError(
            "ASSET column is not plausible under either $-thousands or "
            "$-actual-dollars convention for the latest year in this file "
            f"(max={max_val:,.0f}, total={total_val:,.0f} in raw units). "
            "This matches the earlier finding with fdic_data.zip: the file "
            "is likely truncated (missing large banks) or not a genuine "
            "full-population FDIC pull. STOP and re-verify the source "
            "before running any regression on this file. Re-pull from "
            "https://banks.data.fdic.gov/api/financials with no implicit "
            "row/size filter, and confirm total row count against the "
            "known ~4,000-5,500 FDIC-insured institutions."
        )

    if len(plausible_labels) > 1:
        print(
            "[sanity check] both unit conventions look superficially "
            "plausible -- confirm units against the FDIC API docs rather "
            "than trusting this heuristic alone."
        )

    return plausible_labels[0]


# ----------------------------------------------------------------------
# 2. CERT -> RSSD crosswalk (FDIC institutions endpoint; small live pull)
# ----------------------------------------------------------------------

def get_cert_rssd_crosswalk(certs: list) -> pd.DataFrame:
    rows = []
    chunk = 50
    for i in range(0, len(certs), chunk):
        batch = certs[i : i + chunk]
        filt = " OR ".join(f"CERT:{c}" for c in batch)
        params = {
            "filters": filt,
            "fields": "CERT,NAME,FED_RSSD,ACTIVE",
            "limit": chunk,
            "format": "json",
        }
        r = requests.get(FDIC_INSTITUTIONS_ENDPOINT, params=params, timeout=60)
        r.raise_for_status()
        for rec in r.json().get("data", []):
            rows.append(rec["data"])
        time.sleep(SLEEP_BETWEEN_CALLS)
    return pd.DataFrame(rows)


# ----------------------------------------------------------------------
# 3. RSSD -> LEI crosswalk (Philly Fed HMDA Lender File)
# ----------------------------------------------------------------------

def load_rssd_lei_crosswalk() -> pd.DataFrame:
    df = pd.read_csv(HMDA_LENDER_FILE_URL)
    df = df.rename(columns=str.upper)
    keep = [c for c in df.columns if c in ("RSSD9001", "ENTITY", "LEI", "RESPONDENT_LEI")]
    return df[keep].drop_duplicates()


# ----------------------------------------------------------------------
# 4. HMDA AUS pull (live, per lender-year)
# ----------------------------------------------------------------------

def get_lender_aus_records(lei: str, year: int) -> pd.DataFrame:
    params = {
        "leis": lei,
        "years": year,
        "loan_types": "1",
        "loan_purposes": "1",
        "lien_statuses": "1",
        "dwelling_categories": "Single Family (1-4 Units):Site-Built",
        "total_units": "1,2,3,4",
    }
    r = requests.get(HMDA_CSV_ENDPOINT, params=params, timeout=120)
    r.raise_for_status()
    df = pd.read_csv(io.StringIO(r.text), low_memory=False)
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    return df


def summarize_aus_reliance(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"n_apps": 0, "du_share": np.nan, "lpa_share": np.nan, "du_lpa_share": np.nan}
    aus_cols = [c for c in df.columns if c.startswith("aus")]
    if not aus_cols:
        raise KeyError(f"No AUS column found. Columns: {list(df.columns)}")
    vals = df[aus_cols[0]].astype(str).str.strip()
    is_du = vals.eq("1")
    is_lpa = vals.eq("2")
    return {
        "n_apps": len(df),
        "du_share": is_du.mean(),
        "lpa_share": is_lpa.mean(),
        "du_lpa_share": (is_du | is_lpa).mean(),
    }


# ----------------------------------------------------------------------
# 5. Build panel: local FDIC assets + live HMDA AUS, joined via crosswalks
# ----------------------------------------------------------------------

def build_panel(sample_certs: list, years: list) -> pd.DataFrame:
    fdic = load_local_fdic_financials()
    unit = sanity_check_assets(fdic)   # raises if implausible -- do not catch this
    fdic["assets_usd"] = fdic["ASSET"] * (1_000 if unit == "thousands_of_dollars" else 1)

    fdic_sample = fdic[fdic["CERT"].isin(sample_certs)].copy()

    cert_rssd = get_cert_rssd_crosswalk(sample_certs)
    rssd_lei = load_rssd_lei_crosswalk()

    merged_ids = cert_rssd.merge(
        rssd_lei, left_on="FED_RSSD", right_on="RSSD9001", how="left"
    )
    missing = merged_ids["LEI"].isna().sum()
    if missing:
        print(f"[warn] {missing} of {len(merged_ids)} sample banks have no LEI match "
              "in the HMDA lender file -- they will be dropped.", file=sys.stderr)
    merged_ids = merged_ids.dropna(subset=["LEI"])

    fdic_sample = fdic_sample.merge(
        merged_ids[["CERT", "NAME", "FED_RSSD", "LEI"]], on="CERT", how="inner"
    )

    rows = []
    for _, row in fdic_sample.drop_duplicates(["CERT", "LEI"]).iterrows():
        for year in years:
            try:
                aus_df = get_lender_aus_records(row["LEI"], year)
                summary = summarize_aus_reliance(aus_df)
            except Exception as e:
                print(f"[warn] HMDA pull failed for {row['NAME']}/{year}: {e}", file=sys.stderr)
                summary = {"n_apps": np.nan, "du_share": np.nan, "lpa_share": np.nan, "du_lpa_share": np.nan}
            time.sleep(SLEEP_BETWEEN_CALLS)

            assets_row = fdic_sample[
                (fdic_sample["CERT"] == row["CERT"]) & (fdic_sample["year"] == year)
            ]
            assets_usd = assets_row["assets_usd"].iloc[0] if not assets_row.empty else np.nan

            rows.append(
                {
                    "cert": row["CERT"],
                    "rssd": row["FED_RSSD"],
                    "name": row["NAME"],
                    "year": year,
                    "assets_usd": assets_usd,
                    **summary,
                }
            )

    return pd.DataFrame(rows)


# ----------------------------------------------------------------------
# 6. Regression: robustness check on the same relationship
# ----------------------------------------------------------------------

def test_prediction_5(panel: pd.DataFrame):
    import statsmodels.formula.api as smf

    df = panel.dropna(subset=["assets_usd", "du_lpa_share"]).copy()
    df = df[df["n_apps"] >= 30]
    df["log_assets"] = np.log(df["assets_usd"])
    df["year"] = df["year"].astype(str)

    model = smf.ols("du_lpa_share ~ log_assets + C(year)", data=df).fit(
        cov_type="cluster", cov_kwds={"groups": df["cert"]}
    )
    print(model.summary())
    return model


# ----------------------------------------------------------------------
if __name__ == "__main__":
    # Replace with your real preferred-sample CERT numbers.
    SAMPLE_CERTS = [628, 3510, 3511]  # placeholder smoke-test values only

    YEARS = list(range(2018, 2026))

    panel = build_panel(SAMPLE_CERTS, YEARS)
    panel.to_csv("hmda_fdic_panel_v2.csv", index=False)
    print(panel)

    print("\n--- Robustness-check regression ---")
    test_prediction_5(panel)
