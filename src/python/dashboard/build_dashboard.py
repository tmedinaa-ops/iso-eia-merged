"""
Build the interactive county-level dashboard for ISO_EIA_Merged_v7.

Reads the merged CSV, attaches 5-digit county FIPS codes, computes per-county
aggregates, and writes a single self-contained HTML file that ships the
Plotly choropleth, click-to-drill rows, search, and CSV export inline.

Path layout follows the rest of the Python pipeline: a single
PLANNING_QUEUES_DATA env var points at the data folder, defaulting to
<repo>/data when unset. The built HTML goes to <repo>/build/dashboard.html
(or wherever PLANNING_QUEUES_BUILD points).

Usage
-----
    python src/python/dashboard/build_dashboard.py
    python src/python/dashboard/build_dashboard.py --input custom.csv --out out.html

Dependencies: pandas, addfips, plus one-off download of the US counties GeoJSON
from the Plotly datasets repo (cached under <build>/counties.geojson).
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
import urllib.request
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    import addfips
except ImportError:
    sys.stderr.write(
        "error: `addfips` is not installed. Install with: pip install addfips\n"
    )
    sys.exit(1)

# --------------------------------------------------------------------------
# Paths
# --------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_DATA = REPO_ROOT / "data"
DEFAULT_BUILD = REPO_ROOT / "build"
DEFAULT_TEMPLATE = Path(__file__).resolve().parent / "dashboard_template.html"

DATA_DIR = Path(os.environ.get("PLANNING_QUEUES_DATA", str(DEFAULT_DATA)))
BUILD_DIR = Path(os.environ.get("PLANNING_QUEUES_BUILD", str(DEFAULT_BUILD)))

GEOJSON_URL = (
    "https://raw.githubusercontent.com/plotly/datasets/master/"
    "geojson-counties-fips.json"
)
GEOJSON_CACHE = BUILD_DIR / "counties.geojson"

# --------------------------------------------------------------------------
# FIPS matching
# --------------------------------------------------------------------------

# Counties the `addfips` library gets wrong out of the box. Keys are
# (STATE, UPPERCASE_COUNTY) as they appear in the merged CSV.
COUNTY_FIX = {
    ("FL", "MIAMI DADE"): "Miami-Dade",
    ("NY", "NY"): "New York",
    ("AK", "VALDEZ CORDOVA"): "Valdez-Cordova",
    ("AK", "SKAGWAY HOONAH ANGOON"): "Skagway-Hoonah-Angoon",
    ("AK", "YUKON KOYUKUK"): "Yukon-Koyukuk",
    ("AK", "WRANGELL PETERSBURG"): "Wrangell-Petersburg",
    ("AK", "PRINCE OF WALES KETCHIKAN"): "Prince of Wales-Outer Ketchikan",
    ("NC", "NORTHHAMPTON"): "Northampton",
    ("NY", "ONEIDA-DUTCHESS"): "Oneida",
}

NULLISH = {"UNKNOWN", "NOT IN FILE", "TBD", "NA", ""}
STRIP_WORDS = re.compile(
    r"\b(COUNTY|PARISH|BOROUGH|CENSUS AREA|MUNICIPALITY|CITY AND BOROUGH)\b",
    re.I,
)
ST_DOT = re.compile(r"\bSt\b(?!\.)", re.I)
CITY_OF = re.compile(r"^City of\s+(.+)$", re.I)


def _normalize(county: Any, state: Any) -> tuple[str | None, str | None]:
    if pd.isna(county) or pd.isna(state):
        return None, None
    c = str(county).strip()
    s = str(state).strip()
    if not c or not s or c.upper() in NULLISH:
        return None, None
    key = (s.upper(), c.upper())
    if key in COUNTY_FIX:
        c = COUNTY_FIX[key]
    c = ST_DOT.sub("St.", c)
    m = CITY_OF.match(c)
    if m:
        c = f"{m.group(1).strip()} city"
    return c, s


def attach_fips(df: pd.DataFrame) -> pd.DataFrame:
    """Return a copy of df with a 'fips' column (5-digit string or NaN)."""
    af = addfips.AddFIPS()

    def _lookup(row: pd.Series) -> str | None:
        c, s = _normalize(row.get("county"), row.get("state"))
        if c is None:
            return None
        try:
            fips = af.get_county_fips(c, state=s)
            if fips:
                return fips
            c2 = STRIP_WORDS.sub("", c).strip()
            if c2 != c:
                return af.get_county_fips(c2, state=s)
        except Exception:
            return None
        return None

    out = df.copy()
    out["fips"] = out.apply(_lookup, axis=1)
    return out


# --------------------------------------------------------------------------
# Aggregation
# --------------------------------------------------------------------------


def compute_agg(df: pd.DataFrame) -> pd.DataFrame:
    """Per-FIPS aggregate table."""
    d = df[df["fips"].notna()].copy()
    d["time_in_queue"] = pd.to_numeric(d.get("time_in_queue"), errors="coerce")
    d["count_mw"] = pd.to_numeric(d.get("count_mw"), errors="coerce")

    agg = (
        d.groupby("fips")
        .agg(
            avg_time_in_queue=("time_in_queue", "mean"),
            n_rows=("queue_id", "count"),
            total_mw=("count_mw", "sum"),
            county=("county", "first"),
            state=("state", "first"),
            isos=(
                "iso",
                lambda s: ", ".join(sorted({str(x) for x in s.dropna()})),
            ),
        )
        .reset_index()
    )
    agg["avg_time_in_queue"] = agg["avg_time_in_queue"].round(1)
    agg["total_mw"] = agg["total_mw"].round(1)
    return agg


# --------------------------------------------------------------------------
# GeoJSON
# --------------------------------------------------------------------------


def ensure_geojson() -> dict:
    """Load the US counties GeoJSON, downloading + caching if absent."""
    GEOJSON_CACHE.parent.mkdir(parents=True, exist_ok=True)
    if not GEOJSON_CACHE.exists():
        print(f"downloading counties GeoJSON -> {GEOJSON_CACHE}")
        urllib.request.urlretrieve(GEOJSON_URL, GEOJSON_CACHE)
    with open(GEOJSON_CACHE) as f:
        return json.load(f)


# --------------------------------------------------------------------------
# Payload assembly
# --------------------------------------------------------------------------


def _to_cell(v: Any) -> Any:
    if pd.isna(v):
        return None
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.floating):
        if np.isnan(v):
            return None
        return round(float(v), 4)
    return str(v)


def _clean_nan(v: Any) -> Any:
    if isinstance(v, float) and math.isnan(v):
        return None
    if isinstance(v, dict):
        return {k: _clean_nan(vv) for k, vv in v.items()}
    if isinstance(v, list):
        return [_clean_nan(vv) for vv in v]
    return v


def build_payloads(df_matched: pd.DataFrame, agg: pd.DataFrame, geo: dict) -> dict:
    cols = [c for c in df_matched.columns if c != "fips"]
    df_matched = df_matched.copy()
    df_matched["fips"] = df_matched["fips"].astype(str).str.zfill(5)

    rows_by_fips: dict[str, list[list]] = {}
    for fips_val, grp in df_matched.groupby("fips"):
        rows_by_fips[fips_val] = [
            [_to_cell(row[c]) for c in cols] for _, row in grp.iterrows()
        ]

    agg = agg.copy()
    agg["fips"] = agg["fips"].astype(str).str.zfill(5)
    agg_dict = _clean_nan(agg.set_index("fips").to_dict(orient="index"))

    county_labels: dict[str, str] = {}
    for feat in geo.get("features", []):
        fid = feat.get("id")
        props = feat.get("properties", {})
        if fid:
            county_labels[str(fid).zfill(5)] = (
                f"{props.get('NAME', '')}, {props.get('STATE', '')}"
            )

    return {
        "AGG": agg_dict,
        "ROWS": rows_by_fips,
        "COLS": cols,
        "LABELS": county_labels,
        "GEO": geo,
    }


def render_html(template_path: Path, payloads: dict) -> str:
    tmpl = template_path.read_text(encoding="utf-8")
    for key, value in payloads.items():
        tmpl = tmpl.replace(f"__{key}__", json.dumps(value, separators=(",", ":")))
    return tmpl


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_DIR / "ISO_EIA_Merged_v7.csv",
        help="Path to the merged v7 CSV (default: %(default)s)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=BUILD_DIR / "interconnection_county_explorer.html",
        help="Output HTML file (default: %(default)s)",
    )
    parser.add_argument(
        "--template",
        type=Path,
        default=DEFAULT_TEMPLATE,
        help="HTML template (default: %(default)s)",
    )
    parser.add_argument(
        "--agg-csv",
        type=Path,
        default=None,
        help="Optional path to also write the per-county aggregate as CSV.",
    )
    args = parser.parse_args(argv)

    if not args.input.exists():
        sys.stderr.write(
            f"error: input CSV not found at {args.input}\n"
            f"       set PLANNING_QUEUES_DATA or pass --input to point at the v7 CSV.\n"
        )
        return 2
    if not args.template.exists():
        sys.stderr.write(f"error: template not found at {args.template}\n")
        return 2

    print(f"reading {args.input}")
    df = pd.read_csv(args.input, low_memory=False)
    print(f"  {len(df):,} rows, {df.shape[1]} columns")

    print("attaching FIPS codes")
    df = attach_fips(df)
    matched = df["fips"].notna().sum()
    print(f"  matched {matched:,}/{len(df):,} = {matched / len(df) * 100:.1f}%")

    print("computing per-county aggregates")
    agg = compute_agg(df)
    print(f"  {len(agg):,} counties with at least one row")

    if args.agg_csv is not None:
        args.agg_csv.parent.mkdir(parents=True, exist_ok=True)
        agg.to_csv(args.agg_csv, index=False)
        print(f"  wrote agg CSV -> {args.agg_csv}")

    geo = ensure_geojson()
    print(f"  geojson: {len(geo.get('features', []))} county features")

    print("building payloads")
    df_matched = df[df["fips"].notna()].copy()
    payloads = build_payloads(df_matched, agg, geo)

    print("rendering HTML")
    html = render_html(args.template, payloads)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(html, encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
