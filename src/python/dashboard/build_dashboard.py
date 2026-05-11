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

# --------------------------------------------------------------------------
# Status filter + fuel normalization
# --------------------------------------------------------------------------

# The four real values of the `status_group` column in v7. Order here
# controls the order of radio buttons in the UI.
STATUS_KEYS = ["completed", "active", "withdrawn", "eia_only"]
STATUS_LABELS = {
    "all": "All projects",
    "completed": "Completed",
    "active": "Active",
    "withdrawn": "Withdrawn",
    "eia_only": "Operating (not in queue)",
}


def fuel_bucket(row: pd.Series) -> str:
    """Collapse the merged dataset's many fuel/type labels into a small chart-
    friendly bucket. Prefers `lbnl_type_clean` (already normalized) and falls
    back to `type1`/`fuel1` for rows that came from EIA only."""
    text = None
    for col in ("lbnl_type_clean", "type1", "fuel1"):
        v = row.get(col)
        if isinstance(v, str) and v.strip():
            text = v.strip().lower()
            break
    if not text:
        return "Unknown"
    if "solar" in text and ("battery" in text or "storage" in text):
        return "Solar+Battery"
    if "solar" in text or text in {"sun", "photovoltaic"}:
        return "Solar"
    if "wind" in text or text == "wnd":
        return "Wind"
    if "battery" in text or text in {"storage", "es", "mwh"}:
        return "Battery"
    if "nuclear" in text:
        return "Nuclear"
    if "landfill" in text or text == "lfg":
        return "Landfill Gas"
    if "biomass" in text or "biogas" in text or text in {"wds", "ob", "msw"}:
        return "Biomass"
    if "geothermal" in text or text == "geo":
        return "Geothermal"
    if "coal" in text or text in {"bit", "sub", "lig", "ant"}:
        return "Coal"
    if "hydro" in text or text == "wat":
        return "Hydro"
    if (
        "oil" in text
        or "petroleum" in text
        or "diesel" in text
        or text in {"dfo", "rfo", "jf", "ker"}
    ):
        return "Oil"
    if "gas" in text or text == "ng":
        return "Gas"
    return "Other"


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


def _agg_to_dict(agg_df: pd.DataFrame) -> dict:
    a = agg_df.copy()
    a["fips"] = a["fips"].astype(str).str.zfill(5)
    return _clean_nan(a.set_index("fips").to_dict(orient="index"))


def build_payloads(df_matched: pd.DataFrame, agg: pd.DataFrame, geo: dict) -> dict:
    df_matched = df_matched.copy()
    df_matched["fips"] = df_matched["fips"].astype(str).str.zfill(5)

    cols = [c for c in df_matched.columns if c != "fips"]

    rows_by_fips: dict[str, list[list]] = {}
    for fips_val, grp in df_matched.groupby("fips"):
        rows_by_fips[fips_val] = [
            [_to_cell(row[c]) for c in cols] for _, row in grp.iterrows()
        ]

    agg_dict = _agg_to_dict(agg)

    # Per-status aggregates so the map can recolor without re-iterating rows.
    agg_by_status: dict[str, dict] = {"all": agg_dict}
    for sv in STATUS_KEYS:
        sub = df_matched[df_matched["status_group"] == sv]
        if len(sub) == 0:
            agg_by_status[sv] = {}
            continue
        agg_by_status[sv] = _agg_to_dict(compute_agg(sub))

    # Chart payloads. Pre-aggregated in Python so the JS just renders bars.
    charts = compute_chart_data(df_matched)

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
        "AGG_BY_STATUS": agg_by_status,
        "CHARTS": charts,
        "STATUS_KEYS": STATUS_KEYS,
        "STATUS_LABELS": STATUS_LABELS,
        "ROWS": rows_by_fips,
        "COLS": cols,
        "LABELS": county_labels,
        "GEO": geo,
    }


def compute_chart_data(df: pd.DataFrame) -> dict:
    """Return chart-ready dicts keyed by status filter.

    - `status_counts` is a fixed bar chart of the four status_group buckets
      (it doesn't change with the filter, since the filter IS status).
    - `fuel_by_status[s]` and `year_by_status[s]` give the per-status fuel
      and per-year counts. `s` is one of "all", "completed", "active",
      "withdrawn", "eia_only".
    """
    d = df.copy()
    d["_fuel"] = d.apply(fuel_bucket, axis=1)
    d["_year"] = pd.to_numeric(d.get("effective_year"), errors="coerce")

    sc = d["status_group"].value_counts(dropna=False)
    status_counts = {
        STATUS_LABELS.get(k if isinstance(k, str) else "unknown", str(k)): int(v)
        for k, v in sc.items()
    }

    fuel_by_status: dict[str, dict] = {}
    year_by_status: dict[str, dict] = {}
    for s in ["all"] + STATUS_KEYS:
        sub = d if s == "all" else d[d["status_group"] == s]
        fc = sub["_fuel"].value_counts(dropna=False)
        fuel_by_status[s] = {str(k): int(v) for k, v in fc.items() if pd.notna(k)}
        yc = sub.dropna(subset=["_year"]).groupby("_year").size()
        year_by_status[s] = {str(int(k)): int(v) for k, v in yc.items()}

    return {
        "status_counts": status_counts,
        "fuel_by_status": fuel_by_status,
        "year_by_status": year_by_status,
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
