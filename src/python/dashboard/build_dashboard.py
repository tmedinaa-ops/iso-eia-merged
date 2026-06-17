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


# --------------------------------------------------------------------------
# Reconciled technology label
# --------------------------------------------------------------------------
#
# The raw `type1` column is copied verbatim from each ISO's queue file and is
# not always internally consistent. CAISO in particular sometimes records a
# prime mover (e.g. "Steam Turbine") in the Type field of a project whose
# Fuel field correctly says "Solar" — so a solar PV plant can display as a
# steam turbine. NYISO encodes fuel as single letters (S = Solar, W = Wind).
# Some EIA fuzzy matches are also wrong (a solar project linked to a gas unit).
#
# `reconcile_technology` derives a single clean technology label per row
# WITHOUT mutating the source columns. Priority:
#   * eia_only rows  -> trust the EIA generator record (eia_technologies).
#   * queue rows     -> trust the ISO's own type1/fuel1 consensus; fall back
#                       to fuel1 when type1/fuel1 disagree (the type field is
#                       where the upstream errors live); use EIA only as a
#                       last resort so a bad fuzzy match cannot corrupt a row
#                       whose type1 and fuel1 already agree.
# Raw `type1` is preserved in the data and remains available in the dashboard
# via the "Columns" toggle.

# Exact-match decoder for EIA energy-source codes and ISO short codes
# (keys are lowercased).
TECH_CODE = {
    "sun": "Solar", "s": "Solar", "pv": "Solar", "sol": "Solar",
    "wnd": "Wind", "w": "Wind", "wt": "Wind",
    "osw": "Offshore Wind",
    "wat": "Hydro", "h": "Hydro", "hy": "Hydro",
    "ps": "Pumped Storage",
    "es": "Battery Storage", "bat": "Battery Storage", "mwh": "Battery Storage",
    "ba": "Battery Storage",
    "nuc": "Nuclear", "nu": "Nuclear",
    "geo": "Geothermal",
    "ng": "Natural Gas", "og": "Natural Gas", "pg": "Natural Gas",
    "bfg": "Natural Gas", "sgp": "Natural Gas", "h2": "Natural Gas", "gas": "Natural Gas",
    "cc": "Natural Gas (CC)", "ct": "Natural Gas (CT)", "gt": "Natural Gas (CT)",
    "ctg": "Natural Gas (CT)",
    "bit": "Coal", "sub": "Coal", "lig": "Coal", "ant": "Coal", "rc": "Coal",
    "wc": "Coal", "sgc": "Coal", "pc": "Coal",
    "dfo": "Oil", "rfo": "Oil", "ker": "Oil", "jf": "Oil", "oil": "Oil",
    "lfg": "Landfill Gas",
    "wds": "Biomass", "wdl": "Biomass", "obl": "Biomass", "obg": "Biomass",
    "obs": "Biomass", "ab": "Biomass", "blq": "Biomass", "slw": "Biomass",
    "wo": "Biomass", "msw": "Biomass", "tdf": "Biomass",
    "fc": "Fuel Cell",
}


def _tech_txt(v: Any) -> str:
    return v.strip().lower() if isinstance(v, str) and v.strip() else ""


def tech_label(text: Any) -> str | None:
    """Map one free-text / coded type or fuel string to a clean technology
    label, or None if it can't be classified (e.g. a bare prime mover like
    "Steam Turbine", which is fuel-ambiguous)."""
    t = _tech_txt(text)
    if not t:
        return None
    if t in TECH_CODE:
        return TECH_CODE[t]
    if "solar" in t and ("batt" in t or "storage" in t):
        return "Solar + Storage"
    if "wind" in t and ("batt" in t or "storage" in t):
        return "Wind + Storage"
    if "photovolt" in t:
        return "Solar"
    if "solar" in t and "thermal" in t:
        return "Solar Thermal"
    if "solar" in t:
        return "Solar"
    if "offshore" in t and "wind" in t:
        return "Offshore Wind"
    if "wind" in t:
        return "Wind"
    if "pumped" in t:
        return "Pumped Storage"
    if "batt" in t or "storage" in t:
        return "Battery Storage"
    if "nuclear" in t:
        return "Nuclear"
    if "geothermal" in t:
        return "Geothermal"
    if "landfill" in t:
        return "Landfill Gas"
    if "biomass" in t or "biogas" in t or "wood" in t or "waste" in t:
        return "Biomass"
    if "coal" in t:
        return "Coal"
    if "hydro" in t or "water" in t:
        return "Hydro"
    if "petroleum" in t or "diesel" in t or "oil" in t:
        return "Oil"
    if "combined cycle" in t:
        return "Natural Gas (CC)"
    if "combustion" in t or "gas turbine" in t:
        return "Natural Gas (CT)"
    if "reciprocating" in t or "internal combustion" in t:
        return "Natural Gas (RICE)"
    if "cogen" in t:
        return "Cogeneration"
    if "fuel cell" in t:
        return "Fuel Cell"
    if "steam" in t:
        return None  # bare prime mover: let the fuel field decide
    if "gas" in t:
        return "Natural Gas"
    return None


def tech_family(label: str | None) -> str:
    """Collapse a clean technology label into a coarse family for the
    type1/fuel1 consensus check."""
    l = (label or "").lower()
    if "solar" in l:
        return "solar"
    if "wind" in l:
        return "wind"
    if "pumped" in l:
        return "pumped"
    if "storage" in l or "batt" in l:
        return "storage"
    if "nuclear" in l:
        return "nuclear"
    if "geotherm" in l:
        return "geo"
    if "hydro" in l:
        return "hydro"
    if "landfill" in l or "biomass" in l:
        return "bio"
    if "coal" in l:
        return "coal"
    if "oil" in l:
        return "oil"
    if "gas" in l or "cogen" in l:
        return "gas"
    if "fuel cell" in l:
        return "fuelcell"
    return "other"


def reconcile_technology(row: pd.Series) -> str:
    """Derived, internally-consistent technology label. See module note above.
    Does not mutate type1/fuel1; those stay verbatim in the data."""
    src = row.get("source")
    eia = tech_label(row.get("eia_technologies"))
    fuel = tech_label(row.get("fuel1"))
    typ = tech_label(row.get("type1"))
    lbnl = tech_label(row.get("lbnl_type_clean"))

    # EIA-only rows are EIA generator records, not fuzzy matches -> authoritative.
    if src == "eia_only":
        return eia or fuel or lbnl or "Other"

    # Queue rows: pumped storage = hydro fuel + a storage/pumped type field.
    t1 = _tech_txt(row.get("type1"))
    if fuel == "Hydro" and ("pump" in t1 or "storage" in t1):
        return "Pumped Storage"
    # type1 and fuel1 agree on a family -> use that (prefer the more specific
    # type label); EIA is ignored here so a bad fuzzy match can't override the
    # ISO's own internal consensus.
    if typ and fuel and tech_family(typ) == tech_family(fuel):
        return typ or fuel
    # Otherwise prefer fuel1 (the type field carries the upstream errors),
    # then type1, then the cleaned LBNL label, then EIA as a last resort.
    return fuel or typ or lbnl or eia or "Other"


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

    # Derived technology label, inserted right after the raw type1 column.
    # type1 is left untouched and is hidden by default in the table (see
    # DEFAULT_HIDDEN_COLS) but stays available via the Columns toggle.
    tech = df_matched.apply(reconcile_technology, axis=1)
    if "technology" in df_matched.columns:
        df_matched["technology"] = tech
    elif "type1" in df_matched.columns:
        df_matched.insert(df_matched.columns.get_loc("type1") + 1, "technology", tech)
    else:
        df_matched["technology"] = tech

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

    # Columns hidden by default in the table (still toggleable via Columns).
    # Raw type1 is hidden in favour of the reconciled `technology` column.
    default_hidden = [c for c in ("type1",) if c in cols]

    return {
        "AGG": agg_dict,
        "AGG_BY_STATUS": agg_by_status,
        "CHARTS": charts,
        "STATUS_KEYS": STATUS_KEYS,
        "STATUS_LABELS": STATUS_LABELS,
        "ROWS": rows_by_fips,
        "COLS": cols,
        "HIDDEN": default_hidden,
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
