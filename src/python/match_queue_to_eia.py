"""
Match 03. Complete Queue Data-Table 1 copy.csv to EIA-860 plant/generator data.
Produces Queue_with_EIA_Online_Dates.csv with online date information.

Match priority:
  1. project_name (exact or fuzzy) → EIA Plant Name
  2. poi_name (exact or fuzzy) → EIA Plant Name
  3. Combined project_name + poi_name tokens
State is used as a hard filter when available.
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
from rapidfuzz import fuzz, process
from datetime import datetime, timedelta
import re
import warnings
warnings.filterwarnings("ignore")

# Resolve data directory: honor PLANNING_QUEUES_DATA env var; otherwise default
# to repo/data relative to this file's location (src/python/<file>.py → ../../data).
BASE = os.environ.get(
    "PLANNING_QUEUES_DATA",
    str(Path(__file__).resolve().parents[2] / "data"),
)

# ── Helpers ───────────────────────────────────────────────────────────────────
def excel_serial_to_date(n):
    """Convert Excel serial date number to YYYY-MM-DD string."""
    try:
        n = float(n)
        if pd.isna(n):
            return None
        if n > 60:
            n -= 1  # Excel 1900 leap-year bug
        return (datetime(1899, 12, 31) + timedelta(days=n)).strftime("%Y-%m-%d")
    except Exception:
        return None

STRIP_WORDS = re.compile(
    r"\b(solar|wind|energy|storage|battery|gas|generator|power|plant|"
    r"station|substation|project|farm|llc|inc|lp|corp|co|sub|generating|"
    r"generation|interconnect|kv|mw|transmission|line|system|electric|"
    r"center|utility|services?|holdings?|resources?|renewable|renewables|"
    r"clean|green|hub|complex|facility|facilities|campus)\b",
    re.IGNORECASE,
)

def normalize(s):
    """Normalise a name string for fuzzy comparison."""
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = STRIP_WORDS.sub(" ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

# ── 1. Load queue data ────────────────────────────────────────────────────────
print("Loading queue data...")
queue = pd.read_csv(f"{BASE}/03. Complete Queue Data-Table 1 copy.csv", low_memory=False)
print(f"  Queue rows: {len(queue):,}")

# Convert Excel serial on_date → readable date
queue["on_date_converted"] = queue["on_date"].apply(excel_serial_to_date)
queue["prop_date_converted"] = queue["prop_date"].apply(excel_serial_to_date)

# ── 2. Load EIA generator data (all technology sheets) ───────────────────────
print("\nLoading EIA generator data...")

def load_gen_sheet(path, sheet, label):
    df = pd.read_excel(path, sheet_name=sheet, header=1)
    rename = {}
    for c in df.columns:
        cl = c.strip()
        m = {
            "Plant Name": "Plant Name", "State": "State", "County": "County",
            "Utility Name": "Utility Name", "Technology": "Technology",
            "Nameplate Capacity (MW)": "Nameplate Capacity (MW)",
            "Summer Capacity (MW)": "Summer Capacity (MW)",
            "Operating Year": "Operating Year", "Operating Month": "Operating Month",
            "Effective Year": "Effective Year", "Effective Month": "Effective Month",
        }
        if cl in m:
            rename[c] = m[cl]
    df = df.rename(columns=rename)
    if "Operating Year" not in df.columns and "Effective Year" in df.columns:
        df["Operating Year"]  = df["Effective Year"]
        df["Operating Month"] = df.get("Effective Month", np.nan)
    needed = ["Plant Name","State","County","Utility Name","Technology",
              "Nameplate Capacity (MW)","Operating Year","Operating Month"]
    for col in needed:
        if col not in df.columns:
            df[col] = np.nan
    # Coerce numeric columns
    for col in ["Nameplate Capacity (MW)","Operating Year","Operating Month"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["_source"] = label
    return df[["Plant Name","State","County","Utility Name","Technology",
               "Nameplate Capacity (MW)","Operating Year","Operating Month","_source"]]

sheets = [
    (f"{BASE}/3_1_Generator_Y2024.xlsx", "Operable",            "Gen-Operable"),
    (f"{BASE}/3_1_Generator_Y2024.xlsx", "Proposed",            "Gen-Proposed"),
    (f"{BASE}/3_1_Generator_Y2024.xlsx", "Retired and Canceled","Gen-Retired"),
    (f"{BASE}/3_2_Wind_Y2024.xlsx",      "Operable",            "Wind-Operable"),
    (f"{BASE}/3_3_Solar_Y2024.xlsx",     "Operable",            "Solar-Operable"),
    (f"{BASE}/3_4_Energy_Storage_Y2024.xlsx","Operable",        "Storage-Operable"),
    (f"{BASE}/3_4_Energy_Storage_Y2024.xlsx","Proposed",        "Storage-Proposed"),
    (f"{BASE}/3_5_Multifuel_Y2024.xlsx", "Operable",            "Multifuel-Operable"),
]

frames = []
for path, sheet, label in sheets:
    try:
        df = load_gen_sheet(path, sheet, label)
        print(f"  {label}: {len(df):,} rows")
        frames.append(df)
    except Exception as e:
        print(f"  Warning – could not load {label}: {e}")

all_gen = pd.concat(frames, ignore_index=True)
all_gen = all_gen.dropna(subset=["Plant Name"])
all_gen["Plant Name"] = all_gen["Plant Name"].astype(str).str.strip()
all_gen["State"]      = all_gen["State"].astype(str).str.strip().str.upper()
all_gen["County"]     = all_gen["County"].astype(str).str.strip().str.upper()
print(f"\nTotal generator records loaded: {len(all_gen):,}")

# ── 3. Build per-plant summary ────────────────────────────────────────────────
# Prefer Operable operating years; fall back to other sources
# Sort so Operable rows come first → min Operating Year will be from operable data
source_priority = {"Gen-Operable":0,"Wind-Operable":1,"Solar-Operable":2,
                   "Storage-Operable":3,"Multifuel-Operable":4,
                   "Gen-Proposed":5,"Storage-Proposed":6,"Gen-Retired":7}
all_gen["_priority"] = all_gen["_source"].map(source_priority).fillna(9)
all_gen = all_gen.sort_values(["Plant Name","State","County","_priority","Operating Year"])

plant_summary = (
    all_gen.groupby(["Plant Name","State","County"], as_index=False)
    .agg(
        Utility_Name      = ("Utility Name", "first"),
        Technologies      = ("Technology",   lambda x: "; ".join(sorted(set(x.dropna().astype(str))))),
        Total_Capacity_MW = ("Nameplate Capacity (MW)", "sum"),
        Min_Op_Year       = ("Operating Year",  "min"),
        Min_Op_Month      = ("Operating Month", lambda x: pd.to_numeric(x, errors="coerce").min()),
        Num_Generators    = ("Plant Name",  "count"),
        Sources           = ("_source",     lambda x: "; ".join(sorted(set(x)))),
    )
)
print(f"Unique EIA plant+state+county records: {len(plant_summary):,}")

# ── 4. Build matching lookup ──────────────────────────────────────────────────
eia_names   = plant_summary["Plant Name"].tolist()
eia_norm    = [normalize(n) for n in eia_names]
eia_states  = plant_summary["State"].tolist()
eia_counties= plant_summary["County"].tolist()

# Pre-build state→indices map for fast filtering
from collections import defaultdict
state_idx_map = defaultdict(list)
for i, s in enumerate(eia_states):
    state_idx_map[s].append(i)

# ── 5. Matching function ──────────────────────────────────────────────────────
THRESHOLD_HIGH = 88   # confident match
THRESHOLD_LOW  = 75   # possible match (lower confidence flag)

def match_one(poi, proj, state, county):
    """
    Returns dict with eia_plant_name, match_score, match_confidence, match_method.
    """
    # Build candidate index list (filtered by state)
    state_up = state.strip().upper() if isinstance(state, str) and state.strip() else None
    county_up = county.strip().upper() if isinstance(county, str) and county.strip() else None

    if state_up and state_up in state_idx_map:
        idx_pool = state_idx_map[state_up]
    else:
        idx_pool = list(range(len(eia_names)))

    if not idx_pool:
        idx_pool = list(range(len(eia_names)))

    sub_norms = [eia_norm[i] for i in idx_pool]
    sub_names = [eia_names[i] for i in idx_pool]

    candidates = []

    def run_fuzzy(query_raw, method_tag):
        q_norm = normalize(query_raw)
        if not q_norm:
            return
        # token_sort_ratio on normalised
        results = process.extract(q_norm, sub_norms, scorer=fuzz.token_sort_ratio, limit=5)
        for _, score, rel_idx in results:
            abs_idx = idx_pool[rel_idx]
            # Bonus if county matches
            bonus = 3 if (county_up and eia_counties[abs_idx] == county_up) else 0
            candidates.append((score + bonus, abs_idx, f"norm_token:{method_tag}"))
        # partial_ratio on raw
        q_raw_lower = query_raw.lower()
        results2 = process.extract(q_raw_lower, [n.lower() for n in sub_names],
                                   scorer=fuzz.partial_ratio, limit=3)
        for _, score2, rel_idx in results2:
            abs_idx = idx_pool[rel_idx]
            bonus = 3 if (county_up and eia_counties[abs_idx] == county_up) else 0
            candidates.append((score2 * 0.92 + bonus, abs_idx, f"partial:{method_tag}"))

    # Priority 1: project_name
    if isinstance(proj, str) and proj.strip():
        run_fuzzy(proj.strip(), "project_name")

    # Priority 2: poi_name
    if isinstance(poi, str) and poi.strip():
        run_fuzzy(poi.strip(), "poi_name")

    # Priority 3: combined tokens from both
    combined_tokens = " ".join(filter(None, [
        isinstance(proj, str) and proj.strip() or "",
        isinstance(poi, str)  and poi.strip()  or "",
    ]))
    if combined_tokens.strip():
        run_fuzzy(combined_tokens, "combined")

    if not candidates:
        return None

    candidates.sort(key=lambda x: -x[0])
    best_score, best_idx, best_method = candidates[0]

    return best_score, best_idx, best_method

# ── 6. Run matching across all queue records ──────────────────────────────────
print(f"\nRunning fuzzy matching on {len(queue):,} records...")

matched_rows = []
for i, row in queue.iterrows():
    if i % 5000 == 0:
        print(f"  Processing row {i:,}/{len(queue):,}...")

    result = match_one(
        poi    = row.get("poi_name", ""),
        proj   = row.get("project_name", ""),
        state  = row.get("state", ""),
        county = row.get("county", ""),
    )

    if result is None or result[0] < THRESHOLD_LOW:
        matched_rows.append({
            "eia_matched_plant":    None,
            "eia_matched_state":    None,
            "eia_matched_county":   None,
            "eia_utility":          None,
            "eia_technologies":     None,
            "eia_total_capacity_mw":None,
            "eia_online_year":      None,
            "eia_online_month":     None,
            "eia_num_generators":   None,
            "eia_data_sources":     None,
            "match_score":          round(result[0], 1) if result else 0,
            "match_confidence":     "no_match",
            "match_method":         result[2] if result else "none",
        })
    else:
        score, idx, method = result
        r = plant_summary.iloc[idx]
        confidence = "high" if score >= THRESHOLD_HIGH else "low"
        matched_rows.append({
            "eia_matched_plant":    r["Plant Name"],
            "eia_matched_state":    r["State"],
            "eia_matched_county":   r["County"],
            "eia_utility":          r["Utility_Name"],
            "eia_technologies":     r["Technologies"],
            "eia_total_capacity_mw":r["Total_Capacity_MW"],
            "eia_online_year":      r["Min_Op_Year"],
            "eia_online_month":     r["Min_Op_Month"],
            "eia_num_generators":   r["Num_Generators"],
            "eia_data_sources":     r["Sources"],
            "match_score":          round(score, 1),
            "match_confidence":     confidence,
            "match_method":         method,
        })

match_df = pd.DataFrame(matched_rows)

# ── 7. Combine and save ───────────────────────────────────────────────────────
combined = pd.concat([queue.reset_index(drop=True), match_df], axis=1)

# Determine best available online year: prefer queue on_date year, fall back to EIA
def best_online_year(row):
    if row["on_date_converted"]:
        return int(row["on_date_converted"][:4])
    if pd.notna(row["eia_online_year"]):
        return int(row["eia_online_year"])
    return None

def best_online_date(row):
    if row["on_date_converted"]:
        return row["on_date_converted"]
    if pd.notna(row["eia_online_year"]):
        month = int(row["eia_online_month"]) if pd.notna(row["eia_online_month"]) else 1
        return f"{int(row['eia_online_year'])}-{month:02d}"
    return None

combined["best_online_year"] = combined.apply(best_online_year, axis=1)
combined["best_online_date"] = combined.apply(best_online_date, axis=1)
combined["online_date_source"] = combined.apply(
    lambda r: "queue_on_date" if r["on_date_converted"] else
              ("eia_match"    if pd.notna(r["eia_online_year"]) else "none"),
    axis=1
)

# Reorder: put key columns near the front
key_cols = ["q_id","q_status","state","county","poi_name","project_name",
            "on_date_converted","prop_date_converted","prop_year",
            "best_online_year","best_online_date","online_date_source",
            "eia_matched_plant","eia_matched_state","eia_matched_county",
            "match_score","match_confidence","match_method",
            "eia_online_year","eia_online_month","eia_technologies",
            "eia_total_capacity_mw","eia_num_generators","eia_utility","eia_data_sources"]
other_cols = [c for c in combined.columns if c not in key_cols]
combined = combined[key_cols + other_cols]

out_path = f"{BASE}/Queue_with_EIA_Online_Dates.csv"
combined.to_csv(out_path, index=False)
print(f"\nSaved: {out_path}")

# Also save a version with ONLY operational/matched records
op_matched = combined[
    (combined["q_status"] == "operational") |
    (combined["match_confidence"].isin(["high","low"]))
]
op_path = f"{BASE}/Queue_Operational_with_Online_Dates.csv"
op_matched.to_csv(op_path, index=False)
print(f"Saved operational+matched subset: {op_path}  ({len(op_matched):,} rows)")

# ── 8. Summary ───────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print("MATCHING SUMMARY")
print(f"{'='*65}")
matched    = combined[combined["eia_matched_plant"].notna()]
unmatched  = combined[combined["eia_matched_plant"].isna()]
high_conf  = combined[combined["match_confidence"] == "high"]
low_conf   = combined[combined["match_confidence"] == "low"]
has_yr     = combined[combined["best_online_year"].notna()]

print(f"Total queue records       : {len(combined):>8,}")
print(f"  High-confidence matches : {len(high_conf):>8,}  ({100*len(high_conf)/len(combined):.1f}%)")
print(f"  Low-confidence matches  : {len(low_conf):>8,}  ({100*len(low_conf)/len(combined):.1f}%)")
print(f"  No match                : {len(unmatched):>8,}  ({100*len(unmatched)/len(combined):.1f}%)")
print(f"Records with online year  : {len(has_yr):>8,}  ({100*len(has_yr)/len(combined):.1f}%)")
print(f"  From queue on_date      : {combined['online_date_source'].eq('queue_on_date').sum():>8,}")
print(f"  From EIA match          : {combined['online_date_source'].eq('eia_match').sum():>8,}")

print(f"\nBy q_status (operational records with online year):")
for status in ["operational","active","withdrawn","suspended"]:
    sub = combined[combined["q_status"]==status]
    with_yr = sub[sub["best_online_year"].notna()]
    print(f"  {status:<12} total={len(sub):>6,}  with_online_year={len(with_yr):>6,}  ({100*len(with_yr)/max(len(sub),1):.1f}%)")

print(f"\nSample high-confidence operational matches:")
sample = combined[
    (combined["match_confidence"]=="high") & (combined["q_status"]=="operational")
][["poi_name","project_name","state","eia_matched_plant","match_score","eia_online_year","eia_online_month","best_online_year"]].drop_duplicates().head(40)
print(sample.to_string(index=False))

print(f"\nDone. Output files:")
print(f"  Full:      {out_path}")
print(f"  Op+Match:  {op_path}")
