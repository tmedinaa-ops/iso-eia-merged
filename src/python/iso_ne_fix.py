"""
iso_ne_fix.py — v7 ISO-NE LBNL merge fix
=========================================
Produces ISO_EIA_Merged_v7.csv by appending 514 LBNL ISO-NE rows that had
no representation in v5. All existing v5 rows are preserved unchanged.

Root cause (discovered 2026-04-02):
    When v5 was built, the LBNL "ISO-NE" region was not handled in the merge
    script. Of the 1,281 LBNL ISO-NE rows, 767 were already matched to
    existing NEISO iso_only rows by project name + state (fuzzy score ≥ 85).
    The remaining 514 had no NEISO counterpart and were silently dropped.
    These 514 rows include 18 completed, 377 withdrawn, 119 active projects
    and are predominantly newer Battery and Solar entries.

    NOTE: Existing NEISO rows also carry spurious lbnl_* metadata (e.g.,
    lbnl_region = "CAISO") from cross-ISO queue_id false matches in the
    original v5 build. Correcting that metadata is a separate Fix 2 task
    not included here to limit scope and risk.

What this script does:
    1. Loads v5 (base — original rows untouched).
    2. Loads Queue_Full_Status_Timeline.csv (LBNL pipeline output).
    3. Filters to region == "ISO-NE".
    4. Deduplicates against existing NEISO rows in v5 using fuzzy project
       name + state matching (score threshold = 85). Queue IDs are NOT used
       for dedup because ISO-NE and LBNL use different ID numbering systems.
    5. Maps the 514 unmatched LBNL rows to the v5 column schema.
    6. Appends new rows and saves as ISO_EIA_Merged_v7.csv.

Schema mapping rules (verified against existing lbnl_only rows in v5):
    - source                = "lbnl_only"
    - iso                   = "NEISO"
    - lbnl_region           = "ISO-NE"
    - status_group          = came_online→completed, withdrew→withdrawn, else→active
    - count_for_capacity    = completed→"unverified_completed", else→"do_not_count"
    - count_mw              = 0 for all lbnl_only rows (consistent with v5)
    - match_score           = 0  (no EIA match attempted for lbnl_only)
    - match_confidence      = "no_match"
    - All EIA fields        = None/NaN (no EIA match for lbnl_only rows)

Run:
    python3 iso_ne_fix.py

Output:
    $PLANNING_QUEUES_DATA/ISO_EIA_Merged_v7.csv
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
import re

# Resolve data directory: honor PLANNING_QUEUES_DATA env var; otherwise default
# to repo/data relative to this file's location.
BASE = os.environ.get(
    "PLANNING_QUEUES_DATA",
    str(Path(__file__).resolve().parents[2] / "data"),
)
OUT_DIR   = BASE
V5_PATH   = f"{OUT_DIR}/ISO_EIA_Merged_v5.csv"
QF_PATH   = f"{OUT_DIR}/Queue_Full_Status_Timeline.csv"
V7_PATH   = f"{OUT_DIR}/ISO_EIA_Merged_v7.csv"

# ── Name normalisation (same STRIP_WORDS regex used in match_queue_to_eia.py) ──
STRIP_WORDS = re.compile(
    r"\b(solar|wind|energy|storage|battery|gas|generator|power|plant|"
    r"station|substation|project|farm|llc|inc|lp|corp|co|sub|generating|"
    r"generation|interconnect|kv|mw|transmission|line|system|electric|"
    r"center|utility|services?|holdings?|resources?|renewable|renewables|"
    r"clean|green|hub|complex|facility|facilities|campus)\b",
    re.IGNORECASE,
)

def normalize(s):
    if not isinstance(s, str):
        return ""
    s = s.lower()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    s = STRIP_WORDS.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


# ══════════════════════════════════════════════════════════════════════════════
print("=" * 65)
print("ISO-NE LBNL merge fix — v5 → v7")
print("=" * 65)

# ── 1. Load base files ────────────────────────────────────────────────────────
print("\n[1] Loading v5 ...")
v5 = pd.read_csv(V5_PATH, low_memory=False)
print(f"    v5 rows: {len(v5):,}  |  cols: {v5.shape[1]}")

print("[1] Loading Queue_Full_Status_Timeline ...")
qf = pd.read_csv(QF_PATH, low_memory=False)
print(f"    QF rows: {len(qf):,}")

# Confirm ISO-NE rows exist
isone = qf[qf["region"] == "ISO-NE"].copy()
print(f"\n    LBNL ISO-NE rows found: {len(isone):,}")
print(f"    Outcomes: {isone['outcome'].value_counts().to_dict()}")


# ── 2. Dedup against existing NEISO rows in v5 ───────────────────────────────
# NOTE: queue_id is NOT used for dedup because ISO-NE and LBNL use different
# numbering systems. A raw queue_id match produces false positives (numeric
# IDs collide across ISOs). Instead we use project name + state fuzzy match.
print("\n[2] Deduplicating against existing NEISO rows in v5 (name+state, score ≥ 85) ...")

from rapidfuzz import fuzz

existing_neiso = v5[v5["iso"] == "NEISO"].copy().reset_index(drop=True)
print(f"    Existing NEISO rows in v5: {len(existing_neiso):,}")

isone = isone.reset_index(drop=True)
isone["_name_n"] = isone["project_name"].apply(normalize)
isone["_state"]  = isone["state"].astype(str).str.upper().str.strip()

existing_neiso["_name_n"] = existing_neiso["project_name"].apply(normalize)
existing_neiso["_state"]  = existing_neiso["state"].astype(str).str.upper().str.strip()

v5_tuples = list(zip(existing_neiso["_name_n"].tolist(),
                     existing_neiso["_state"].tolist()))

DEDUP_THRESHOLD = 85
matched_idxs   = []
unmatched_idxs = []

for i, lrow in isone.iterrows():
    l_name  = lrow["_name_n"]
    l_state = lrow["_state"]
    if not l_name or l_name == "nan":
        unmatched_idxs.append(i)
        continue

    best_sc = 0
    for (v5_name, v5_state) in v5_tuples:
        if not v5_name:
            continue
        # Skip if both states are known and different
        if (l_state not in ("NAN", "") and
                v5_state not in ("NAN", "") and
                l_state != v5_state):
            continue
        sc = max(fuzz.token_sort_ratio(l_name, v5_name),
                 fuzz.partial_ratio(l_name, v5_name))
        if sc > best_sc:
            best_sc = sc

    if best_sc >= DEDUP_THRESHOLD:
        matched_idxs.append(i)
    else:
        unmatched_idxs.append(i)

print(f"    Matched to existing NEISO row (≥{DEDUP_THRESHOLD}): {len(matched_idxs)}")
print(f"    No match — net new rows to append:                  {len(unmatched_idxs)}")

new_rows = isone.loc[unmatched_idxs].copy()
print(f"    Outcome distribution of new rows: {new_rows['outcome'].value_counts().to_dict()}")


# ── 3. Map LBNL fields to v5 schema ─────────────────────────────────────────
print("\n[3] Mapping LBNL fields to v5 schema ...")

def map_status_group(outcome):
    if outcome == "came_online":
        return "completed"
    elif outcome == "withdrew":
        return "withdrawn"
    else:
        return "active"

def map_count_for_capacity(outcome):
    if outcome == "came_online":
        return "unverified_completed"
    else:
        return "do_not_count"

def map_resolution_date(row):
    outcome = row.get("outcome", "")
    if outcome == "came_online":
        for col in ["activation_date", "final_date"]:
            val = row.get(col)
            if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                return str(val)[:10]
    elif outcome == "withdrew":
        for col in ["withdrawal_date", "final_date"]:
            val = row.get(col)
            if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                return str(val)[:10]
    return None

def map_resolution_source(row):
    outcome = row.get("outcome", "")
    src = row.get("final_date_source", "none")
    if pd.isna(src) or str(src).strip() in ("", "nan"):
        return "none"
    return str(src)

def map_online_date(row):
    if row.get("outcome") == "came_online":
        for col in ["activation_date", "final_date"]:
            val = row.get(col)
            if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                return str(val)[:10]
    return None

def map_online_year(row):
    od = map_online_date(row)
    if od:
        try:
            return int(od[:4])
        except Exception:
            pass
    return None

def map_withdrawn_date(row):
    if row.get("outcome") == "withdrew":
        for col in ["withdrawal_date", "final_date"]:
            val = row.get(col)
            if pd.notna(val) and str(val).strip() not in ("", "nan", "None"):
                return str(val)[:10]
    return None

# Build new rows conforming exactly to v5 column schema
records = []
for _, row in new_rows.iterrows():
    outcome     = str(row.get("outcome", "")).strip()
    sg          = map_status_group(outcome)
    pname       = str(row.get("project_name", "")) if pd.notna(row.get("project_name")) else ""
    county_val  = str(row.get("county", "")) if pd.notna(row.get("county")) else ""
    type1_val   = str(row.get("type1", "")) if pd.notna(row.get("type1")) else ""
    tiq_days    = row.get("time_in_queue_days")

    rec = {
        # ── Core identifiers ──────────────────────────────────────────────────
        "iso":              "NEISO",
        "queue_id":         row.get("queue_id"),
        "project_name":     pname if pname not in ("", "nan") else None,
        "status":           row.get("ia_status_clean"),

        # ── Technology & capacity ─────────────────────────────────────────────
        "type1":            type1_val if type1_val not in ("", "nan") else None,
        "fuel1":            type1_val if type1_val not in ("", "nan") else None,
        "mw":               row.get("mw1"),

        # ── Location ──────────────────────────────────────────────────────────
        "county":           county_val if county_val not in ("", "nan") else None,
        "state":            row.get("state"),
        "utility":          row.get("utility"),
        "poi":              row.get("poi_name"),

        # ── Dates ─────────────────────────────────────────────────────────────
        "queue_date":       row.get("queue_entry_date"),
        "online_date":      map_online_date(row),
        "online_year":      map_online_year(row),
        "withdrawn_date":   map_withdrawn_date(row),

        # ── Status ────────────────────────────────────────────────────────────
        "status_group":     sg,

        # ── Normalised name/location/fuel (for fuzzy matching reference) ──────
        "name_norm":        normalize(pname),
        "county_norm":      county_val.lower() if county_val not in ("", "nan") else None,
        "fuel_norm":        normalize(type1_val),

        # ── EIA match fields — all null for lbnl_only rows ───────────────────
        "eia_plant_code":       None,
        "eia_plant_name":       None,
        "match_score":          0.0,
        "match_method":         None,
        "eia_nameplate_mw":     None,
        "eia_summer_mw":        None,
        "eia_technologies":     None,
        "eia_fuels":            None,
        "eia_operating_year_min": None,
        "eia_operating_year_max": None,
        "eia_n_generators":     None,
        "match_confidence":     "no_match",

        # ── Source & capacity counting ────────────────────────────────────────
        "source":               "lbnl_only",
        "count_for_capacity":   map_count_for_capacity(outcome),
        "days_to_finish":       tiq_days if pd.notna(tiq_days) else None,

        # ── LBNL-derived fields ───────────────────────────────────────────────
        "lbnl_region":      "ISO-NE",
        "lbnl_developer":   row.get("developer"),
        "lbnl_ia_date":     row.get("ia_signed_date"),
        "lbnl_type_clean":  row.get("technology"),
        "lbnl_service":     row.get("service"),
        "lbnl_cluster":     row.get("cluster"),

        # ── Capacity counting ─────────────────────────────────────────────────
        "count_mw":         0.0,   # consistent with all other lbnl_only rows
        "count_reason":     "not_operational" if sg != "completed" else None,

        # ── Temporal ──────────────────────────────────────────────────────────
        "effective_year":   row.get("queue_entry_year"),
        "resolution_date":  map_resolution_date(row),
        "resolution_source": map_resolution_source(row),
        "time_in_queue":    tiq_days if pd.notna(tiq_days) else None,
    }
    records.append(rec)

new_df = pd.DataFrame(records, columns=v5.columns)
print(f"    Mapped {len(new_df):,} new rows")
print(f"    status_group distribution: {new_df['status_group'].value_counts().to_dict()}")
print(f"    count_for_capacity:        {new_df['count_for_capacity'].value_counts().to_dict()}")


# ── 4. Append and save ───────────────────────────────────────────────────────
print("\n[4] Appending and saving v7 ...")

v7 = pd.concat([v5, new_df], ignore_index=True)
print(f"    v7 rows: {len(v7):,}  (was {len(v5):,}, added {len(new_df):,})")

# Verify all original rows intact
assert len(v7) == len(v5) + len(new_df), "Row count mismatch!"
assert v7.shape[1] == v5.shape[1], f"Column count mismatch: v7={v7.shape[1]} vs v5={v5.shape[1]}"

v7.to_csv(V7_PATH, index=False)
print(f"    Saved: {V7_PATH}")


# ── 5. Validation report ─────────────────────────────────────────────────────
print("\n" + "=" * 65)
print("VALIDATION REPORT")
print("=" * 65)

print(f"\n  Total rows  : v5={len(v5):,}  →  v7={len(v7):,}  (+{len(new_df):,})")
print(f"  Total cols  : v5={v5.shape[1]}  →  v7={v7.shape[1]}  (must be equal)")
print(f"  Columns match: {list(v5.columns) == list(v7.columns)}")

print(f"\n  NEISO rows  : v5={len(v5[v5['iso']=='NEISO']):,}  →  v7={len(v7[v7['iso']=='NEISO']):,}")

v7_neiso = v7[v7["iso"] == "NEISO"]
print(f"\n  NEISO source breakdown (v7):")
print(v7_neiso["source"].value_counts().to_string())

print(f"\n  NEISO status_group breakdown (v7):")
print(v7_neiso["status_group"].value_counts().to_string())

print(f"\n  NEISO lbnl_only rows — key field fill rates:")
neiso_lbnl = v7_neiso[v7_neiso["source"] == "lbnl_only"]
for col in ["queue_date", "mw", "type1", "lbnl_type_clean", "lbnl_service",
            "resolution_date", "time_in_queue"]:
    fill = neiso_lbnl[col].notna().mean()
    print(f"    {col:30s}: {fill:.0%}")

print(f"\n  Source distribution v7:")
print(v7["source"].value_counts().to_string())

print(f"\n  ISO distribution v7 (top 12):")
print(v7["iso"].value_counts(dropna=False).head(12).to_string())

# Confirm original v5 rows are untouched
orig_rows = v7.iloc[:len(v5)]
changed = (orig_rows.fillna("__NA__") != v5.fillna("__NA__")).any(axis=1).sum()
print(f"\n  Original v5 rows changed: {changed}  (must be 0)")

print(f"\n{'=' * 65}")
print("Done.")
