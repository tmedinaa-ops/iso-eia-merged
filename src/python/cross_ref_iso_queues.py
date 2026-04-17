"""
Cross-reference master queue with ISO/RTO queue files to fill missing dates.
ISOs handled: BPA (West), NYISO, SPP (partial - active only)
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import warnings
warnings.filterwarnings("ignore")

# Resolve data directory: honor PLANNING_QUEUES_DATA env var; otherwise default
# to repo/data relative to this file's location.
BASE = os.environ.get(
    "PLANNING_QUEUES_DATA",
    str(Path(__file__).resolve().parents[2] / "data"),
)
THRESH  = 80   # fuzzy match threshold
FUZZY_W = 75   # lower threshold for when state also matches

# ── Load master queue ──────────────────────────────────────────────────────────
print("Loading master queue...")
df = pd.read_csv(f"{BASE}/Queue_Full_Status_Timeline.csv", low_memory=False)
print(f"  Total rows: {len(df):,}")

# Rows missing final_date
missing_mask = df["final_date"].isna()
print(f"  Missing final_date: {missing_mask.sum():,}")

# ── Fuzzy match helper ─────────────────────────────────────────────────────────
def best_score(name_a, name_b):
    """Return the best fuzzy score between two names."""
    a = str(name_a).strip().lower()
    b = str(name_b).strip().lower()
    return max(
        fuzz.token_sort_ratio(a, b),
        fuzz.partial_ratio(a, b),
    )

def find_match(master_name, master_poi, master_state, iso_df, iso_name_col, iso_poi_col=None, iso_state_col=None):
    """
    Find the best matching row in iso_df for a master queue entry.
    Returns (best_idx, best_score, match_type) or (None, 0, None).
    """
    best_idx   = None
    best_sc    = 0
    match_type = None

    for idx, row in iso_df.iterrows():
        iso_name  = str(row.get(iso_name_col, "")).strip()
        iso_state = str(row.get(iso_state_col, "")).strip() if iso_state_col else ""
        iso_poi   = str(row.get(iso_poi_col, "")).strip() if iso_poi_col else ""

        # State filter (soft – don't exclude if state unknown)
        state_ok = (not iso_state or not master_state or
                    iso_state.upper() == str(master_state).strip().upper())

        # Name score
        name_sc = max(
            best_score(master_name, iso_name),
            best_score(master_poi,  iso_name) if master_poi else 0,
            best_score(master_name, iso_poi)  if iso_poi else 0,
        )

        threshold = FUZZY_W if state_ok else THRESH + 5
        if name_sc >= threshold and name_sc > best_sc:
            best_sc    = name_sc
            best_idx   = idx
            match_type = "name"

    return best_idx, best_sc, match_type


# ═══════════════════════════════════════════════════════════════════════════════
# 1. BPA – West region
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("BPA (West region) matching")
print("="*65)

xl_bpa = pd.ExcelFile(f"{BASE}/BPA_Queue_Real.xlsx")
bpa = pd.read_excel(xl_bpa, sheet_name="Sheet1", header=4)
print(f"  BPA rows loaded: {len(bpa):,}")

# Split into energized and withdrawn
bpa_en = bpa[bpa["Status"] == "ENERGIZED"].copy()
bpa_wd = bpa[bpa["Status"] == "WITHDRAWN"].copy()
print(f"  ENERGIZED: {len(bpa_en)}, WITHDRAWN: {len(bpa_wd)}")

# Master queue – West region, missing final_date
west_missing = df[(df["region"] == "West") & missing_mask].copy()
print(f"  West rows missing final_date: {len(west_missing):,}")

bpa_matches = []

for master_idx, mrow in west_missing.iterrows():
    m_name  = str(mrow.get("project_name", "")).strip()
    m_poi   = str(mrow.get("poi_name", "")).strip()
    m_state = str(mrow.get("state", "")).strip()
    outcome = str(mrow.get("outcome", "")).strip()

    # Pick the right BPA pool
    pool    = bpa_en if outcome == "came_online" else (
              bpa_wd if outcome == "withdrew"    else bpa)  # still_in_queue: try all

    idx, sc, mtype = find_match(m_name, m_poi, m_state,
                                pool,
                                iso_name_col="Project Name",
                                iso_poi_col="Point Of Interconnection",
                                iso_state_col="State")
    if idx is not None:
        iso_row    = pool.loc[idx]
        iso_status = str(iso_row["Status"])
        agreed     = iso_row.get("Agreed To: (Blank=TBD)")
        req_is     = iso_row.get("Requested In-Service Date")

        # Pick best date
        date_val = None
        if pd.notna(agreed) and str(agreed) not in ("NaT", "None", "nan"):
            date_val = str(agreed)[:10]
        elif pd.notna(req_is) and str(req_is) not in ("NaT", "None", "nan"):
            date_val = str(req_is)[:10]

        bpa_matches.append({
            "master_idx":    master_idx,
            "bpa_request":   iso_row.get("Request Number"),
            "bpa_name":      iso_row.get("Project Name"),
            "bpa_status":    iso_status,
            "iso_date":      date_val,
            "fuzzy_score":   sc,
            "master_name":   m_name,
            "master_outcome": outcome,
        })

print(f"\n  Matched {len(bpa_matches)} West rows via BPA")

# Apply BPA matches
bpa_applied = 0
for m in bpa_matches:
    idx = m["master_idx"]
    if m["iso_date"]:
        df.loc[idx, "web_found_date"]   = m["iso_date"]
        df.loc[idx, "web_found_source"] = "BPA Interconnection Queue"
        df.loc[idx, "web_notes"]        = f"BPA#{m['bpa_request']} status={m['bpa_status']} score={m['fuzzy_score']:.0f} match={m['bpa_name']}"
        bpa_applied += 1

print(f"  Applied dates to {bpa_applied} rows")
if bpa_matches:
    avg_sc = np.mean([m["fuzzy_score"] for m in bpa_matches])
    pct    = 100 * bpa_applied / len(west_missing) if len(west_missing) > 0 else 0
    print(f"  Avg fuzzy score: {avg_sc:.1f} | Recovery rate: {pct:.1f}%")
    print(f"\n  Sample matches:")
    for m in bpa_matches[:10]:
        print(f"    [{m['fuzzy_score']:3.0f}] {str(m['master_name']):<40} → {str(m['bpa_name']):<40} ({m['bpa_status']}) {m['iso_date']}")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. NYISO – NYISO region
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("NYISO (NYISO region) matching")
print("="*65)

xl_ny = pd.ExcelFile(f"{BASE}/ISO_Queue_NYISO_hist.xlsx")

# In Service sheet (two-row header)
df_raw_is = pd.read_excel(xl_ny, sheet_name="In Service", header=None)
h0 = df_raw_is.iloc[0].fillna("")
h1 = df_raw_is.iloc[1].fillna("")
is_cols = [f"{a} {b}".strip() for a, b in zip(h0, h1)]
ny_is = df_raw_is.iloc[2:].copy()
ny_is.columns = is_cols
ny_is = ny_is.reset_index(drop=True)
print(f"  NYISO In Service rows: {len(ny_is)}")

# Withdrawn sheet (single header)
ny_wd = pd.read_excel(xl_ny, sheet_name="Withdrawn", header=0)
print(f"  NYISO Withdrawn rows: {len(ny_wd)}")

# Cluster Withdrawn sheet
ny_wd_cl = pd.read_excel(xl_ny, sheet_name="Cluster Projects-Withdrawn", header=0)
print(f"  NYISO Cluster-Withdrawn rows: {len(ny_wd_cl)}")

# Combine all withdrawn
ny_wd_all = pd.concat([ny_wd, ny_wd_cl], ignore_index=True)

# Master – NYISO region
nyiso_missing = df[(df["region"] == "NYISO") & missing_mask].copy()
print(f"  NYISO rows missing final_date: {len(nyiso_missing):,}")

nyiso_matches = []

def match_nyiso(master_subset, iso_df, iso_name_col, iso_date_col, source_tag):
    results = []
    for master_idx, mrow in master_subset.iterrows():
        m_name  = str(mrow.get("project_name", "")).strip()
        m_poi   = str(mrow.get("poi_name", "")).strip()
        m_state = str(mrow.get("state", "")).strip()

        idx, sc, mtype = find_match(m_name, m_poi, m_state,
                                    iso_df,
                                    iso_name_col=iso_name_col,
                                    iso_state_col="State")
        if idx is not None:
            iso_row  = iso_df.loc[idx]
            date_raw = iso_row.get(iso_date_col)
            date_val = None
            if pd.notna(date_raw) and str(date_raw) not in ("NaT", "None", "nan", ""):
                try:
                    date_val = pd.to_datetime(date_raw).strftime("%Y-%m-%d")
                except Exception:
                    date_val = str(date_raw)[:10]
            results.append({
                "master_idx":    master_idx,
                "iso_name":      iso_row.get(iso_name_col),
                "iso_date":      date_val,
                "fuzzy_score":   sc,
                "source":        source_tag,
                "master_name":   m_name,
            })
    return results

# In Service → came_online rows
nyiso_online_missing = nyiso_missing[nyiso_missing["outcome"] == "came_online"]
res_is = match_nyiso(nyiso_online_missing, ny_is, "Project Name", "Last Update", "NYISO In Service")
print(f"\n  In Service matches: {len(res_is)}")

# Withdrawn → withdrew rows
nyiso_wd_missing = nyiso_missing[nyiso_missing["outcome"] == "withdrew"]
res_wd = match_nyiso(nyiso_wd_missing, ny_wd_all, "Project Name", "Last Update", "NYISO Withdrawn")
print(f"  Withdrawn matches: {len(res_wd)}")

nyiso_matches = res_is + res_wd

# Apply NYISO matches
nyiso_applied = 0
for m in nyiso_matches:
    idx = m["master_idx"]
    if m["iso_date"]:
        df.loc[idx, "web_found_date"]   = m["iso_date"]
        df.loc[idx, "web_found_source"] = m["source"]
        df.loc[idx, "web_notes"]        = f"score={m['fuzzy_score']:.0f} match={m['iso_name']}"
        nyiso_applied += 1

print(f"  Applied dates to {nyiso_applied} rows")
if nyiso_matches:
    avg_sc = np.mean([m["fuzzy_score"] for m in nyiso_matches])
    pct    = 100 * nyiso_applied / len(nyiso_missing) if len(nyiso_missing) > 0 else 0
    print(f"  Avg fuzzy score: {avg_sc:.1f} | Recovery rate: {pct:.1f}%")
    print(f"\n  Sample matches:")
    for m in (res_is + res_wd)[:15]:
        print(f"    [{m['fuzzy_score']:3.0f}] {str(m['master_name']):<45} → {str(m['iso_name']):<45} {m['iso_date']} ({m['source']})")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. SPP – SPP region (active queue only – limited historical data)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("SPP (SPP region) matching – active queue")
print("="*65)

spp = pd.read_csv(f"{BASE}/SPP_Queue_Active.csv", skiprows=1, low_memory=False)
spp.columns = spp.columns.str.strip()
print(f"  SPP rows loaded: {len(spp):,}")

# Master – SPP region
spp_missing = df[(df["region"] == "SPP") & missing_mask].copy()
print(f"  SPP rows missing final_date: {len(spp_missing):,}")

spp_matches = []
for master_idx, mrow in spp_missing.iterrows():
    m_name  = str(mrow.get("project_name", "")).strip()
    m_poi   = str(mrow.get("poi_name", "")).strip()
    m_state = str(mrow.get("state", "")).strip()

    idx, sc, mtype = find_match(m_name, m_poi, m_state,
                                spp,
                                iso_name_col="Substation or Line",
                                iso_state_col="State")
    if idx is not None:
        iso_row = spp.loc[idx]
        # For SPP, try Commercial Operation Date first, then In-Service Date
        cod_raw = iso_row.get("Commercial Operation Date")
        isd_raw = iso_row.get("In-Service Date")
        date_val = None
        for raw in [cod_raw, isd_raw]:
            if pd.notna(raw) and str(raw) not in ("NaT", "None", "nan", ""):
                try:
                    date_val = pd.to_datetime(raw).strftime("%Y-%m-%d")
                    break
                except Exception:
                    date_val = str(raw)[:10]
                    break

        spp_matches.append({
            "master_idx":  master_idx,
            "spp_id":      iso_row.get("Generation Interconnection Number"),
            "spp_name":    iso_row.get("Substation or Line"),
            "iso_date":    date_val,
            "fuzzy_score": sc,
            "master_name": m_name,
        })

print(f"\n  Matched {len(spp_matches)} SPP rows")
spp_applied = 0
for m in spp_matches:
    idx = m["master_idx"]
    if m["iso_date"]:
        df.loc[idx, "web_found_date"]   = m["iso_date"]
        df.loc[idx, "web_found_source"] = "SPP Generator Interconnection Queue"
        df.loc[idx, "web_notes"]        = f"SPP#{m['spp_id']} score={m['fuzzy_score']:.0f} match={m['spp_name']}"
        spp_applied += 1

print(f"  Applied dates to {spp_applied} rows")
if spp_matches:
    avg_sc = np.mean([m["fuzzy_score"] for m in spp_matches])
    print(f"  Avg fuzzy score: {avg_sc:.1f}")
    for m in spp_matches[:10]:
        print(f"    [{m['fuzzy_score']:3.0f}] {str(m['master_name']):<45} → {str(m['spp_name']):<45} {m['iso_date']}")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Rebuild final_date column
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*65)
print("Rebuilding final_date column")
print("="*65)

def final_date(row):
    if row["outcome"] == "came_online":
        if pd.notna(row.get("activation_date")) and str(row["activation_date"]) not in ("nan", "None", ""):
            return str(row["activation_date"]), "queue_on_date"
        if row.get("web_found_date"):
            return str(row["web_found_date"]), "web_search"
        if pd.notna(row.get("eia_online_year")):
            mo = int(row["eia_online_month"]) if pd.notna(row.get("eia_online_month")) else 1
            return f"{int(row['eia_online_year'])}-{mo:02d}", "eia_match"
    elif row["outcome"] == "withdrew":
        if pd.notna(row.get("withdrawal_date")) and str(row["withdrawal_date"]) not in ("nan", "None", ""):
            return str(row["withdrawal_date"]), "queue_wd_date"
        if row.get("web_found_date"):
            return str(row["web_found_date"]), "web_search"
    elif row["outcome"] == "still_in_queue":
        if row.get("web_found_date"):
            return str(row["web_found_date"]), "web_search"
    return None, "none"

df[["final_date", "final_date_source"]] = df.apply(
    lambda r: pd.Series(final_date(r)), axis=1
)

# ── Save ───────────────────────────────────────────────────────────────────────
out = f"{BASE}/Queue_Full_Status_Timeline.csv"
df.to_csv(out, index=False)
print(f"\nSaved: {out}")
print(f"Total rows: {len(df):,}")

# ── Summary ────────────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print("FILL RATE BY REGION")
print(f"{'='*65}")
for region in df["region"].unique():
    sub = df[df["region"] == region]
    filled = sub["final_date"].notna().sum()
    total  = len(sub)
    pct    = 100 * filled / total
    missing = total - filled
    print(f"  {region:<12} {filled:>5,}/{total:>5,} = {pct:5.1f}%  (missing: {missing:,})")

print(f"\n  TOTAL: {df['final_date'].notna().sum():,}/{len(df):,} = "
      f"{100*df['final_date'].notna().sum()/len(df):.1f}% filled")

print(f"\n{'='*65}")
print("MATCHES BY ISO")
print(f"{'='*65}")
print(f"  BPA (West)  : {len(bpa_matches):>4} matched, {bpa_applied:>4} dates applied")
print(f"  NYISO       : {len(nyiso_matches):>4} matched, {nyiso_applied:>4} dates applied")
print(f"  SPP         : {len(spp_matches):>4} matched, {spp_applied:>4} dates applied")
total_new = bpa_applied + nyiso_applied + spp_applied
print(f"  TOTAL NEW   : {total_new:>4} dates added this run")
