"""
Master script: apply all ISO/RTO cross-reference matches to Queue_Full_Status_Timeline.csv
Run order:
  1. Load base file (from enrich_queue_status.py output)
  2. Apply manual web patches (patch_web_dates.py patches)
  3. Apply MISO API matches
  4. Apply CAISO / ERCOT / ISO-NE matches (gridstatus)
  5. Apply BPA matches (direct queue_id + fuzzy)
  6. Apply NYISO matches (direct queue position)
  7. Rebuild final_date
  8. Save

Priority: manual patches > ISO queue > EIA match
NOTE: ISO matches do NOT overwrite existing web_found_date from prior runs.
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
TODAY   = "2026-03-08"

# ── Load ─────────────────────────────────────────────────────────────────────
print("Loading master queue...")
df = pd.read_csv(f"{BASE}/Queue_Full_Status_Timeline.csv", low_memory=False)
print(f"  Rows: {len(df):,}")

# Ensure web columns exist (don't clear existing)
for col in ["web_found_date","web_found_source","web_notes"]:
    if col not in df.columns:
        df[col] = None

# ── Helpers ───────────────────────────────────────────────────────────────────
def safe_date(d):
    """Parse a date value to YYYY-MM-DD string, or None."""
    if d is None or (isinstance(d, float) and np.isnan(d)):
        return None
    s = str(d).strip()
    if s in ("","nan","NaT","None","NaN"):
        return None
    try:
        return pd.to_datetime(s).strftime("%Y-%m-%d")
    except:
        return s[:10] if len(s) >= 10 else None

def set_web(df, idx, date_val, source, notes, overwrite_existing=False):
    """Set web_found_date for a row if not already set (or if overwrite_existing=True)."""
    existing = df.at[idx, "web_found_date"]
    if (not overwrite_existing and
        existing is not None and
        str(existing) not in ("","nan","None","NaT")):
        return False
    df.at[idx, "web_found_date"]   = date_val
    df.at[idx, "web_found_source"] = source
    df.at[idx, "web_notes"]        = notes
    return True

def best_name_score(a, b):
    a, b = str(a).strip().lower(), str(b).strip().lower()
    if not a or not b or a=="nan" or b=="nan":
        return 0
    return max(fuzz.token_sort_ratio(a,b), fuzz.partial_ratio(a,b))


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Manual web patches (from patch_web_dates.py)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("1. Manual web patches")
print("="*60)

patches = [
    # ── Came Online ────────────────────────────────────────────────
    {"match":{"project_name":"Plant McDonough, Units 4B, 4STG, 5B, 5STG, & 6CC"},
     "date":"2011-12",
     "source":"https://www.prnewswire.com/news-releases/georgia-power-brings-second-plant-mcdonough-atkinson-natural-gas-unit-into-service-149488845.html | https://www.gem.wiki/McDonough_Steam_Generating_Plant",
     "notes":"First CC block Dec 2011, second Apr 2012, third Nov 2012."},
    {"match":{"project_name":"Athens Gen","state":"NY"},
     "date":"2004-05",
     "source":"https://www.gridinfo.com/plant/athens-generating-plant/55405 | https://www.gem.wiki/Athens_generating_plant",
     "notes":"Athens Generating Plant, Greene County NY. 1,080 MW CCGT. COD May 2004."},
    {"match":{"project_name":"Astoria Energy - Phase 1"},
     "date":"2006-05-21",
     "source":"https://www.power-eng.com/gas/nyc-astoria-500-mw-plant-opens/ | https://www.power-technology.com/projects/500mw/",
     "notes":"Astoria Energy I, Queens NY. 500 MW CCGT. COD May 21, 2006."},
    {"match":{"project_name":"Astoria Energy II","state":"NY"},
     "date":"2011-07-01",
     "source":"https://www.powermag.com/top-plantastoria-ii-combined-cycle-plant-queens-new-york/",
     "notes":"Astoria Energy II, Queens NY. 617 MW CCGT. COD July 1, 2011."},
    {"match":{"project_name":"Kleen Energy Project"},
     "date":"2011-07",
     "source":"https://www.kyuden-intl.co.jp/en/business/kleen.html | https://www.gem.wiki/Kleen_Energy_Systems_Project",
     "notes":"Kleen Energy, Middletown CT. 620 MW CCGT. COD July 2011."},
    {"match":{"project_name":"CPV Towantic Energy Center"},
     "date":"2018-05-21",
     "source":"https://www.globenewswire.com/news-release/2018/06/14/1524636/0/en/Competitive-Power-Ventures-and-GE-Achieve-Commercial-Operation-at-805-MW-CPV-Towantic-Energy-Center-Connecticut.html",
     "notes":"CPV Towantic, Oxford CT. 805 MW CCGT. COD May 21, 2018."},
    {"match":{"project_name":"CPV Valley Energy Center"},
     "date":"2018-09",
     "source":"https://www.cpv.com/2018/09/30/competitive-power-ventures-achieves-commercial-operation-at-680-mw-cpv-valley-energy-center-new-york/",
     "notes":"CPV Valley, Wawayanda NY. 680 MW CCGT. COD Sep 2018."},
    {"match":{"project_name":"Shepherds Flat"},
     "date":"2012-09",
     "source":"https://en.wikipedia.org/wiki/Shepherds_Flat_Wind_Farm | https://www.energy.gov/lpo/shepherds-flat",
     "notes":"Shepherds Flat Wind Farm, Gilliam/Morrow OR. 845 MW. Sep 22, 2012."},
    {"match":{"project_name":"Biglow Canyon Wind"},
     "date":"2007-12",
     "source":"https://en.wikipedia.org/wiki/Biglow_Canyon_Wind_Farm | https://investors.portlandgeneral.com/news-releases/news-release-details/final-phase-pges-biglow-canyon-wind-farm-begins-spin-power",
     "notes":"Biglow Canyon Wind Farm, Sherman OR. Phase 1: Dec 2007."},
    {"match":{"project_name":"Carty CCCT"},
     "date":"2016-07-29",
     "source":"https://www.power-eng.com/renewables/portland-general-s-carty-plant-goes-into-service-renewable-rfp-shelved/ | https://www.oregon.gov/energy/facilities-safety/facilities/pages/cgs.aspx",
     "notes":"Carty Generating Station, Boardman OR. ~440 MW NGCC. COD Jul 29, 2016."},
    {"match":{"project_name":"J K Spruce 2"},
     "date":"2010-05-28",
     "source":"https://www.powermag.com/top-plantj-k-spruce-2-calaveras-power-station-san-antonio-texas/ | https://en.wikipedia.org/wiki/Calaveras_Power_Station",
     "notes":"J.K. Spruce Unit 2, Calaveras Power Station, Bexar TX. 750 MW coal. COD May 28, 2010."},
    {"match":{"project_name":"Bayonne Energy Center"},
     "date":"2012-06",
     "source":"https://en.wikipedia.org/wiki/Bayonne_Energy_Center | https://www.industrialinfo.com/news/article/bayonne-energy-center-to-begin-commercial-operations-soon--219010",
     "notes":"Bayonne Energy Center, Bayonne NJ. 644 MW simple cycle. Phase 1 COD Jun 2012."},
    {"match":{"project_name":"Empire Generating","state":"NY"},
     "date":"2010-09",
     "source":"https://www.gridinfo.com/plant/empire-generating-co-llc/56259 | https://www.ecpgp.com/equity/portfolio/empire",
     "notes":"Empire Generating Co., Rensselaer NY. 635 MW CCGT. COD Sep 2010."},
    {"match":{"project_name":"Kemper County IGCC"},
     "date":"2014-08-09",
     "source":"https://en.wikipedia.org/wiki/Kemper_Project | https://www.southerncompany.com/newsroom/2017/june-2017/0628-kemper.html",
     "notes":"Kemper County MS. Natural gas CC online Aug 9, 2014."},
    # ── Withdrew ───────────────────────────────────────────────────
    {"match":{"project_name":"Nine Mile Point Station Unit #3"},
     "date":"2014-03-31",
     "source":"https://www.federalregister.gov/documents/2014/04/04/2014-07580/nine-mile-point-3-nuclear-project-llc-and-unistar-nuclear-operating-services-llc",
     "notes":"Nine Mile Point Unit 3, Oswego NY. 1,660 MW nuclear. Withdrawal granted Mar 31, 2014."},
    {"match":{"project_name":"Poletti Expansion"},
     "date":"2010-01-31",
     "source":"https://en.wikipedia.org/wiki/Charles_Poletti_Power_Project | https://www.power-eng.com/emissions/ny-power-authority-to-shut-down-poletti-power-project/",
     "notes":"Charles Poletti Power Project, Queens NY. Plant shut Jan 31, 2010."},
]

patch_applied = 0
for patch in patches:
    mask = pd.Series([True]*len(df), index=df.index)
    for col, val in patch["match"].items():
        mask &= df[col].astype(str).str.strip() == str(val).strip()
    n = mask.sum()
    if n == 0:
        print(f"  WARNING: No match for {patch['match']}")
    else:
        for idx in df.index[mask]:
            set_web(df, idx, patch["date"], patch["source"], patch["notes"],
                    overwrite_existing=True)  # Manual patches always win
        patch_applied += n

print(f"  Applied {patch_applied} manual patch rows")


# ═══════════════════════════════════════════════════════════════════════════════
# 2. MISO API matches
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("2. MISO API matches")
print("="*60)

miso_raw = pd.read_csv(f"{BASE}/MISO_Queue_Raw.csv", low_memory=False)
print(f"  MISO rows: {len(miso_raw):,}")

miso_master = df[df["region"]=="MISO"].copy()

# Normalize columns
name_col = "Project Name" if "Project Name" in miso_raw.columns else miso_raw.columns[0]
status_col = [c for c in miso_raw.columns if "status" in c.lower()][0] if any("status" in c.lower() for c in miso_raw.columns) else None
date_cols = [c for c in miso_raw.columns if "in.service" in c.lower() or "online" in c.lower() or "cod" in c.lower()]
wd_cols   = [c for c in miso_raw.columns if "withdraw" in c.lower()]
id_col    = [c for c in miso_raw.columns if "queue" in c.lower() and "id" in c.lower()][0] if any("queue" in c.lower() and "id" in c.lower() for c in miso_raw.columns) else None

print(f"  ID col: {id_col}, Date cols: {date_cols[:3]}, WD cols: {wd_cols[:2]}")

miso_applied = 0
for master_idx, mrow in miso_master.iterrows():
    if pd.notna(df.at[master_idx,"web_found_date"]):
        continue  # Don't overwrite existing good matches

    qid = str(mrow.get("queue_id","")).strip()
    outcome = str(mrow.get("outcome","")).strip()

    # Try direct queue_id match
    match = pd.DataFrame()
    if id_col:
        match = miso_raw[miso_raw[id_col].astype(str).str.strip() == qid]

    if len(match) == 0:
        # Fuzzy name match
        m_name = str(mrow.get("project_name","")).strip()
        m_poi  = str(mrow.get("poi_name","")).strip()
        if m_name in ("","nan","None") and m_poi in ("","nan","None"):
            continue

        best_sc = 0; best_idx = None
        for bidx, br in miso_raw.iterrows():
            iso_name = str(br.get(name_col,"")).strip()
            if iso_name in ("","nan","None"): continue
            sc = max(best_name_score(m_name, iso_name),
                     best_name_score(m_poi,  iso_name))
            if sc > best_sc and sc >= 85:
                best_sc = sc; best_idx = bidx
        if best_idx is None: continue
        match_row = miso_raw.loc[best_idx]
        score_used = best_sc
    else:
        match_row = match.iloc[0]
        score_used = 100

    # Get date
    date_val = None
    if outcome == "came_online":
        for dc in date_cols:
            d = safe_date(match_row.get(dc))
            if d: date_val = d; break
    elif outcome == "withdrew":
        for dc in wd_cols:
            d = safe_date(match_row.get(dc))
            if d: date_val = d; break

    if date_val:
        set_web(df, master_idx, date_val,
                "https://www.misoenergy.org/api/giqueue/getprojects (MISO Queue API)",
                f"MISO match score={score_used}")
        miso_applied += 1

print(f"  MISO matches applied: {miso_applied}")


# ═══════════════════════════════════════════════════════════════════════════════
# 3. CAISO / ERCOT / ISO-NE (gridstatus files)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("3. CAISO / ERCOT / ISO-NE matches")
print("="*60)

iso_files = [
    ("CAISO", f"{BASE}/ISO_Queue_CAISO.csv"),
    ("ERCOT", f"{BASE}/ISO_Queue_ERCOT.csv"),
    ("ISO-NE", f"{BASE}/ISO_Queue_ISONE.csv"),
]

for region_label, iso_path in iso_files:
    try:
        iso_df = pd.read_csv(iso_path, low_memory=False)
    except:
        print(f"  {region_label}: file not found")
        continue

    # Find useful columns
    name_candidates = [c for c in iso_df.columns if any(kw in c.lower() for kw in ["project","name","generator"])]
    date_candidates = [c for c in iso_df.columns if any(kw in c.lower() for kw in ["online","service","cod","operation"])]
    wd_candidates   = [c for c in iso_df.columns if any(kw in c.lower() for kw in ["withdraw","cancel","termina"])]
    id_candidates   = [c for c in iso_df.columns if any(kw in c.lower() for kw in ["queue_id","project_id","id"])]

    name_col_iso = name_candidates[0] if name_candidates else None
    date_col_iso = date_candidates[0] if date_candidates else None
    id_col_iso   = id_candidates[0]   if id_candidates else None

    if not name_col_iso: print(f"  {region_label}: no name col found"); continue

    iso_master = df[df["region"]==region_label].copy()
    applied_iso = 0

    for master_idx, mrow in iso_master.iterrows():
        if pd.notna(df.at[master_idx,"web_found_date"]):
            continue

        m_name = str(mrow.get("project_name","")).strip()
        m_poi  = str(mrow.get("poi_name","")).strip()
        m_qid  = str(mrow.get("queue_id","")).strip()
        outcome= str(mrow.get("outcome","")).strip()

        # Direct ID match first
        match = pd.DataFrame()
        if id_col_iso:
            match = iso_df[iso_df[id_col_iso].astype(str).str.strip() == m_qid]

        if len(match) == 0 and not (m_name in ("","nan","None") and m_poi in ("","nan","None")):
            # Fuzzy
            best_sc = 0; best_idx = None
            for bidx, br in iso_df.iterrows():
                iso_name = str(br.get(name_col_iso,"")).strip()
                if iso_name in ("","nan","None"): continue
                sc = max(best_name_score(m_name, iso_name),
                         best_name_score(m_poi,  iso_name))
                if sc > best_sc and sc >= 85:
                    best_sc = sc; best_idx = bidx
            if best_idx is None: continue
            match_row  = iso_df.loc[best_idx]
            score_used = best_sc
        elif len(match) > 0:
            match_row  = match.iloc[0]
            score_used = 100
        else:
            continue

        # Get date
        date_val = None
        if outcome == "came_online" and date_col_iso:
            date_val = safe_date(match_row.get(date_col_iso))
        elif outcome == "withdrew" and wd_candidates:
            date_val = safe_date(match_row.get(wd_candidates[0]))

        if date_val:
            set_web(df, master_idx, date_val,
                    f"https://opensource.gridstatus.io  (gridstatus Python library v0.21)",
                    f"{region_label} match score={score_used}")
            applied_iso += 1

    print(f"  {region_label}: {applied_iso} new dates applied")


# ═══════════════════════════════════════════════════════════════════════════════
# 4. BPA matches (West region)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("4. BPA (West) matches")
print("="*60)

xl_bpa = pd.ExcelFile(f"{BASE}/BPA_Queue_Real.xlsx")
bpa = pd.read_excel(xl_bpa, sheet_name="Sheet1", header=4)
bpa["req_num"] = bpa["Request Number"].astype(str).str.strip()
bpa_en = bpa[bpa["Status"]=="ENERGIZED"].copy()

west_master = df[df["region"]=="West"].copy()
bpa_applied = 0

for master_idx, mrow in west_master.iterrows():
    if pd.notna(df.at[master_idx,"web_found_date"]):
        continue

    qid     = str(mrow.get("queue_id","")).strip()
    outcome = str(mrow.get("outcome","")).strip()
    m_name  = str(mrow.get("project_name","")).strip()
    m_poi   = str(mrow.get("poi_name","")).strip()
    m_state = str(mrow.get("state","")).strip()

    if outcome != "came_online":
        continue  # BPA "Agreed To" for WITHDRAWN = planned IS date, not withdrawal date

    # Pass 1: direct queue_id
    bpa_match = bpa_en[bpa_en["req_num"] == qid]

    if len(bpa_match) == 1:
        br = bpa_match.iloc[0]
        date_val = safe_date(br.get("Agreed To: (Blank=TBD)")) or safe_date(br.get("Requested In-Service Date"))
        if date_val:
            set_web(df, master_idx, date_val,
                    "BPA Interconnection Queue (direct match)",
                    f"BPA#{qid} ENERGIZED project={br['Project Name']}")
            bpa_applied += 1
        continue

    # Pass 2: fuzzy name (only if both names are non-empty)
    if m_name in ("","nan","None") and m_poi in ("","nan","None"):
        continue

    bpa_states = ["OR","WA","ID","MT"]
    if m_state not in bpa_states:
        continue

    best_sc = 0; best_idx = None
    for bidx, br in bpa_en.iterrows():
        bpa_name = str(br.get("Project Name","")).strip()
        bpa_poi  = str(br.get("Point Of Interconnection","")).strip()
        bpa_st   = str(br.get("State","")).strip()

        if bpa_name in ("","nan","None") and bpa_poi in ("","nan","None"): continue
        if bpa_st and m_state and bpa_st.upper() != m_state.upper(): continue

        sc = max(best_name_score(m_name, bpa_name),
                 best_name_score(m_poi,  bpa_poi),
                 best_name_score(m_name, bpa_poi))
        if sc > best_sc and sc >= 80:
            best_sc = sc; best_idx = bidx

    if best_idx is not None:
        br       = bpa_en.loc[best_idx]
        date_val = safe_date(br.get("Agreed To: (Blank=TBD)")) or safe_date(br.get("Requested In-Service Date"))
        if date_val:
            set_web(df, master_idx, date_val,
                    "BPA Interconnection Queue (fuzzy match)",
                    f"BPA#{br['Request Number']} ENERGIZED score={best_sc:.0f} project={br['Project Name']}")
            bpa_applied += 1

print(f"  BPA matches applied: {bpa_applied}")


# ═══════════════════════════════════════════════════════════════════════════════
# 5. NYISO matches (direct queue position)
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("5. NYISO matches")
print("="*60)

xl_ny = pd.ExcelFile(f"{BASE}/ISO_Queue_NYISO_hist.xlsx")

dr    = pd.read_excel(xl_ny, sheet_name="In Service", header=None)
h0    = dr.iloc[0].fillna(""); h1 = dr.iloc[1].fillna("")
cols  = [f"{a} {b}".strip() for a, b in zip(h0, h1)]
ny_is = dr.iloc[2:].copy(); ny_is.columns = cols; ny_is = ny_is.reset_index(drop=True)
ny_is["qpos_int"] = pd.to_numeric(ny_is["Queue Pos."].astype(str).str.strip(), errors="coerce")

ny_wd    = pd.read_excel(xl_ny, sheet_name="Withdrawn", header=0)
ny_wd_cl = pd.read_excel(xl_ny, sheet_name="Cluster Projects-Withdrawn", header=0)
ny_wd_all = pd.concat([ny_wd, ny_wd_cl], ignore_index=True)
ny_wd_all["qpos_int"] = pd.to_numeric(ny_wd_all["Queue Pos."].astype(str).str.strip(), errors="coerce")

ny_master = df[df["region"]=="NYISO"].copy()
nyiso_applied = 0

for master_idx, mrow in ny_master.iterrows():
    if pd.notna(df.at[master_idx,"web_found_date"]):
        continue

    outcome = str(mrow.get("outcome","")).strip()
    try:
        qid_int = int(float(str(mrow.get("queue_id","")).strip()))
    except:
        continue

    if outcome == "came_online":
        pool = ny_is
    elif outcome == "withdrew":
        pool = ny_wd_all
    else:
        continue

    match = pool[pool["qpos_int"] == qid_int]
    if len(match) == 0:
        # Fuzzy fallback
        m_name = str(mrow.get("project_name","")).strip()
        if m_name in ("","nan","None"):
            continue
        best_sc = 0; best_bidx = None
        for bidx, br in pool.iterrows():
            iso_name = str(br.get("Project Name","")).strip()
            if iso_name in ("","nan","None"): continue
            sc = max(fuzz.token_sort_ratio(m_name.lower(),iso_name.lower()),
                     fuzz.partial_ratio(m_name.lower(),iso_name.lower()))
            if sc > best_sc and sc >= 85:
                best_sc = sc; best_bidx = bidx
        if best_bidx is None: continue
        match_row  = pool.loc[best_bidx]
        score_used = best_sc
    else:
        match_row  = match.iloc[0]
        score_used = 100

    date_val = safe_date(match_row.get("Last Update"))
    if not date_val:
        continue

    set_web(df, master_idx, date_val,
            "NYISO Interconnection Queue",
            f"NYISO qpos={match_row.get('qpos_int')} score={score_used} project={match_row.get('Project Name')}")
    nyiso_applied += 1

print(f"  NYISO matches applied: {nyiso_applied}")


# ═══════════════════════════════════════════════════════════════════════════════
# 6. Rebuild final_date
# ═══════════════════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("6. Rebuilding final_date")
print("="*60)

def final_date_fn(row):
    out = str(row["outcome"]).strip()
    if out == "came_online":
        ad = row.get("activation_date")
        if pd.notna(ad) and str(ad) not in ("nan","None",""): return str(ad), "queue_on_date"
        wd = row.get("web_found_date")
        if pd.notna(wd) and str(wd) not in ("nan","None",""): return str(wd), "web_search"
        yr = row.get("eia_online_year")
        if pd.notna(yr):
            try:
                mo = int(row["eia_online_month"]) if pd.notna(row.get("eia_online_month")) else 1
                return f"{int(yr)}-{mo:02d}", "eia_match"
            except: pass
    elif out == "withdrew":
        wdd = row.get("withdrawal_date")
        if pd.notna(wdd) and str(wdd) not in ("nan","None",""): return str(wdd), "queue_wd_date"
        wd = row.get("web_found_date")
        if pd.notna(wd) and str(wd) not in ("nan","None",""): return str(wd), "web_search"
    elif out == "still_in_queue":
        wd = row.get("web_found_date")
        if pd.notna(wd) and str(wd) not in ("nan","None",""): return str(wd), "web_search"
    return None, "none"

results = df.apply(final_date_fn, axis=1)
df["final_date"]        = [r[0] for r in results]
df["final_date_source"] = [r[1] for r in results]

# ═══════════════════════════════════════════════════════════════════════════════
# 7. Save and summarize
# ═══════════════════════════════════════════════════════════════════════════════
df.to_csv(f"{BASE}/Queue_Full_Status_Timeline.csv", index=False)
print(f"\nSaved: {BASE}/Queue_Full_Status_Timeline.csv")
print(f"Total rows: {len(df):,}")
print()

print(f"{'='*60}")
print("FILL RATE BY REGION")
print(f"{'='*60}")
for region in sorted(df["region"].unique()):
    sub    = df[df["region"]==region]
    filled = sub["final_date"].notna().sum()
    pct    = 100*filled/len(sub)
    miss   = len(sub)-filled
    print(f"  {region:<12} {filled:>5,}/{len(sub):>5,} = {pct:5.1f}%  (missing: {miss:,})")

print(f"\n  TOTAL  {df['final_date'].notna().sum():>6,}/{len(df):>6,} = "
      f"{100*df['final_date'].notna().sum()/len(df):.1f}%")

print(f"\n{'='*60}")
print("WEB SOURCE BREAKDOWN")
print(f"{'='*60}")
print(df["web_found_source"].value_counts().to_string())

print(f"\n{'='*60}")
print("MATCH COUNTS BY ISO")
print(f"{'='*60}")
print(f"  Manual patches : {patch_applied}")
print(f"  MISO API       : {miso_applied}")
print(f"  BPA (West)     : {bpa_applied}")
print(f"  NYISO          : {nyiso_applied}")
