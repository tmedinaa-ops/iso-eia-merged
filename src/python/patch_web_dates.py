"""
Patch Queue_Full_Status_Timeline.csv with web-sourced dates.
Matches on (queue_id + project_name) or (queue_id + state) to avoid
collisions from non-unique queue IDs across utilities.
"""
import os
from pathlib import Path
import pandas as pd
import warnings
warnings.filterwarnings("ignore")

# Resolve data directory: honor PLANNING_QUEUES_DATA env var; otherwise default
# to repo/data relative to this file's location.
BASE = os.environ.get(
    "PLANNING_QUEUES_DATA",
    str(Path(__file__).resolve().parents[2] / "data"),
)
df = pd.read_csv(f"{BASE}/Queue_Full_Status_Timeline.csv", low_memory=False)
print(f"Loaded {len(df):,} rows")

# Only initialize columns if they don't already exist
# (preserves ISO cross-reference matches from other scripts)
if "web_found_date" not in df.columns:
    df["web_found_date"]   = None
if "web_found_source" not in df.columns:
    df["web_found_source"] = None
if "web_notes" not in df.columns:
    df["web_notes"]        = None

# Each patch: match_col/match_val uniquely identifies the row(s)
# Use project_name when available, or poi_name, to avoid cross-utility collisions
patches = [
    # ── Came Online ──────────────────────────────────────────────────────────
    {
        "match": {"project_name": "Plant McDonough, Units 4B, 4STG, 5B, 5STG, & 6CC"},
        "date":   "2011-12",
        "source": "https://www.prnewswire.com/news-releases/georgia-power-brings-second-plant-mcdonough-atkinson-natural-gas-unit-into-service-149488845.html | https://www.gem.wiki/McDonough_Steam_Generating_Plant",
        "notes":  "First CC block Dec 2011, second Apr 2012, third Nov 2012. Dec 2011 used as earliest COD.",
    },
    {
        "match": {"project_name": "Athens Gen", "state": "NY"},
        "date":   "2004-05",
        "source": "https://www.gridinfo.com/plant/athens-generating-plant/55405 | https://www.gem.wiki/Athens_generating_plant",
        "notes":  "Athens Generating Plant, Greene County NY. 1,080 MW CCGT. COD May 2004.",
    },
    {
        "match": {"project_name": "Astoria Energy - Phase 1"},
        "date":   "2006-05-21",
        "source": "https://www.power-eng.com/gas/nyc-astoria-500-mw-plant-opens/ | https://www.power-technology.com/projects/500mw/",
        "notes":  "Astoria Energy I, Queens NY. 500 MW CCGT. Commercial operation May 21, 2006.",
    },
    {
        "match": {"project_name": "Astoria Energy II", "state": "NY"},
        "date":   "2011-07-01",
        "source": "https://www.powermag.com/top-plantastoria-ii-combined-cycle-plant-queens-new-york/",
        "notes":  "Astoria Energy II, Queens NY. 617 MW CCGT. COD July 1, 2011.",
    },
    {
        "match": {"project_name": "Kleen Energy Project"},
        "date":   "2011-07",
        "source": "https://www.kyuden-intl.co.jp/en/business/kleen.html | https://www.gem.wiki/Kleen_Energy_Systems_Project",
        "notes":  "Kleen Energy, Middletown CT. 620 MW CCGT. COD July 2011 (delayed from Nov 2010 by Feb 2010 explosion).",
    },
    {
        "match": {"project_name": "CPV Towantic Energy Center"},
        "date":   "2018-05-21",
        "source": "https://www.globenewswire.com/news-release/2018/06/14/1524636/0/en/Competitive-Power-Ventures-and-GE-Achieve-Commercial-Operation-at-805-MW-CPV-Towantic-Energy-Center-Connecticut.html",
        "notes":  "CPV Towantic, Oxford CT. 805 MW CCGT. Substantial completion May 21, 2018; official COD June 14, 2018.",
    },
    {
        "match": {"project_name": "CPV Valley Energy Center"},
        "date":   "2018-09",
        "source": "https://www.cpv.com/2018/09/30/competitive-power-ventures-achieves-commercial-operation-at-680-mw-cpv-valley-energy-center-new-york/",
        "notes":  "CPV Valley, Wawayanda NY. 680 MW CCGT. COD September 2018.",
    },
    {
        "match": {"project_name": "Shepherds Flat"},
        "date":   "2012-09",
        "source": "https://en.wikipedia.org/wiki/Shepherds_Flat_Wind_Farm | https://www.energy.gov/lpo/shepherds-flat | https://www.oregon.gov/energy/facilities-safety/facilities/Pages/SFN.aspx",
        "notes":  "Shepherds Flat Wind Farm, Gilliam/Morrow OR. 845 MW. Official opening Sep 22, 2012; full ops Nov 2012.",
    },
    {
        "match": {"project_name": "Biglow Canyon Wind"},
        "date":   "2007-12",
        "source": "https://en.wikipedia.org/wiki/Biglow_Canyon_Wind_Farm | https://investors.portlandgeneral.com/news-releases/news-release-details/final-phase-pges-biglow-canyon-wind-farm-begins-spin-power",
        "notes":  "Biglow Canyon Wind Farm (Portland General Electric), Sherman OR. Phase 1: Dec 2007 (125 MW); Phase 2: Aug 2009 (275 MW); Phase 3: Q3 2010 (450 MW total).",
    },
    {
        "match": {"project_name": "Carty CCCT"},
        "date":   "2016-07-29",
        "source": "https://www.power-eng.com/renewables/portland-general-s-carty-plant-goes-into-service-renewable-rfp-shelved/ | https://www.oregon.gov/energy/facilities-safety/facilities/pages/cgs.aspx",
        "notes":  "Carty Generating Station, Boardman OR. ~440 MW natural gas (Portland General Electric). COD July 29, 2016.",
    },
    {
        "match": {"project_name": "J K Spruce 2"},
        "date":   "2010-05-28",
        "source": "https://www.powermag.com/top-plantj-k-spruce-2-calaveras-power-station-san-antonio-texas/ | https://en.wikipedia.org/wiki/Calaveras_Power_Station",
        "notes":  "J.K. Spruce Unit 2, Calaveras Power Station, Bexar TX. 750 MW supercritical coal. COD May 28, 2010.",
    },
    {
        "match": {"project_name": "Bayonne Energy Center"},
        "date":   "2012-06",
        "source": "https://en.wikipedia.org/wiki/Bayonne_Energy_Center | https://www.industrialinfo.com/news/article/bayonne-energy-center-to-begin-commercial-operations-soon--219010",
        "notes":  "Bayonne Energy Center, Bayonne NJ. 644 MW dual-fuel simple cycle. Phase 1 (Units 1-8) COD June 2012.",
    },
    {
        "match": {"project_name": "Empire Generating", "state": "NY"},
        "date":   "2010-09",
        "source": "https://www.gridinfo.com/plant/empire-generating-co-llc/56259 | https://www.ecpgp.com/equity/portfolio/empire",
        "notes":  "Empire Generating Co., Rensselaer NY. 635 MW CCGT dual-fuel. COD September 2010.",
    },
    {
        "match": {"project_name": "Kemper County IGCC"},
        "date":   "2014-08-09",
        "source": "https://en.wikipedia.org/wiki/Kemper_Project | https://www.southerncompany.com/newsroom/2017/june-2017/0628-kemper.html",
        "notes":  "Kemper County, MS. Natural gas CC online Aug 9, 2014. IGCC/gasification abandoned Jun 28, 2017 after $7.5B cost overrun. Now gas-only.",
    },
    # ── Withdrew ─────────────────────────────────────────────────────────────
    {
        "match": {"project_name": "Nine Mile Point Station Unit #3"},
        "date":   "2014-03-31",
        "source": "https://www.federalregister.gov/documents/2014/04/04/2014-07580/nine-mile-point-3-nuclear-project-llc-and-unistar-nuclear-operating-services-llc | https://www.nrc.gov/reactors/new-reactors/large-lwr/col/nine-mile-point",
        "notes":  "Nine Mile Point Unit 3, Oswego NY. 1,660 MW nuclear (UniStar/Constellation+EDF). Withdrawal requested Nov 26, 2013; NRC granted Mar 31, 2014. Reason: no federal loan guarantees.",
    },
    {
        "match": {"project_name": "Poletti Expansion"},
        "date":   "2010-01-31",
        "source": "https://en.wikipedia.org/wiki/Charles_Poletti_Power_Project | https://www.power-eng.com/emissions/ny-power-authority-to-shut-down-poletti-power-project/",
        "notes":  "Charles Poletti Power Project (NYPA), Queens NY. Plant permanently shut Jan 31, 2010. Proposed expansion never built; replaced by Astoria Energy I & II.",
    },
]

# Apply patches using multi-column exact matching
updated_rows = 0
for patch in patches:
    mask = pd.Series([True] * len(df), index=df.index)
    for col, val in patch["match"].items():
        mask &= df[col].astype(str).str.strip() == str(val).strip()
    n = mask.sum()
    if n == 0:
        print(f"  WARNING: No match for {patch['match']}")
    else:
        df.loc[mask, "web_found_date"]   = patch["date"]
        df.loc[mask, "web_found_source"] = patch["source"]
        df.loc[mask, "web_notes"]        = patch["notes"]
        updated_rows += n
        print(f"  Patched {n} row(s): {patch['match']}")

print(f"\nTotal rows patched: {updated_rows}")

# Build final_date column: best available date per row
def final_date(row):
    if row["outcome"] == "came_online":
        if pd.notna(row.get("activation_date")) and str(row["activation_date"]) not in ("nan","None",""):
            return str(row["activation_date"]), "queue_on_date"
        if row["web_found_date"]:
            return str(row["web_found_date"]), "web_search"
        if pd.notna(row.get("eia_online_year")):
            mo = int(row["eia_online_month"]) if pd.notna(row.get("eia_online_month")) else 1
            return f"{int(row['eia_online_year'])}-{mo:02d}", "eia_match"
    elif row["outcome"] == "withdrew":
        if pd.notna(row.get("withdrawal_date")) and str(row["withdrawal_date"]) not in ("nan","None",""):
            return str(row["withdrawal_date"]), "queue_wd_date"
        if row["web_found_date"]:
            return str(row["web_found_date"]), "web_search"
    elif row["outcome"] == "still_in_queue":
        if row["web_found_date"]:
            return str(row["web_found_date"]), "web_search"
    return None, "none"

df[["final_date","final_date_source"]] = df.apply(
    lambda r: pd.Series(final_date(r)), axis=1
)

# Save
out = f"{BASE}/Queue_Full_Status_Timeline.csv"
df.to_csv(out, index=False)
print(f"\nSaved: {out}")
print(f"Total rows: {len(df):,}")
print(f"Rows with web_found_date: {df['web_found_date'].notna().sum()}")
print(f"Rows with final_date:     {df['final_date'].notna().sum()}")

# Summary of patched rows
print(f"\n=== Patched rows ===")
patched = df[df["web_found_date"].notna()][
    ["queue_id","project_name","state","outcome","web_found_date","time_in_queue","web_found_source"]
]
for _, r in patched.iterrows():
    src_short = str(r["web_found_source"])[:70]
    print(f"  {r['queue_id']:<12} | {str(r['project_name']):<48} | {str(r['outcome']):<12} | {str(r['web_found_date']):<12} | {r['time_in_queue']}")
    print(f"              Source: {src_short}...")
