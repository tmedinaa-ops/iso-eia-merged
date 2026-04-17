"""
Enrich queue data with:
  - Outcome: still_in_queue | came_online | withdrew
  - Queue step (for active/suspended projects)
  - Activation/withdrawal date and duration from queue entry
"""

import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings("ignore")

# Resolve data directory: honor PLANNING_QUEUES_DATA env var; otherwise default
# to repo/data relative to this file's location.
BASE = os.environ.get(
    "PLANNING_QUEUES_DATA",
    str(Path(__file__).resolve().parents[2] / "data"),
)
TODAY = datetime(2026, 3, 7)   # reference "current" date

# ── Date helpers ──────────────────────────────────────────────────────────────
def excel_to_dt(n):
    """Excel serial → datetime, or None."""
    try:
        n = float(n)
        if pd.isna(n) or n <= 0:
            return None
        if n > 60:
            n -= 1  # Excel 1900 leap-year bug
        return datetime(1899, 12, 31) + timedelta(days=n)
    except Exception:
        return None

def fmt(dt):
    try:
        if dt is None or (hasattr(dt, 'isnull') and dt.isnull()):
            return None
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def duration_str(start_dt, end_dt):
    """Return human-readable duration between two datetimes."""
    if start_dt is None or end_dt is None:
        return None
    delta = end_dt - start_dt
    days  = delta.days
    if days < 0:
        days = abs(days)
    years  = days // 365
    months = (days % 365) // 30
    rem    = (days % 365) % 30
    parts  = []
    if years:  parts.append(f"{years}y")
    if months: parts.append(f"{months}m")
    if rem or not parts: parts.append(f"{rem}d")
    return " ".join(parts)

def duration_days(start_dt, end_dt):
    if start_dt is None or end_dt is None:
        return None
    return (end_dt - start_dt).days

# ── Queue-step ordering (for display) ─────────────────────────────────────────
STEP_ORDER = {
    "Not Started":              1,
    "Feasibility Study":        2,
    "System Impact Study":      3,
    "Cluster Study":            3,
    "In Progress (unknown study)": 4,
    "Facility Study":           5,
    "IA Pending":               6,
    "IA Executed":              7,
    "Construction":             8,
    "Operational":              9,
    "Suspended":               10,
    "Withdrawn":               11,
    "Combined":                 4,
}

# ── Load combined file ────────────────────────────────────────────────────────
print("Loading combined queue+EIA file...")
df = pd.read_csv(f"{BASE}/Queue_with_EIA_Online_Dates.csv", low_memory=False)
print(f"  Rows: {len(df):,}")

# ── Convert all date columns ──────────────────────────────────────────────────
print("Converting date columns...")
df["q_date_dt"]    = df["q_date"].apply(excel_to_dt)
df["on_date_dt"]   = df["on_date"].apply(excel_to_dt)
df["wd_date_dt"]   = df["wd_date"].apply(excel_to_dt)
df["ia_date_dt"]   = df["ia_date"].apply(excel_to_dt)
df["prop_date_dt"] = df["prop_date"].apply(excel_to_dt)

# Use object dtype to avoid NaT/strftime issues
df["q_date_str"]    = [fmt(x) for x in df["q_date_dt"]]
df["wd_date_str"]   = [fmt(x) for x in df["wd_date_dt"]]
df["ia_date_str"]   = [fmt(x) for x in df["ia_date_dt"]]
df["prop_date_str"] = [fmt(x) for x in df["prop_date_dt"]]

# on_date_converted already exists from previous script
# Use it plus EIA fallback for activation date
def get_activation_dt(row):
    # Priority 1: queue on_date
    if row["on_date_dt"]:
        return row["on_date_dt"]
    # Priority 2: EIA online year/month
    yr = row.get("eia_online_year")
    if pd.notna(yr) and yr > 0:
        mo = int(row["eia_online_month"]) if pd.notna(row.get("eia_online_month")) else 1
        try:
            return datetime(int(yr), mo, 1)
        except Exception:
            pass
    return None

df["activation_dt"] = df.apply(get_activation_dt, axis=1)
df["activation_date_str"] = df["activation_dt"].apply(fmt)

# ── Determine outcome and resolution date ─────────────────────────────────────
def classify(row):
    qs = str(row.get("q_status","")).strip().lower()
    ia = str(row.get("IA_status_clean","")).strip()

    if qs == "operational":
        return "came_online"
    elif qs == "withdrawn":
        return "withdrew"
    elif qs in ("active", "suspended", "unknown"):
        return "still_in_queue"
    else:
        # Fall back to IA status
        if "withdrawn" in ia.lower():
            return "withdrew"
        elif ia in ("Operational",):
            return "came_online"
        else:
            return "still_in_queue"

df["outcome"] = df.apply(classify, axis=1)

def resolution_dt(row):
    out = row["outcome"]
    if out == "came_online":
        return row["activation_dt"]
    elif out == "withdrew":
        return row["wd_date_dt"]
    else:
        return TODAY   # still in queue: measure to today

df["resolution_dt"] = df.apply(resolution_dt, axis=1)

# ── Duration from queue entry to resolution ───────────────────────────────────
df["queue_duration_days"] = df.apply(
    lambda r: duration_days(r["q_date_dt"], r["resolution_dt"]), axis=1
)
df["queue_duration"] = df.apply(
    lambda r: duration_str(r["q_date_dt"], r["resolution_dt"]), axis=1
)

# ── Queue step label ──────────────────────────────────────────────────────────
def queue_step(row):
    out = row["outcome"]
    ia  = str(row.get("IA_status_clean","")).strip()
    qs  = str(row.get("q_status","")).strip().lower()
    raw = str(row.get("IA_status_raw","")).strip()

    if out == "came_online":
        return "Operational (came online)"
    elif out == "withdrew":
        return "Withdrawn"
    else:
        # still in queue
        if ia and ia not in ("nan",""):
            return f"Still in Queue – {ia}"
        elif raw and raw not in ("nan",""):
            return f"Still in Queue – {raw[:60]}"
        elif qs == "suspended":
            return "Still in Queue – Suspended"
        else:
            return "Still in Queue"

df["queue_step_label"] = df.apply(queue_step, axis=1)

# ── Build final enriched table ────────────────────────────────────────────────
print("Building output table...")

out = df[[
    # Identifiers
    "q_id", "q_status", "region", "state", "county",
    "poi_name", "project_name", "utility", "developer",
    # Technology & capacity
    "type_clean", "type1", "type2", "type3", "mw1", "mw2", "mw3",
    "service", "cluster",
    # Queue dates
    "q_date_str", "q_year",
    "prop_date_str", "prop_year",
    "ia_date_str",
    # Status / step
    "IA_status_clean", "IA_status_raw",
    "outcome", "queue_step_label",
    # Resolution dates
    "activation_date_str", "wd_date_str",
    # Duration
    "queue_duration", "queue_duration_days",
    # EIA match info
    "eia_matched_plant", "match_score", "match_confidence",
    "eia_online_year", "eia_online_month",
    "eia_technologies", "eia_total_capacity_mw",
    "online_date_source",
]].copy()

# Rename for clarity
out.columns = [
    "queue_id", "q_status", "region", "state", "county",
    "poi_name", "project_name", "utility", "developer",
    "technology", "type1", "type2", "type3", "mw1", "mw2", "mw3",
    "service", "cluster",
    "queue_entry_date", "queue_entry_year",
    "proposed_online_date", "proposed_online_year",
    "ia_signed_date",
    "ia_status_clean", "ia_status_raw",
    "outcome", "queue_step",
    "activation_date", "withdrawal_date",
    "time_in_queue", "time_in_queue_days",
    "eia_matched_plant", "eia_match_score", "eia_match_confidence",
    "eia_online_year", "eia_online_month",
    "eia_technologies", "eia_total_capacity_mw",
    "online_date_source",
]

out_path = f"{BASE}/Queue_Full_Status_Timeline.csv"
out.to_csv(out_path, index=False)
print(f"\nSaved: {out_path}  ({len(out):,} rows × {len(out.columns)} columns)")

# ── Summary stats ─────────────────────────────────────────────────────────────
print(f"\n{'='*65}")
print("OUTCOME SUMMARY")
print(f"{'='*65}")
oc = out["outcome"].value_counts()
for o, n in oc.items():
    pct = 100*n/len(out)
    print(f"  {o:<20} {n:>7,}  ({pct:.1f}%)")

print(f"\n{'='*65}")
print("QUEUE STEP BREAKDOWN (still_in_queue only)")
print(f"{'='*65}")
siq = out[out["outcome"]=="still_in_queue"]
step_ct = siq["ia_status_clean"].value_counts()
for s, n in step_ct.items():
    pct = 100*n/len(siq)
    print(f"  {s:<35} {n:>6,}  ({pct:.1f}%)")

print(f"\n{'='*65}")
print("AVERAGE TIME IN QUEUE (days) BY OUTCOME")
print(f"{'='*65}")
avg = out.groupby("outcome")["time_in_queue_days"].agg(["mean","median","min","max"])
print(avg.round(0).astype("Int64").to_string())

print(f"\n{'='*65}")
print("AVERAGE TIME IN QUEUE BY QUEUE STEP (still in queue)")
print(f"{'='*65}")
step_dur = (
    siq.groupby("ia_status_clean")["time_in_queue_days"]
    .agg(count="count", mean_days="mean", median_days="median")
    .sort_values("mean_days", ascending=False)
)
print(step_dur.round(0).astype({"count":int}).to_string())

print(f"\n{'='*65}")
print("SAMPLE: Came Online (first 20)")
print(f"{'='*65}")
s1 = out[out["outcome"]=="came_online"][
    ["queue_id","poi_name","project_name","state","technology","mw1",
     "queue_entry_date","activation_date","time_in_queue"]
].dropna(subset=["activation_date"]).head(20)
print(s1.to_string(index=False))

print(f"\n{'='*65}")
print("SAMPLE: Withdrew (first 20)")
print(f"{'='*65}")
s2 = out[out["outcome"]=="withdrew"][
    ["queue_id","poi_name","project_name","state","technology","mw1",
     "queue_entry_date","withdrawal_date","time_in_queue"]
].dropna(subset=["withdrawal_date"]).head(20)
print(s2.to_string(index=False))

print(f"\n{'='*65}")
print("SAMPLE: Still in Queue (first 20, active)")
print(f"{'='*65}")
s3 = out[
    (out["outcome"]=="still_in_queue") & (out["q_status"]=="active")
][
    ["queue_id","poi_name","project_name","state","technology","mw1",
     "queue_entry_date","ia_status_clean","time_in_queue"]
].head(20)
print(s3.to_string(index=False))

print(f"\nDone. Full output: {out_path}")
