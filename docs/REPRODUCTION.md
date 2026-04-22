# Reproduction guide

This document walks through every step of reproducing the results. If you only want the short version, see the "Reproduce in three steps" section of the top-level `README.md`.

## What "reproduce" means here

Two scopes, in order of effort:

| Scope | Inputs required | Command | Produces |
|---|---|---|---|
| **Analysis + dashboard** | `data/ISO_EIA_Merged_v7.csv` | `make all` | `build/analysis/*.html`, `build/interconnection_county_explorer.html` |
| **Full pipeline from raw** | all LBNL + EIA + ISO source files | `python src/python/apply_all_iso_matches.py` then `make all` | everything above, plus `data/ISO_EIA_Merged_v7.csv` |

The analysis path is the one the Makefile supports as a single command. The full-pipeline path is documented but not automated end-to-end because the raw source files are not yet redistributable (see `DATA_ACCESS.md`).

## Prerequisites

- Python ≥ 3.9 with `pip`
- R ≥ 4.2 with `Rscript` on your `PATH`
- `make` (ships with macOS Command Line Tools and most Linux distros)
- Internet access on first run (the dashboard downloads a 3 MB GeoJSON and R packages from CRAN)

Disk: the merged CSV is ~20 MB, rendered analysis HTML is a few MB per file, and the dashboard HTML is ~30 MB.

## Analysis + dashboard (recommended)

1. **Place the merged CSV** at `data/ISO_EIA_Merged_v7.csv`. See `DATA_ACCESS.md` for how to obtain it.

2. **Install dependencies:**

       make install

   This runs `pip install -r requirements.txt` and `Rscript scripts/install_r_packages.R`. Both are idempotent — rerunning is cheap.

3. **Preflight check:**

       make check

   Reports Python version, Python package presence and minimum versions, R + package presence, and whether the v7 CSV is in place. Exits non-zero on any blocker.

4. **Run everything:**

       make all

   This renders the seven R Markdown analyses in dependency-safe order (descriptives first, then Stage 0 missingness, then Stage 1 IPW construction and validation, then Stage 2 policy model), and builds the dashboard. Elapsed time on a recent laptop: roughly 5–15 minutes depending on R package install state and the IPW bootstrap budget.

   Individual targets if you want to run pieces:

       make analysis           # just the Rmds
       make dashboard          # just the county explorer
       Rscript scripts/render_analysis.R policy_model   # one Rmd at a time

5. **Inspect outputs** under `build/`:

       build/analysis/
           queue_time_analysis.html
           queue_completion_times.html
           queue_time_by_technology.html
           missingness_mechanism_analysis.html
           compute_ipw_weights.html
           ipw_weight_validation.html
           policy_model.html
       build/interconnection_county_explorer.html
       build/counties.geojson           (cached for subsequent dashboard builds)

   Open any of them in a browser.

## Full pipeline from raw inputs

If you have the original LBNL Queued Up workbook, the EIA Form 860 extract, and the seven ISO queue files:

1. Place them directly under `data/` (flat layout — no `raw/` subfolder needed).

2. Run the orchestrator:

       python3 src/python/apply_all_iso_matches.py

   This runs match_queue_to_eia → enrich_queue_status → patch_web_dates → cross_ref_iso_queues → iso_ne_fix → final merge, and writes `data/ISO_EIA_Merged_v7.csv`.

3. Proceed with `make all` as above.

This path is brittle in practice: file names, sheet names, and schema columns in the raw LBNL and EIA files change between annual releases. Expect to adjust `match_queue_to_eia.py` and possibly `enrich_queue_status.py` for each new vintage.

## Customizing paths

All three env vars below have repo-relative defaults; set them to redirect inputs or outputs without touching code:

- `PLANNING_QUEUES_DATA` — directory that contains `ISO_EIA_Merged_v7.csv` and any raw pipeline inputs. Default `<repo>/data`.
- `PLANNING_QUEUES_BUILD` — directory where rendered HTML and cached artifacts land. Default `<repo>/build`.
- `PLANNING_QUEUES_DATA_V7` — full path to the merged CSV, used only by the R Markdown files. Default `<repo>/data/ISO_EIA_Merged_v7.csv`.
- `PLANNING_QUEUES_MODEL_DIR` — where the Rmds read and write fitted model artifacts (IPW weights, etc.). Default `<repo>/data`.

Example, running against a dataset on an external drive without touching the repo:

    export PLANNING_QUEUES_DATA=/Volumes/Research/planning_queues/data
    export PLANNING_QUEUES_BUILD=/Volumes/Research/planning_queues/build
    make all

## Known reproducibility gaps

These are real limitations worth flagging to anyone trying to reproduce the paper exactly:

1. **Descriptive Rmds still read v5 and a pipeline intermediate, not v7.**
   Three of the seven Rmds — `queue_completion_times.Rmd`,
   `queue_time_analysis.Rmd`, `queue_time_by_technology.Rmd` — point at
   `ISO_EIA_Merged_v5.csv` and `TIQ_Analysis_Clean.csv` (a 9,256-row
   filtered extract from an earlier vintage). The Stage 0-2 policy
   pipeline (missingness, IPW construction, IPW validation, policy
   model) correctly reads v7. All four analysis CSVs (v5, v7,
   `TIQ_Analysis_Clean.csv`, `EIA_860_All_Generators.csv`) are checked
   into `data/` so `make all` works today, but the descriptive analyses
   are running against a 65,203-row v5 snapshot while the policy model
   runs against the 65,717-row v7.

   **Target end state:** repo ships v7 and `EIA_860_All_Generators.csv`
   only; v5 and `TIQ_Analysis_Clean.csv` get deleted. v5 is a strict
   subset of v7 (same schema, 514 fewer ISO-NE rows — the rows that
   `iso_ne_fix.py` inserts). `TIQ_Analysis_Clean.csv` is a pipeline
   intermediate that can be regenerated from v7 inline with a filter
   block. `EIA_860_All_Generators.csv` stays because it is the
   independent ground-truth universe used by section 8 of
   `queue_completion_times.Rmd` as the denominator for the
   capture-rate chart — v7 contains EIA metadata only for matched
   rows, so it cannot reconstruct that denominator.

   **Migration steps:**

   *Step 1 — queue_completion_times.Rmd.* Change the filename on line 34
   from `ISO_EIA_Merged_v5.csv` to `ISO_EIA_Merged_v7.csv`. Update the
   caption on line 152 and the footer on line 675 to match. Expect row
   counts to shift by ~514 rows. No other code changes; the two files
   share a schema. Section 8 already reads `EIA_860_All_Generators.csv`;
   leave it alone.

   *Step 2 — queue_time_analysis.Rmd and queue_time_by_technology.Rmd.*
   Replace the `read_csv(... "TIQ_Analysis_Clean.csv" ...)` block with a
   v7 read that applies the TIQ filter inline:
   ```r
   df_raw <- read_csv(file.path(DATA_DIR, "ISO_EIA_Merged_v7.csv"),
                      show_col_types = FALSE) %>%
     filter(status_group %in% c("completed", "withdrawn"),
            !is.na(time_in_queue), time_in_queue > 0) %>%
     mutate(outcome           = if_else(status_group == "completed",
                                        "came_online", "withdrew"),
            queue_entry_year  = year(as.Date(queue_date)),
            mw1               = mw,
            region            = lbnl_region,
            time_in_queue_days = time_in_queue) %>%
     filter(queue_entry_year >= 2000, queue_entry_year <= 2019)
   ```
   Before committing, confirm that v7's `time_in_queue` is already in
   days (TIQ uses `time_in_queue_days`). If it is not, adjust the
   rename. The existing downstream `mutate` blocks rely on `mw1`,
   `region`, `type1`, `outcome`, `queue_entry_year`, and
   `time_in_queue_days`, so the shim above covers them.

   *Step 3 — verification.* Before deleting anything, render all seven
   Rmds against v7 and diff the rendered HTML, plus the IPW weights
   CSV, against the current v5/TIQ outputs. Expect descriptive numbers
   to move slightly because of the 514 added ISO-NE rows. Flag any
   headline number that moves by more than ~2% and update the paper
   text before shipping.

   *Step 4 — cleanup.* Once the diff is reviewed:
   `git rm data/ISO_EIA_Merged_v5.csv data/TIQ_Analysis_Clean.csv`,
   remove the corresponding `!data/...` whitelist entries from
   `.gitignore`, drop this gap entry, and update `DATA_ACCESS.md` so it
   lists only v7 and `EIA_860_All_Generators.csv` as the required
   inputs.

   Estimate: 2-4 hours if descriptive numbers move only at the noise
   level; longer if any headline needs a paper edit.

2. **Python dependency versions are lower-bounded, not pinned.** `requirements.txt` uses `>=` constraints. For bit-exact reproduction, pin to the exact versions used in the paper (add a `requirements.lock` via `pip-compile` before publishing).

3. **R packages aren't locked.** The installer pulls current CRAN versions. For archival reproduction, initialize `renv` at the repo root (`renv::init()`) and commit the resulting `renv.lock`. This is on the TODO list but not yet done.

4. **Raw-data pipeline isn't automated end-to-end.** The raw LBNL/EIA/ISO files aren't redistributable yet, so the `make all` path starts from the merged CSV rather than rebuilding it. Once the archive release is live, this guide will be updated with a `make pipeline` target that downloads the raw files and rebuilds `ISO_EIA_Merged_v7.csv` from scratch.

5. **Right-censoring of active projects.** The policy model's completion hazard estimate is subject to right-censoring bias; see `CLAUDE.md` in the Obsidian vault and the paper for the AFT sensitivity analysis.
