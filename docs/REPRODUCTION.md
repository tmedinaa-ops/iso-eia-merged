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

1. **Python dependency versions are lower-bounded, not pinned.** `requirements.txt` uses `>=` constraints. For bit-exact reproduction, pin to the exact versions used in the paper (add a `requirements.lock` via `pip-compile` before publishing).

2. **R packages aren't locked.** The installer pulls current CRAN versions. For archival reproduction, initialize `renv` at the repo root (`renv::init()`) and commit the resulting `renv.lock`. This is on the TODO list but not yet done.

3. **Raw-data pipeline isn't automated end-to-end.** The raw LBNL/EIA/ISO files aren't redistributable yet, so the `make all` path starts from the merged CSV rather than rebuilding it. Once the archive release is live, this guide will be updated with a `make pipeline` target that downloads the raw files and rebuilds `ISO_EIA_Merged_v7.csv` from scratch.

4. **Right-censoring of active projects.** The policy model's completion hazard estimate is subject to right-censoring bias; see `CLAUDE.md` in the Obsidian vault and the paper for the AFT sensitivity analysis.
