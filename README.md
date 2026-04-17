# planning-queues

Pipeline and analysis code for the ISO_EIA_Merged_v7 interconnection queue dataset, a cross-ISO merge of U.S. interconnection queue records, LBNL Queued Up, and EIA Form 860.

The accompanying data descriptor paper (Arriaga-Medina, 2026) documents the construction methodology, the variable schema, and known limitations. This repository holds the pipeline code that produces the dataset and the downstream R analysis code that consumes it.

## Repository layout

    .
    ├── src/
    │   ├── python/          Data-construction pipeline (6 modules)
    │   └── r/               Downstream survival and policy analysis (7 R Markdown)
    ├── data/                Working directory for raw inputs and pipeline outputs
    │   ├── raw/             Optional subfolder (gitignored)
    │   └── processed/       Optional subfolder (gitignored)
    ├── docs/                Data descriptor and supplementary documentation
    ├── requirements.txt     Python dependencies
    └── .gitignore

## Paths are self-contained

All scripts resolve their data directory from a single environment variable with a repo-relative default. Python modules use:

    BASE = os.environ.get(
        "PLANNING_QUEUES_DATA",
        str(Path(__file__).resolve().parents[2] / "data"),
    )

R Markdown files use the equivalent pattern:

    DATA_PATH <- Sys.getenv(
      "PLANNING_QUEUES_DATA_V7",
      "../../data/ISO_EIA_Merged_v7.csv"
    )
    MODEL_DIR <- Sys.getenv("PLANNING_QUEUES_MODEL_DIR", "../../data")

If `PLANNING_QUEUES_DATA` is unset, Python scripts read and write under `repo/data/`. If the two R env vars are unset, the R Markdown files read `../../data/ISO_EIA_Merged_v7.csv` (resolved from `src/r/`) and write IPW artifacts to the same folder.

To point the pipeline at a different location, export the variables before running. For example:

    export PLANNING_QUEUES_DATA=/path/to/your/data
    python3 src/python/apply_all_iso_matches.py

## Data placement

Place the seven ISO queue files, the LBNL Queued Up workbook, and the EIA Form 860 extract directly under `data/` (flat). The pipeline reads from and writes to `data/`; it does not require the raw/processed subsplit. The optional `data/raw/` and `data/processed/` subfolders are retained for users who want to separate inputs from intermediate and final outputs by hand.

The merged dataset and the raw source files are not checked into this repository. They will be released separately through an academic data archive (Zenodo or ICPSR) after journal acceptance. For pre-release access under a data-use agreement, contact the author.

## Pipeline (Python)

Modules in execution order, from `src/python/`:

1. `match_queue_to_eia.py` — three-pass rapidfuzz name match of LBNL and ISO queue entries to EIA-860 generators; applies fuel blocking, minimum name length, and capacity ratio gates.
2. `enrich_queue_status.py` — classifies each row into completed, withdrawn, or active; computes time_in_queue in days; writes resolution_date and resolution_source.
3. `patch_web_dates.py` — applies manually researched online and withdrawal dates as the highest-priority layer.
4. `cross_ref_iso_queues.py` — targeted cross-reference against BPA, NYISO, and SPP source files to recover LBNL gaps.
5. `iso_ne_fix.py` — resolves the v5 ISO-NE queue_id collision; inserts absent LBNL ISO-NE rows without modifying existing v5 records.
6. `apply_all_iso_matches.py` — orchestrator; runs the full pipeline and emits the final merged CSV.

Dependencies: pandas (≥2.0), rapidfuzz (≥3.0), openpyxl (≥3.1), scipy (≥1.11), numpy (≥1.24). See `requirements.txt` for pinned versions.

## Analysis (R)

Files in `src/r/`:

- `queue_time_analysis.Rmd` — trend decomposition of queue durations.
- `queue_completion_times.Rmd` — cohort survival curves for completed projects.
- `queue_time_by_technology.Rmd` — stratified analysis by fuel category.
- `missingness_mechanism_analysis.Rmd` — diagnoses the missingness mechanism for IPW eligibility.
- `compute_ipw_weights.Rmd` — constructs inverse probability weights for the policy model.
- `ipw_weight_validation.Rmd` — diagnostic checks on the computed weights (positivity, balance, variance).
- `policy_model.Rmd` — Fine-Gray competing-risks model of completion versus withdrawal.

R package dependencies: tidyverse, survival, survminer, cmprsk, quantreg, Kendall, scales, patchwork, ggridges, kableExtra, viridis.

## Citation

Arriaga-Medina, T. (2026). Constructing a Cross-ISO Interconnection Queue Dataset for U.S. Electricity Policy Research: Methodology, Provenance, and Known Limitations of ISO_EIA_Merged_v7. Working paper, Carnegie Mellon University.

## License

The code in this repository is released under the MIT License (see `LICENSE`). The merged dataset itself is distributed separately through an academic data archive and carries its own license terms; commercial redistribution of the dataset is restricted pending release review.
