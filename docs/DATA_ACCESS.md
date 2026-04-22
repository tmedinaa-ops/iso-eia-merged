# Data access

The merged dataset (`ISO_EIA_Merged_v7.csv`) and the raw source files it is built from are **not** included in this repository. They will be released through an academic data archive (Zenodo or ICPSR) after the accompanying data descriptor (Arriaga-Medina, 2026) is accepted for publication.

## What you need to reproduce the analyses

For the "hit run" reproduction path (`make all`), the only file required is the merged CSV:

    data/ISO_EIA_Merged_v7.csv

Everything downstream — the seven R Markdown analyses, the county dashboard — is derived from this one file.

## How to obtain the merged CSV

### Pre-release access (reviewers, collaborators, students)

Contact the author for pre-release access under a data-use agreement:

- Tomas Arriaga-Medina, Carnegie Mellon University
- Email: tmedinaa@andrew.cmu.edu

### Post-release

Once the archive DOI is minted, this file will be updated with a direct download link and the exact archive record identifier. Verify the CSV by checking its SHA-256 against the value published in the release notes.

## Full pipeline reproduction (rebuild v7 from raw inputs)

If you have the original LBNL Queued Up workbook, EIA Form 860 extract, and the seven ISO queue files, you can regenerate `ISO_EIA_Merged_v7.csv` from scratch using the Python pipeline in `src/python/`. Place the raw files directly under `data/` (flat layout) and run:

    python3 src/python/apply_all_iso_matches.py

This is the fragile path — file names and workbook sheet layouts change year to year, and the raw files are not yet redistributable. See `docs/REPRODUCTION.md` for the step-by-step.

## Layout

    data/
    ├── ISO_EIA_Merged_v7.csv     # you provide this
    ├── raw/                       # optional: raw ISO/LBNL/EIA files
    └── processed/                 # optional: intermediate outputs

Both `raw/` and `processed/` are retained as conventions for users who want to keep inputs and intermediates separate. The pipeline itself reads from and writes to `data/` at the flat level.
