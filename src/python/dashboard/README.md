# County-level dashboard

Generates `build/interconnection_county_explorer.html` from
`data/ISO_EIA_Merged_v7.csv`. The output is a single self-contained HTML file
(~30 MB) — all project rows, county aggregates, and the US counties GeoJSON
are inlined so it works offline once opened.

## One command

From the repo root:

    make dashboard

or directly:

    python src/python/dashboard/build_dashboard.py

## What it does

1. Reads the merged v7 CSV.
2. Normalizes county names (handles NYC, Miami-Dade, VA independent cities,
   the "St" → "St." variant, and a few AK/NC edge cases) and attaches a
   5-digit FIPS code using the `addfips` library. Current match rate is
   ~93.4% of rows; the rest are "Unknown"/"TBD" or multi-county projects.
3. Groups by FIPS and computes `n_rows`, `avg_time_in_queue`, `total_mw`,
   and the distinct ISO set per county.
4. Downloads the Plotly US-counties GeoJSON once (cached under `build/`).
5. Inlines the aggregate dict, per-county row arrays, column list, county
   labels, and the GeoJSON into `dashboard_template.html` and writes the
   final HTML.

## Paths

Both the input CSV location and the output directory are configurable with
environment variables, consistent with the rest of the pipeline:

    export PLANNING_QUEUES_DATA=/path/to/data        # default: <repo>/data
    export PLANNING_QUEUES_BUILD=/path/to/build       # default: <repo>/build

Or pass them on the command line:

    python src/python/dashboard/build_dashboard.py \
        --input  /path/to/ISO_EIA_Merged_v7.csv \
        --out    /path/to/explorer.html \
        --agg-csv /path/to/county_agg.csv

## Metric coverage

The map supports three metrics: project count, average time in queue, total
MW. Project count is always populated; the other two are null for
`eia_only` / `lbnl_only` source rows that have no queue timestamps or
no nameplate capacity. Counties with projects but a null metric value are
shaded in soft grey and remain clickable.
