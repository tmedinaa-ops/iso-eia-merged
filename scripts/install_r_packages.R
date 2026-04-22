#!/usr/bin/env Rscript
# Install the R packages the analysis Rmds need.
#
# Idempotent: skips any package that's already installed unless --force is passed.
# Writes nothing outside the R library path.

args <- commandArgs(trailingOnly = TRUE)
force <- "--force" %in% args

packages <- c(
  # core + modeling
  "tidyverse", "survival", "survminer", "cmprsk", "quantreg", "Kendall",
  # plotting + tables
  "scales", "patchwork", "ggridges", "kableExtra", "viridis",
  # rendering
  "rmarkdown", "knitr"
)

repo <- "https://cloud.r-project.org"

to_install <- if (force) packages else setdiff(packages, rownames(installed.packages()))

if (length(to_install) == 0) {
  message("All required R packages are already installed.")
  quit(save = "no", status = 0)
}

message(sprintf("Installing %d package(s): %s",
                length(to_install), paste(to_install, collapse = ", ")))
install.packages(to_install, repos = repo, Ncpus = max(1, parallel::detectCores() - 1))

# Verify
missing <- setdiff(packages, rownames(installed.packages()))
if (length(missing) > 0) {
  message(sprintf("ERROR: failed to install: %s", paste(missing, collapse = ", ")))
  quit(save = "no", status = 1)
}
message("R package install complete.")
