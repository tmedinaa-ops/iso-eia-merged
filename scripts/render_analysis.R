#!/usr/bin/env Rscript
# Knit every Rmd in src/r/ in dependency-safe order and drop the rendered
# HTML into build/analysis/. Intermediate artifacts (fitted IPW weights, model
# objects) still land in data/, which is where the Rmds expect them.
#
# Order
# -----
#   1. queue_time_analysis              (descriptive)
#   2. queue_completion_times           (descriptive)
#   3. queue_time_by_technology         (descriptive)
#   4. missingness_mechanism_analysis   (Stage 0 - produces eligibility)
#   5. compute_ipw_weights              (Stage 1 - produces IPW artifacts)
#   6. ipw_weight_validation            (Stage 1 diagnostics)
#   7. policy_model                     (Stage 2 - reads IPW artifacts)
#
# Usage
# -----
#   Rscript scripts/render_analysis.R            # render all
#   Rscript scripts/render_analysis.R policy_model
#   Rscript scripts/render_analysis.R --skip-descriptive

suppressPackageStartupMessages({
  library(rmarkdown)
})

# Resolve the repo root from this script's path (works under Rscript, source(),
# and interactive use).
find_script_dir <- function() {
  cmd_args <- commandArgs(trailingOnly = FALSE)
  file_arg <- grep("^--file=", cmd_args, value = TRUE)
  if (length(file_arg) > 0) {
    return(dirname(normalizePath(sub("^--file=", "", file_arg[1]))))
  }
  this_frame <- sys.frame(1)
  if (!is.null(this_frame$ofile)) return(dirname(normalizePath(this_frame$ofile)))
  normalizePath(".")
}
script_dir <- find_script_dir()
repo_root  <- normalizePath(file.path(script_dir, ".."), mustWork = FALSE)
if (!file.exists(file.path(repo_root, "src", "r"))) {
  # Fall back to cwd if the layout doesn't match
  repo_root <- normalizePath(".")
}

r_dir      <- file.path(repo_root, "src", "r")
build_dir  <- file.path(repo_root, "build", "analysis")
dir.create(build_dir, recursive = TRUE, showWarnings = FALSE)

all_rmds <- c(
  "queue_time_analysis",
  "queue_completion_times",
  "queue_time_by_technology",
  "missingness_mechanism_analysis",
  "compute_ipw_weights",
  "ipw_weight_validation",
  "policy_model"
)

# Arg parsing
args <- commandArgs(trailingOnly = TRUE)
skip_descriptive <- "--skip-descriptive" %in% args
selected <- setdiff(args, c("--skip-descriptive"))

if (length(selected) == 0) {
  to_run <- all_rmds
} else {
  bad <- setdiff(selected, all_rmds)
  if (length(bad) > 0) {
    stop("Unknown Rmd name(s): ", paste(bad, collapse = ", "),
         "\nAvailable: ", paste(all_rmds, collapse = ", "))
  }
  to_run <- selected
}
if (skip_descriptive) {
  to_run <- setdiff(to_run, c(
    "queue_time_analysis", "queue_completion_times", "queue_time_by_technology"
  ))
}

results <- list()
for (name in to_run) {
  rmd <- file.path(r_dir, paste0(name, ".Rmd"))
  if (!file.exists(rmd)) {
    message(sprintf("[skip] %s - file not found at %s", name, rmd))
    results[[name]] <- "missing"
    next
  }
  message(sprintf("\n[render] %s", name))
  t0 <- Sys.time()
  ok <- tryCatch({
    rmarkdown::render(
      rmd,
      output_dir  = build_dir,
      output_file = paste0(name, ".html"),
      quiet       = TRUE,
      envir       = new.env(parent = globalenv())
    )
    TRUE
  }, error = function(e) {
    message(sprintf("  FAILED: %s", conditionMessage(e)))
    FALSE
  })
  dt <- round(as.numeric(Sys.time() - t0, units = "secs"), 1)
  results[[name]] <- if (ok) sprintf("ok (%ss)", dt) else "failed"
  message(sprintf("  %s", results[[name]]))
}

message("\n== Render summary ==")
for (name in names(results)) {
  message(sprintf("  %-34s %s", name, results[[name]]))
}

failed <- sum(vapply(results, function(x) x == "failed", logical(1)))
if (failed > 0) {
  message(sprintf("\n%d Rmd(s) failed. See output above.", failed))
  quit(save = "no", status = 1)
}
message(sprintf("\nAll %d rendered to: %s", length(to_run), build_dir))
