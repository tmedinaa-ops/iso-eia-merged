# planning-queues
#
# One-command reproduction. Assumes data/ISO_EIA_Merged_v7.csv is in place
# (see docs/DATA_ACCESS.md). Run `make help` for the target list.

PYTHON  ?= python3
RSCRIPT ?= Rscript

REPO_ROOT := $(abspath $(dir $(firstword $(MAKEFILE_LIST))))
export PLANNING_QUEUES_DATA  ?= $(REPO_ROOT)/data
export PLANNING_QUEUES_BUILD ?= $(REPO_ROOT)/build

V7_CSV := $(PLANNING_QUEUES_DATA)/ISO_EIA_Merged_v7.csv
DASHBOARD_OUT := $(PLANNING_QUEUES_BUILD)/interconnection_county_explorer.html
ANALYSIS_DIR := $(PLANNING_QUEUES_BUILD)/analysis

.PHONY: help install install-python install-r check analysis dashboard all clean clean-build

help:  ## Show this help
	@echo "planning-queues -- reproduction targets"
	@echo ""
	@echo "Typical use:"
	@echo "  1. place ISO_EIA_Merged_v7.csv in data/"
	@echo "  2. make install"
	@echo "  3. make all"
	@echo ""
	@echo "Targets:"
	@awk 'BEGIN {FS = ":.*##"; printf "  \033[36m%-18s\033[0m %s\n", "target", "description"} \
		/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
	@echo ""
	@echo "Env vars (with defaults):"
	@echo "  PLANNING_QUEUES_DATA   $(PLANNING_QUEUES_DATA)"
	@echo "  PLANNING_QUEUES_BUILD  $(PLANNING_QUEUES_BUILD)"

install: install-python install-r  ## Install both Python and R dependencies

install-python:  ## pip install -r requirements.txt
	$(PYTHON) -m pip install -r requirements.txt

install-r:  ## Install R packages (idempotent)
	$(RSCRIPT) scripts/install_r_packages.R

check:  ## Preflight: verify env, packages, and that v7 CSV is present
	$(PYTHON) scripts/check_environment.py

analysis: check  ## Knit all seven Rmds into build/analysis/
	$(RSCRIPT) scripts/render_analysis.R

dashboard: check  ## Build the interactive county explorer HTML
	$(PYTHON) src/python/dashboard/build_dashboard.py

all: check analysis dashboard  ## Run everything end-to-end
	@echo ""
	@echo "Done."
	@echo "  Analysis HTML:  $(ANALYSIS_DIR)/"
	@echo "  Dashboard HTML: $(DASHBOARD_OUT)"

clean: clean-build  ## Remove build artifacts (keeps data/)

clean-build:
	rm -rf $(PLANNING_QUEUES_BUILD)
