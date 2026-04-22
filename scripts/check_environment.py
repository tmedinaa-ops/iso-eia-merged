#!/usr/bin/env python3
"""
Preflight check: verify the Python version, required packages, and input
data are all in place before running the pipeline or dashboard build.

Exits 0 on success, 1 on any failure. Intended to run as `make check`.
"""

from __future__ import annotations

import importlib.util
import os
import shutil
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DATA = REPO_ROOT / "data"
DATA_DIR = Path(os.environ.get("PLANNING_QUEUES_DATA", str(DEFAULT_DATA)))
V7_CSV = DATA_DIR / "ISO_EIA_Merged_v7.csv"

REQUIRED_PY = (3, 9)

PY_PACKAGES = [
    ("pandas",    "2.0"),
    ("numpy",     "1.24"),
    ("scipy",     "1.11"),
    ("openpyxl",  "3.1"),
    ("rapidfuzz", "3.0"),
    ("addfips",   None),  # dashboard dep, no strict version floor
]

R_PACKAGES = [
    "tidyverse", "survival", "survminer", "cmprsk", "quantreg",
    "Kendall", "scales", "patchwork", "ggridges", "kableExtra",
    "viridis", "rmarkdown", "knitr",
]

OK    = "\033[32m[ok]\033[0m"
WARN  = "\033[33m[warn]\033[0m"
FAIL  = "\033[31m[fail]\033[0m"
PLAIN = "[..]"


class Report:
    def __init__(self):
        self.failures: list[str] = []
        self.warnings: list[str] = []

    def ok(self, msg):   print(f"  {OK} {msg}")
    def warn(self, msg):
        self.warnings.append(msg)
        print(f"  {WARN} {msg}")
    def fail(self, msg):
        self.failures.append(msg)
        print(f"  {FAIL} {msg}")
    def heading(self, msg): print(f"\n{msg}")


def check_python(r: Report) -> None:
    r.heading("Python interpreter")
    major, minor = sys.version_info.major, sys.version_info.minor
    ver = f"{major}.{minor}.{sys.version_info.micro}"
    if (major, minor) >= REQUIRED_PY:
        r.ok(f"Python {ver} (>= {REQUIRED_PY[0]}.{REQUIRED_PY[1]})")
    else:
        r.fail(f"Python {ver} is below the required {REQUIRED_PY[0]}.{REQUIRED_PY[1]}")


def _parse_version(v: str) -> tuple[int, ...]:
    out = []
    for part in v.split("."):
        try:
            out.append(int(part))
        except ValueError:
            break
    return tuple(out)


def check_py_packages(r: Report) -> None:
    r.heading("Python packages")
    for name, floor in PY_PACKAGES:
        spec = importlib.util.find_spec(name)
        if spec is None:
            r.fail(f"{name} is not installed (pip install -r requirements.txt)")
            continue
        try:
            mod = __import__(name)
            ver = getattr(mod, "__version__", "?")
        except Exception as e:
            r.warn(f"{name} installed but import failed: {e}")
            continue
        if floor and _parse_version(ver) < _parse_version(floor):
            r.warn(f"{name} {ver} is below the lower bound {floor}")
        else:
            r.ok(f"{name} {ver}")


def check_r(r: Report) -> None:
    r.heading("R environment")
    rscript = shutil.which("Rscript")
    if rscript is None:
        r.warn("Rscript not found on PATH - R analyses cannot run.")
        return
    try:
        out = subprocess.check_output(
            [rscript, "--version"], stderr=subprocess.STDOUT, text=True, timeout=10,
        )
        first = out.strip().splitlines()[0]
        r.ok(f"Rscript at {rscript} - {first}")
    except Exception as e:
        r.warn(f"Rscript present but `Rscript --version` failed: {e}")
        return

    # Probe packages in a single R call for speed
    probe = ",".join(f'"{p}"' for p in R_PACKAGES)
    cmd = [
        rscript, "-e",
        f"installed <- rownames(installed.packages()); "
        f"pkgs <- c({probe}); "
        f"for (p in pkgs) cat(p, ifelse(p %in% installed, 'y', 'n'), '\\n', sep=' ')",
    ]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT, text=True, timeout=60)
    except subprocess.CalledProcessError as e:
        r.warn(f"R package probe failed: {e.output.strip()}")
        return
    missing = []
    for line in out.strip().splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[-1] == "n":
            missing.append(parts[0])
    if missing:
        r.warn(
            "missing R packages: "
            + ", ".join(missing)
            + "  (run: Rscript scripts/install_r_packages.R)"
        )
    else:
        r.ok(f"all {len(R_PACKAGES)} R packages present")


def check_data(r: Report) -> None:
    r.heading("Data")
    r.ok(f"PLANNING_QUEUES_DATA resolves to {DATA_DIR}")
    if not DATA_DIR.exists():
        r.fail(f"data directory does not exist: {DATA_DIR}")
        return
    if V7_CSV.exists():
        size_mb = V7_CSV.stat().st_size / 1e6
        r.ok(f"{V7_CSV.name} present ({size_mb:.1f} MB)")
    else:
        r.fail(
            f"{V7_CSV.name} not found in {DATA_DIR}. "
            f"Place the merged v7 CSV there, or set PLANNING_QUEUES_DATA "
            f"to the directory that contains it."
        )


def main() -> int:
    print("== planning-queues preflight ==")
    r = Report()
    check_python(r)
    check_py_packages(r)
    check_r(r)
    check_data(r)
    print()
    if r.failures:
        print(f"{FAIL} {len(r.failures)} blocker(s):")
        for f in r.failures:
            print(f"    - {f}")
    if r.warnings:
        print(f"{WARN} {len(r.warnings)} warning(s):")
        for w in r.warnings:
            print(f"    - {w}")
    if r.failures:
        return 1
    if not r.warnings:
        print(f"{OK} all checks passed.")
    else:
        print(f"{OK} no blockers; warnings above are advisory.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
