from __future__ import annotations

import subprocess
from pathlib import Path


def run_refresh(*, root: Path, r_script: Path, season: int, week: int) -> subprocess.CompletedProcess[str]:
    """
    Run the R refresh for a single week.

    Parameters
    ----------
    root:
        Repo root (used as subprocess cwd so relative paths in R behave).
    r_script:
        Path to r/refresh_pbp.R
    season, week:
        Values passed to the script.

    Returns
    -------
    subprocess.CompletedProcess[str]
    """
    return subprocess.run(
        ["Rscript", str(r_script), "--season", str(season), "--week", str(week)],
        cwd=str(root),
        capture_output=True,
        text=True,
    )
