from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd


@dataclass(frozen=True)
class LogRow:
    refreshed_at: str
    season: int | None = None
    week: int | None = None
    game_ids: str | None = None
    rows_in: int | None = None
    rows_scoring: int | None = None
    rows_out: int | None = None
    status: str | None = None
    detail: str | None = None


def write_log_and_status(row: LogRow, log_path: Path, status_path: Path) -> None:
    """
    Python port of r/lib/logging.R::write_log_and_status(). :contentReference[oaicite:11]{index=11}
    """
    row_df = pd.DataFrame([asdict(row)])

    # Schema-change rotation (same intent as R)
    if log_path.exists():
        try:
            header = list(pd.read_csv(log_path, nrows=0).columns)
        except Exception:
            header = []
        if header and header != list(row_df.columns):
            rotated = log_path.with_name(
                log_path.name.replace(
                    ".csv",
                    f"_old_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                )
            )
            log_path.rename(rotated)

    # Append or create
    if log_path.exists():
        row_df.to_csv(log_path, index=False, header=False, mode="a")
    else:
        row_df.to_csv(log_path, index=False)

    # Status is always last row only
    row_df.to_csv(status_path, index=False)
