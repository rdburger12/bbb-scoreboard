from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class Paths:
    out_path: Path
    processed_dir: Path
    latest_path: Path
    log_path: Path
    status_path: Path
    positions_path: Optional[Path]


def get_paths(out_path: str | None = None, season: int | None = None) -> Paths:
    """
    Python port of r/lib/paths.R::get_paths(). :contentReference[oaicite:9]{index=9}
    """
    Path("data/processed").mkdir(parents=True, exist_ok=True)

    out_path_str = out_path or "data/processed/scoring_plays.csv"
    outp = Path(out_path_str)
    processed_dir = outp.parent

    positions_path = None
    if season is not None:
        positions_path = processed_dir / f"player_positions_{season}.csv"

    return Paths(
        out_path=outp,
        processed_dir=processed_dir,
        latest_path=processed_dir / "scoring_plays_latest.csv",
        log_path=processed_dir / "refresh_log.csv",
        status_path=processed_dir / "refresh_status.csv",
        positions_path=positions_path,
    )
