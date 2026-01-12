from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

from .nflreadpy_pbp import fetch_pbp_for_game_ids_via_nflreadpy
from .logging import LogRow, write_log_and_status
from .paths import get_paths
from .positions import ensure_player_positions
from .scoring_plays import ScoringPlaysConfig, derive_scoring_plays
from .upsert import upsert_latest_wins


@dataclass(frozen=True)
class RefreshResult:
    rows_in: int
    rows_scoring: int
    rows_out: int
    any_loaded: bool


def _read_csv_if_exists(path: Path) -> pd.DataFrame | None:
    if path.exists():
        return pd.read_csv(path)
    return None


def refresh_pbp(
    *,
    season: int,
    week: int | None,
    game_ids: list[str] | None,
    out_path: Path,
    metrics_out_path: Path | None = None,
) -> RefreshResult:
    """
    Python replacement for refresh_pbp.R.

    This version fetches *live* play-by-play (GameCenter GTD JSON) for the requested
    nflfastR-style game_ids, normalizes into a pbp-like DataFrame, derives scoring plays,
    and upserts into the cumulative scoring_plays CSV.

    Notes:
    - If GTD is not available yet (pre-game) or returns no plays, we do NOT overwrite
      existing outputs with empty data.
    - Per-game metrics for refresh state are computed in refresh.py; metrics_out_path
      here is optional and remains a lightweight run-level record unless you extend it.
    """
    paths = get_paths(str(out_path), season=season)
    paths.processed_dir.mkdir(parents=True, exist_ok=True)

    # positions file: stable (not live), so keep existing behavior
    if paths.positions_path is not None:
        ensure_player_positions(season, paths.positions_path)

    if not game_ids or len(game_ids) == 0:
        raise ValueError("refresh_pbp requires explicit game_ids (live mode).")

    refreshed_at = datetime.now().isoformat(timespec="seconds")

    # Fetch live pbp + per-game metrics (used here only to decide whether anything loaded)
    pbp, metrics = fetch_pbp_for_game_ids_via_nflreadpy(season=season, game_ids=game_ids)
    rows_in = int(len(pbp))

    # Determine whether *any* requested game has any pbp rows loaded
    any_loaded = rows_in > 0

    # Derive scoring plays (your logic)
    scoring = derive_scoring_plays(
        pbp,
        ScoringPlaysConfig(refreshed_at=refreshed_at, season=season, week_default=week),
    ) if any_loaded else pd.DataFrame()

    rows_scoring = int(len(scoring)) if not scoring.empty else 0

    # Read existing cumulative output
    old = _read_csv_if_exists(paths.out_path)

    # If nothing loaded yet and we already have an output file, do nothing destructive.
    if not any_loaded and old is not None and not old.empty:
        # Still write a log/status line indicating no-op
        write_log_and_status(
            LogRow(
                refreshed_at=refreshed_at,
                season=season,
                week=week,
                game_ids=",".join(game_ids),
                rows_in=rows_in,
                rows_scoring=rows_scoring,
                rows_out=len(old),
                status="ok",
                detail="no_live_pbp_available_yet",
            ),
            log_path=paths.log_path,
            status_path=paths.status_path,
        )
        return RefreshResult(rows_in=rows_in, rows_scoring=rows_scoring, rows_out=len(old), any_loaded=False)

    # If no scoring rows but pbp exists, we still allow the upsert (it will be no-op),
    # but we should not overwrite with empty if old exists. Upsert handles this safely.
    if old is None or old.empty:
        combined = scoring.copy() if not scoring.empty else pd.DataFrame()
    else:
        combined = upsert_latest_wins(old, scoring) if not scoring.empty else old.copy()

    rows_out = int(len(combined)) if combined is not None and not combined.empty else 0

    # Write cumulative + latest (only if we have a dataframe to write)
    # If combined is empty and old is empty, it's fine to write empty CSV.
    if combined is None:
        combined = pd.DataFrame()

    combined.to_csv(paths.out_path, index=False)
    combined.to_csv(paths.latest_path, index=False)

    # Optional metrics output: keep it lightweight (run-level record).
    # Per-game state metrics remain owned by refresh.py.
    if metrics_out_path is not None:
        metrics_out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "refreshed_at": refreshed_at,
                    "season": season,
                    "week": week,
                    "game_ids": ",".join(game_ids),
                    "rows_in": rows_in,
                    "rows_scoring": rows_scoring,
                    "rows_out": rows_out,
                }
            ]
        ).to_csv(metrics_out_path, index=False)

    # Log + status (mirrors your helper contract)
    write_log_and_status(
        LogRow(
            refreshed_at=refreshed_at,
            season=season,
            week=week,
            game_ids=",".join(game_ids),
            rows_in=rows_in,
            rows_scoring=rows_scoring,
            rows_out=rows_out,
            status="ok",
            detail="",
        ),
        log_path=paths.log_path,
        status_path=paths.status_path,
    )

    return RefreshResult(rows_in=rows_in, rows_scoring=rows_scoring, rows_out=rows_out, any_loaded=any_loaded)


def main(argv: list[str] | None = None) -> int:
    load_dotenv()

    p = argparse.ArgumentParser()
    p.add_argument("--season", type=int, default=int(os.getenv("BBB_SEASON", "0") or "0"))
    p.add_argument("--week", type=int, default=None)
    p.add_argument("--game_ids", type=str, default="")
    p.add_argument("--out", type=str, default=os.getenv("BBB_OUT_PATH", "data/processed/scoring_plays.csv"))
    p.add_argument("--metrics_out", type=str, default=os.getenv("BBB_METRICS_OUT_PATH", ""))

    args = p.parse_args(argv)

    if not args.season:
        raise SystemExit("season must be provided via --season or BBB_SEASON")

    game_ids = [x.strip() for x in (args.game_ids or "").split(",") if x.strip()]
    metrics_path = Path(args.metrics_out) if args.metrics_out else None

    refresh_pbp(
        season=args.season,
        week=args.week,
        game_ids=game_ids if game_ids else None,
        out_path=Path(args.out),
        metrics_out_path=metrics_path,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
