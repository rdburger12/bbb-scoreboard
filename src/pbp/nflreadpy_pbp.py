
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable, Tuple, List

import pandas as pd


@dataclass(frozen=True)
class NflreadpyMetrics:
    refreshed_at: str
    season: int
    game_id: str
    pbp_rows: int
    status: str
    detail: str = ""


def fetch_pbp_for_game_ids_via_nflreadpy(
    *,
    season: int,
    game_ids: Iterable[str],
) -> Tuple[pd.DataFrame, List[NflreadpyMetrics]]:
    """
    Load nflfastR-style PBP from nflverse via nflreadpy, then filter to the requested game_ids.
    Local-first: no caching controls here beyond whatever nflreadpy does by default.
    """
    import nflreadpy as nfl

    refreshed_at = datetime.now().isoformat(timespec="seconds")
    gids = [str(g).strip() for g in game_ids if str(g).strip()]
    if not gids:
        return pd.DataFrame(), []

    # nflreadpy returns Polars; convert to pandas for your existing pipeline.
    pbp_pl = nfl.load_pbp([season])
    pbp = pbp_pl.to_pandas()

    # Filter to requested games
    pbp = pbp.loc[pbp["game_id"].isin(gids)].copy()

    metrics: list[NflreadpyMetrics] = []
    for gid in gids:
        n = int((pbp["game_id"] == gid).sum())
        metrics.append(
            NflreadpyMetrics(
                refreshed_at=refreshed_at,
                season=season,
                game_id=gid,
                pbp_rows=n,
                status="loaded" if n > 0 else "not_found_in_release",
                detail="",
            )
        )

    return pbp, metrics
