from __future__ import annotations

from typing import Iterable, Set

import pandas as pd

from src.domain.teams import canonicalize_team_column
from src.pbp.schedule import load_schedules


def compute_eliminated_teams(*, season: int, playoff_game_ids: Iterable[str]) -> Set[str]:
    """
    Returns the set of teams eliminated from the playoffs (i.e., teams that have lost
    a completed playoff game in `playoff_game_ids`).

    Uses schedule results (home_score/away_score) from nflreadpy schedules.
    """
    gids = {str(g).strip() for g in playoff_game_ids if str(g).strip()}
    if not gids:
        return set()

    sched = load_schedules(season)
    if sched is None or sched.empty:
        return set()

    # Filter to the configured playoff games only
    if "game_id" not in sched.columns:
        return set()

    sub = sched.loc[sched["game_id"].astype(str).isin(gids)].copy()
    if sub.empty:
        return set()

    # Need these columns to determine winner/loser
    needed = {"home_team", "away_team", "home_score", "away_score"}
    if not needed.issubset(sub.columns):
        return set()

    # Only completed games (scores present)
    sub["home_score"] = pd.to_numeric(sub["home_score"], errors="coerce")
    sub["away_score"] = pd.to_numeric(sub["away_score"], errors="coerce")
    sub = sub.loc[sub["home_score"].notna() & sub["away_score"].notna()].copy()
    if sub.empty:
        return set()

    # Canonicalize team abbreviations to match your scoreboard/draft conventions
    sub = canonicalize_team_column(sub, "home_team")
    sub = canonicalize_team_column(sub, "away_team")

    eliminated: set[str] = set()

    for _, r in sub.iterrows():
        ht = str(r["home_team"])
        at = str(r["away_team"])
        hs = float(r["home_score"])
        a_s = float(r["away_score"])

        if hs == a_s:
            # Playoff ties shouldn't happen; ignore defensively.
            continue

        loser = at if hs > a_s else ht
        eliminated.add(loser)

    return eliminated
