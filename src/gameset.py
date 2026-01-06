from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Literal

import pandas as pd

Mode = Literal["regular_season_dev", "playoffs"]


@dataclass(frozen=True)
class GameSet:
    mode: Mode
    season: int
    week_max: int | None = None
    playoff_game_ids_path: Path | None = None

    def describe(self) -> str:
        if self.mode == "playoffs":
            return f"Playoffs (by game_id): {self.playoff_game_ids_path}"
        return f"Regular season dev: season={self.season}, weeks=1..{self.week_max}"


def load_game_ids(scoring_plays: pd.DataFrame, gs: GameSet) -> set[str]:
    """
    Returns the set of game_ids included in the scoring scope.
    """
    if gs.mode == "playoffs":
        if not gs.playoff_game_ids_path:
            raise ValueError("playoff_game_ids_path is required for playoffs mode.")
        p = gs.playoff_game_ids_path
        if not p.exists():
            # empty set is safer than exploding; UI can message this
            return set()
        df = pd.read_csv(p)
        if "game_id" not in df.columns:
            raise ValueError(f"{p} must contain a 'game_id' column.")
        return set(df["game_id"].dropna().astype(str).tolist())

    # regular season dev: derive from scoring_plays filtered by season/week
    if gs.week_max is None:
        raise ValueError("week_max is required for regular_season_dev.")
    if "game_id" not in scoring_plays.columns:
        raise ValueError("scoring_plays is missing required column 'game_id'.")
    if "season" not in scoring_plays.columns or "week" not in scoring_plays.columns:
        raise ValueError("scoring_plays must include 'season' and 'week' columns for regular season dev mode.")

    df = scoring_plays.copy()
    df["game_id"] = df["game_id"].astype(str)
    df["season"] = df["season"].astype("Int64")
    df["week"] = df["week"].astype("Int64")

    df = df[(df["season"] == int(gs.season)) & (df["week"] >= 1) & (df["week"] <= int(gs.week_max))]
    return set(df["game_id"].dropna().unique().tolist())
