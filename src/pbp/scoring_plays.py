from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

import pandas as pd

from .utils import as_chr, as_int, as_lgl, col_or


@dataclass(frozen=True)
class ScoringPlaysConfig:
    refreshed_at: Optional[str] = None
    season: Optional[int] = None
    week_default: Optional[int] = None


def derive_scoring_plays(pbp: pd.DataFrame, cfg: ScoringPlaysConfig | None = None) -> pd.DataFrame:
    """
    Derive scoring plays from a pbp-like dataframe.

    This is a Python port of your R logic (derive_scoring_plays), with the same intent:
    - identify scoring plays (TD/FG/XP/2pt/Safety/Def 2pt)
    - output a stable schema for downstream upsert/processing

    NOTE:
    nflfastR-style pbp (including nflreadpy) frequently contains nullable / NA booleans.
    This implementation ensures missing values are filled before casting to bool.
    """
    cfg = cfg or ScoringPlaysConfig()
    if pbp is None or pbp.empty:
        return pd.DataFrame()

    n = len(pbp)
    refreshed_at = cfg.refreshed_at or datetime.now().isoformat(timespec="seconds")

    # Source columns (or defaults)
    touchdown = as_int(col_or(pbp, "touchdown", [0] * n))
    safety = as_int(col_or(pbp, "safety", [0] * n))

    field_goal_result = as_chr(col_or(pbp, "field_goal_result", [pd.NA] * n))
    extra_point_result = as_chr(col_or(pbp, "extra_point_result", [pd.NA] * n))
    two_point_conv_result = as_chr(col_or(pbp, "two_point_conv_result", [pd.NA] * n))

    # These can be nullable in nflfastR pbp; as_lgl should normalize, but we still guard later.
    pass_touchdown = as_lgl(col_or(pbp, "pass_touchdown", [False] * n))
    rush_touchdown = as_lgl(col_or(pbp, "rush_touchdown", [False] * n))

    defensive_two_point_conv = as_int(col_or(pbp, "defensive_two_point_conv", [0] * n))

    # Canonical scoring flags
    is_td = touchdown.fillna(0).astype("Int64") == 1
    is_fg = field_goal_result.fillna(pd.NA).str.lower().eq("made")
    is_xp = extra_point_result.fillna(pd.NA).str.lower().isin(["good", "made"])
    is_2pt_off = two_point_conv_result.fillna(pd.NA).str.lower().isin(["success", "good"])
    is_def_two_pt = defensive_two_point_conv.fillna(0).astype("Int64") == 1
    is_safety = safety.fillna(0).astype("Int64") == 1

    # Ensure nullable bools do not propagate NA
    pass_td = pass_touchdown.fillna(False).astype(bool)
    rush_td = rush_touchdown.fillna(False).astype(bool)

    is_td_off = is_td.fillna(False) & (pass_td | rush_td)
    is_td_def = is_td.fillna(False) & (~is_td_off)

    is_scoring_play = (
        is_td.fillna(False)
        | is_fg.fillna(False)
        | is_xp.fillna(False)
        | is_2pt_off.fillna(False)
        | is_safety.fillna(False)
        | is_def_two_pt.fillna(False)
    )

    # Season/week fill behavior
    if "season" in pbp.columns:
        season = as_int(pbp["season"])
    else:
        season = as_int([cfg.season] * n)

    if "week" in pbp.columns:
        week = as_int(pbp["week"])
    else:
        week = as_int([cfg.week_default] * n)

    # Build output rows then filter scoring plays
    out = pbp.copy()
    out["refreshed_at"] = refreshed_at
    out["season"] = season
    out["week"] = week

    out["touchdown"] = touchdown
    out["safety"] = safety
    out["field_goal_result"] = field_goal_result
    out["extra_point_result"] = extra_point_result
    out["two_point_conv_result"] = two_point_conv_result

    out["pass_touchdown"] = pass_td
    out["rush_touchdown"] = rush_td
    out["defensive_two_point_conv"] = defensive_two_point_conv

    out["is_td"] = is_td.fillna(False)
    out["is_fg"] = is_fg.fillna(False)
    out["is_xp"] = is_xp.fillna(False)
    out["is_2pt"] = is_2pt_off.fillna(False)  # keep name aligned with your downstream schema
    out["is_safety"] = is_safety.fillna(False)

    out["is_def_two_pt"] = is_def_two_pt.fillna(False)
    out["is_td_off"] = is_td_off.fillna(False)
    out["is_td_def"] = is_td_def.fillna(False)
    out["is_scoring_play"] = is_scoring_play.fillna(False)

    out = out.loc[out["is_scoring_play"].fillna(False)].copy()

    # Helper to pull or default for schema
    def _c(name: str, default) -> pd.Series:
        return col_or(out, name, default)

    def _as_bool(series_like, m: int) -> pd.Series:
        """
        Convert a column to a strict bool Series, treating missing as False.
        Handles pandas nullable boolean, floats with NaN, objects, etc.
        """
        s = series_like
        if not isinstance(s, pd.Series):
            s = pd.Series(s)
        # Align length defensively if caller passes scalar
        if len(s) != m:
            s = pd.Series([s.iloc[0] if len(s) else False] * m)
        return s.fillna(False).astype(bool)

    m = len(out)
    if m == 0:
        return pd.DataFrame()

    # Stable output schema (matches what your upsert expects)
    result = pd.DataFrame(
        {
            "refreshed_at": as_chr(_c("refreshed_at", [pd.NA] * m)),
            "season": as_int(_c("season", [pd.NA] * m)),
            "week": as_int(_c("week", [pd.NA] * m)),
            "game_id": as_chr(_c("game_id", [pd.NA] * m)),
            "game_date": as_chr(_c("game_date", [pd.NA] * m)),
            "posteam": as_chr(_c("posteam", [pd.NA] * m)),
            "defteam": as_chr(_c("defteam", [pd.NA] * m)),
            "qtr": as_int(_c("qtr", [pd.NA] * m)),
            "time": as_chr(_c("time", [pd.NA] * m)),
            "drive": as_int(_c("drive", [pd.NA] * m)),
            "play_id": as_int(_c("play_id", [pd.NA] * m)),
            "desc": as_chr(_c("desc", [pd.NA] * m)),
            "touchdown": as_int(_c("touchdown", [pd.NA] * m)),
            "field_goal_result": as_chr(_c("field_goal_result", [pd.NA] * m)),
            "extra_point_result": as_chr(_c("extra_point_result", [pd.NA] * m)),
            "two_point_conv_result": as_chr(_c("two_point_conv_result", [pd.NA] * m)),
            "safety": as_int(_c("safety", [pd.NA] * m)),
            "is_td": _as_bool(_c("is_td", [False] * m), m),
            "is_fg": _as_bool(_c("is_fg", [False] * m), m),
            "is_xp": _as_bool(_c("is_xp", [False] * m), m),
            "is_2pt": _as_bool(_c("is_2pt", [False] * m), m),
            "is_safety": _as_bool(_c("is_safety", [False] * m), m),
            "pass_touchdown": _as_bool(_c("pass_touchdown", [False] * m), m),
            "rush_touchdown": _as_bool(_c("rush_touchdown", [False] * m), m),
            "is_td_off": _as_bool(_c("is_td_off", [False] * m), m),
            "is_td_def": _as_bool(_c("is_td_def", [False] * m), m),
            "defensive_two_point_conv": as_int(_c("defensive_two_point_conv", [pd.NA] * m)),
            "is_def_two_pt": _as_bool(_c("is_def_two_pt", [False] * m), m),
            "play_type": as_chr(_c("play_type", [pd.NA] * m)),
            "pass": as_int(_c("pass", [pd.NA] * m)),
            "rush": as_int(_c("rush", [pd.NA] * m)),
            "qb_dropback": as_int(_c("qb_dropback", [pd.NA] * m)),
            "sack": as_int(_c("sack", [pd.NA] * m)),
            "interception": as_int(_c("interception", [pd.NA] * m)),
            "fumble_lost": as_int(_c("fumble_lost", [pd.NA] * m)),
            "return_team": as_chr(_c("return_team", [pd.NA] * m)),
            "passer_player_id": as_chr(_c("passer_player_id", [pd.NA] * m)),
            "passer_player_name": as_chr(_c("passer_player_name", [pd.NA] * m)),
            "receiver_player_id": as_chr(_c("receiver_player_id", [pd.NA] * m)),
            "receiver_player_name": as_chr(_c("receiver_player_name", [pd.NA] * m)),
            "rusher_player_id": as_chr(_c("rusher_player_id", [pd.NA] * m)),
            "rusher_player_name": as_chr(_c("rusher_player_name", [pd.NA] * m)),
            "kicker_player_id": as_chr(_c("kicker_player_id", [pd.NA] * m)),
            "kicker_player_name": as_chr(_c("kicker_player_name", [pd.NA] * m)),
        }
    )

    return result
