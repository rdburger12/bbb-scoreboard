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
    Python port of r/lib/scoring_plays.R::derive_scoring_plays().
    Mirrors the canonical BBB flags and output schema. :contentReference[oaicite:5]{index=5}
    """
    cfg = cfg or ScoringPlaysConfig()
    if pbp is None or pbp.empty:
        return pd.DataFrame()

    n = len(pbp)
    refreshed_at = cfg.refreshed_at or datetime.now().isoformat(timespec="seconds")

    # Pull raw columns (or defaults)
    touchdown = as_int(col_or(pbp, "touchdown", [0] * n))
    safety = as_int(col_or(pbp, "safety", [0] * n))

    field_goal_result = as_chr(col_or(pbp, "field_goal_result", [pd.NA] * n))
    extra_point_result = as_chr(col_or(pbp, "extra_point_result", [pd.NA] * n))
    two_point_conv_result = as_chr(col_or(pbp, "two_point_conv_result", [pd.NA] * n))

    pass_touchdown = as_lgl(col_or(pbp, "pass_touchdown", [False] * n))
    rush_touchdown = as_lgl(col_or(pbp, "rush_touchdown", [False] * n))

    defensive_two_point_conv = as_int(col_or(pbp, "defensive_two_point_conv", [0] * n))

    # Canonical scoring booleans
    is_td = touchdown.fillna(0).astype("Int64") == 1

    is_fg = field_goal_result.fillna(pd.NA).str.lower().eq("made")
    is_xp = extra_point_result.fillna(pd.NA).str.lower().isin(["good", "made"])

    is_2pt_off = two_point_conv_result.fillna(pd.NA).str.lower().isin(["success", "good"])

    is_def_two_pt = defensive_two_point_conv.fillna(0).astype("Int64") == 1

    is_safety = safety.fillna(0).astype("Int64") == 1

    is_td_off = is_td & (pass_touchdown | rush_touchdown)
    is_td_def = is_td & (~is_td_off)

    is_scoring_play = is_td | is_fg | is_xp | is_2pt_off | is_safety | is_def_two_pt

    # Season/week fill behavior (match R)
    if "season" in pbp.columns:
        season = as_int(pbp["season"])
    else:
        season = as_int([cfg.season] * n)

    if "week" in pbp.columns:
        week = as_int(pbp["week"])
    else:
        week = as_int([cfg.week_default] * n)

    # Build output rows and then filter scoring plays
    out = pbp.copy()
    out["refreshed_at"] = refreshed_at
    out["season"] = season
    out["week"] = week

    out["touchdown"] = touchdown
    out["safety"] = safety
    out["field_goal_result"] = field_goal_result
    out["extra_point_result"] = extra_point_result
    out["two_point_conv_result"] = two_point_conv_result

    out["pass_touchdown"] = pass_touchdown
    out["rush_touchdown"] = rush_touchdown
    out["defensive_two_point_conv"] = defensive_two_point_conv

    out["is_td"] = is_td
    out["is_fg"] = is_fg
    out["is_xp"] = is_xp
    out["is_2pt"] = is_2pt_off  # keep name consistent with your CSV schema
    out["is_safety"] = is_safety

    out["is_def_two_pt"] = is_def_two_pt
    out["is_td_off"] = is_td_off
    out["is_td_def"] = is_td_def

    out["is_scoring_play"] = is_scoring_play

    out = out.loc[out["is_scoring_play"].fillna(False)].copy()

    # Output schema mirrors the R transmute()
    def _c(name: str, default) -> pd.Series:
        return col_or(out, name, default)

    m = len(out)
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
            "is_td": _c("is_td", [False] * m).astype(bool),
            "is_fg": _c("is_fg", [False] * m).astype(bool),
            "is_xp": _c("is_xp", [False] * m).astype(bool),
            "is_2pt": _c("is_2pt", [False] * m).astype(bool),
            "is_safety": _c("is_safety", [False] * m).astype(bool),
            "pass_touchdown": _c("pass_touchdown", [False] * m).astype(bool),
            "rush_touchdown": _c("rush_touchdown", [False] * m).astype(bool),
            "is_td_off": _c("is_td_off", [False] * m).astype(bool),
            "is_td_def": _c("is_td_def", [False] * m).astype(bool),
            "defensive_two_point_conv": as_int(_c("defensive_two_point_conv", [pd.NA] * m)),
            "is_def_two_pt": _c("is_def_two_pt", [False] * m).astype(bool),
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
