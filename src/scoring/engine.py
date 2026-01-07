# src/scoring/engine.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .io import normalize_position, clean_id

POS_K = "K"
POS_OTH = "OTH"
POSITION_BUCKETS = ["QB", "RB", "WR", "TE", "K", "OTH"]


@dataclass(frozen=True)
class ScoreRules:
    passing_td: int = 4
    rushing_td: int = 6
    receiving_td: int = 6
    fg_made: int = 3
    xp_made: int = 1
    passing_2pt: int = 1
    rushing_2pt: int = 2
    receiving_2pt: int = 2
    safety: int = 2
    def_2pt_return: int = 2
    def_td: int = 6


def _is_true(x) -> pd.Series:
    if isinstance(x, pd.Series) and pd.api.types.is_bool_dtype(x):
        return x.fillna(False)
    if isinstance(x, pd.Series) and pd.api.types.is_numeric_dtype(x):
        return x.fillna(0).astype(int).eq(1)
    if not isinstance(x, pd.Series):
        x = pd.Series(x)
    return x.fillna("").astype(str).str.lower().isin({"1", "true", "t", "yes"})


def _apply_scope(
    scoring_plays: pd.DataFrame,
    *,
    season: int,
    week_max: int | None,
    game_ids: Iterable[str] | None,
) -> pd.DataFrame:
    df = scoring_plays.copy()

    if "season" in df.columns:
        df = df[df["season"].astype("Int64") == int(season)]

    if game_ids is not None:
        gids = {str(g) for g in game_ids}
        df = df[df["game_id"].astype(str).isin(gids)]
    elif week_max is not None:
        df = df[df["week"].astype("Int64") <= int(week_max)]

    return df


def _build_events(
    scoring_plays: pd.DataFrame,
    rosters: pd.DataFrame,
    *,
    season: int,
    week_max: int | None = None,
    game_ids: Iterable[str] | None = None,
    rules: ScoreRules = ScoreRules(),
) -> pd.DataFrame:
    """
    Canonical event builder. Everything else (totals, feeds, diagnostics) is derived from this.
    """
    df = _apply_scope(scoring_plays, season=season, week_max=week_max, game_ids=game_ids)
    if df.empty:
        return pd.DataFrame(columns=["game_id", "play_id", "game_date", "qtr", "time", "team", "player_id", "position", "pts", "reason", "desc"])

    # Required cols
    posteam = df["posteam"].fillna("").astype(str)
    defteam = df["defteam"].fillna("").astype(str)
    play_type = df.get("play_type", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()

    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in df.columns:
            df[c] = clean_id(df[c])

    # Flags from scoring_plays.R
    is_fg = _is_true(df.get("is_fg", False))
    is_xp = _is_true(df.get("is_xp", False))
    is_safety = _is_true(df.get("is_safety", False))

    is_td_off = _is_true(df.get("is_td_off", False))
    is_td_def = _is_true(df.get("is_td_def", False))
    pass_td_flag = _is_true(df.get("pass_touchdown", False))
    rush_td_flag = _is_true(df.get("rush_touchdown", False))

    is_def_two_pt = _is_true(df.get("is_def_two_pt", False))
    if "defensive_two_point_conv" in df.columns:
        is_def_two_pt = is_def_two_pt | (
            pd.to_numeric(df["defensive_two_point_conv"], errors="coerce")
            .fillna(0)
            .astype(int)
            .eq(1)
        )

    is_2pt_off = _is_true(df.get("is_2pt", False)) & (~is_def_two_pt)

    has_passer = df.get("passer_player_id", "").fillna("").astype(str).ne("")
    has_receiver = df.get("receiver_player_id", "").fillna("").astype(str).ne("")
    has_rusher = df.get("rusher_player_id", "").fillna("").astype(str).ne("")

    base = pd.DataFrame(
        {
            "game_id": df.get("game_id", "").astype(str),
            "play_id": df.get("play_id", pd.Series([pd.NA] * len(df))).astype("Int64"),
            "game_date": df.get("game_date", "").fillna("").astype(str),
            "qtr": df.get("qtr", pd.Series([pd.NA] * len(df))).astype("Int64"),
            "time": df.get("time", "").fillna("").astype(str),
            "desc": df.get("desc", "").fillna("").astype(str),
        },
        index=df.index,
    )

    events: list[pd.DataFrame] = []

    def add(mask: pd.Series, team: pd.Series, player_id: pd.Series, pts: int, pos_hint: str | None, reason: str):
        if not mask.any():
            return
        out = base.loc[mask].copy()
        out["team"] = team.loc[mask].values
        out["player_id"] = player_id.loc[mask].values
        out["pts"] = pts
        out["pos_hint"] = pos_hint
        out["reason"] = reason
        events.append(out)

    # K events
    add(is_fg, posteam, df.get("kicker_player_id", pd.Series([""] * len(df), index=df.index)), rules.fg_made, "K", "FG made")
    add(is_xp, posteam, df.get("kicker_player_id", pd.Series([""] * len(df), index=df.index)), rules.xp_made, "K", "XP made")

    # Offensive TD
    add(is_td_off & pass_td_flag & has_passer, posteam, df["passer_player_id"], rules.passing_td, "QB", "Pass TD (QB)")
    add(is_td_off & pass_td_flag & has_receiver, posteam, df["receiver_player_id"], rules.receiving_td, None, "Pass TD (receiver)")
    add(is_td_off & rush_td_flag & has_rusher, posteam, df["rusher_player_id"], rules.rushing_td, None, "Rush TD")

    # Defensive TD / safety / defensive 2pt
    add(is_td_def, defteam, pd.Series([""] * len(df), index=df.index), rules.def_td, POS_OTH, "Defensive TD")
    add(is_safety, defteam, pd.Series([""] * len(df), index=df.index), rules.safety, POS_OTH, "Safety")
    add(is_def_two_pt, defteam, pd.Series([""] * len(df), index=df.index), rules.def_2pt_return, POS_OTH, "Defensive 2pt")

    # Offensive 2pt
    passish = (has_receiver | play_type.str.contains("pass"))
    add(is_2pt_off & passish & has_passer, posteam, df["passer_player_id"], rules.passing_2pt, "QB", "2pt pass (QB)")
    add(is_2pt_off & passish & has_receiver, posteam, df["receiver_player_id"], rules.receiving_2pt, None, "2pt pass (receiver)")
    add(is_2pt_off & (~passish) & (has_rusher | play_type.str.contains(r"run|rush")), posteam, df["rusher_player_id"], rules.rushing_2pt, None, "2pt rush")

    ev = pd.concat(events, ignore_index=True) if events else pd.DataFrame(columns=list(base.columns) + ["team", "player_id", "pts", "pos_hint", "reason"])
    if ev.empty:
        return pd.DataFrame(columns=["game_id", "play_id", "game_date", "qtr", "time", "team", "player_id", "position", "pts", "reason", "desc"])

    # Join roster positions where needed
    pos = rosters.copy()
    pos["player_id"] = pos["player_id"].astype(str)
    pos["position_bucket"] = pos["position_bucket"].astype(str).map(normalize_position)

    ev["player_id"] = ev["player_id"].fillna("").astype(str)
    ev = ev.merge(pos, on="player_id", how="left")

    ev["position"] = ev["pos_hint"].fillna(ev["position_bucket"]).map(normalize_position)

    keep = ["game_id", "play_id", "game_date", "qtr", "time", "team", "position", "pts", "reason", "desc"]
    return ev[keep].sort_values(["game_date", "qtr", "time", "play_id"], ascending=[False, False, False, False]).reset_index(drop=True)


def score_events(
    scoring_plays: pd.DataFrame,
    rosters: pd.DataFrame,
    *,
    season: int,
    week_max: int | None = None,
    game_ids: Iterable[str] | None = None,
    rules: ScoreRules = ScoreRules(),
) -> pd.DataFrame:
    return _build_events(scoring_plays, rosters, season=season, week_max=week_max, game_ids=game_ids, rules=rules)


def score_team_position_totals(
    scoring_plays: pd.DataFrame,
    rosters: pd.DataFrame,
    *,
    season: int,
    week_max: int | None = None,
    game_ids: Iterable[str] | None = None,
    rules: ScoreRules = ScoreRules(),
) -> pd.DataFrame:
    ev = _build_events(scoring_plays, rosters, season=season, week_max=week_max, game_ids=game_ids, rules=rules)
    if ev.empty:
        return pd.DataFrame(columns=["team", "position", "pts"])

    out = (
        ev.groupby(["team", "position"], as_index=False)["pts"]
        .sum()
        .sort_values(["team", "position"])
        .reset_index(drop=True)
    )

    # Stabilize grid
    teams = sorted([t for t in out["team"].dropna().astype(str).unique().tolist() if t])
    if not teams:
        return pd.DataFrame(columns=["team", "position", "pts"])

    idx = pd.MultiIndex.from_product([teams, POSITION_BUCKETS], names=["team", "position"])
    out = out.set_index(["team", "position"]).reindex(idx, fill_value=0).reset_index()
    return out.sort_values(["team", "position"]).reset_index(drop=True)
