from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

# ---------- Position normalization / buckets ----------

POSITIONS_CORE = {"QB", "RB", "WR", "TE"}
POS_K = "K"
POS_OTH = "OTH"
POSITION_BUCKETS = ["QB", "RB", "WR", "TE", "K", "OTH"]


def normalize_position(pos: str | None) -> str:
    if not pos:
        return POS_OTH
    p = str(pos).upper().strip()
    if p == "FB":
        p = "RB"
    if p in POSITIONS_CORE:
        return p
    if p == "K":
        return POS_K
    return POS_OTH


# ---------- IO helpers ----------

def load_scoring_plays_csv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path)

    if "game_id" in df.columns:
        df["game_id"] = df["game_id"].astype(str)
    if "posteam" in df.columns:
        df["posteam"] = df["posteam"].fillna("").astype(str)
    if "defteam" in df.columns:
        df["defteam"] = df["defteam"].fillna("").astype(str)
    if "return_team" in df.columns:
        df["return_team"] = df["return_team"].fillna("").astype(str)
    if "season" in df.columns:
        df["season"] = pd.to_numeric(df["season"], errors="coerce")
    if "week" in df.columns:
        df["week"] = pd.to_numeric(df["week"], errors="coerce")

    # Normalize player id columns defensively (prevents '.0' issues from csv roundtrips)
    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in df.columns:
            df[c] = (
                df[c]
                .fillna("")
                .astype(str)
                .str.strip()
                .str.replace(r"\.0$", "", regex=True)
                .replace({"nan": "", "None": ""})
            )

    return df


def load_player_positions(cache_path: Path) -> pd.DataFrame:
    """
    Local mapping produced by R: player_id -> position_bucket.
    Required columns: player_id, position_bucket
    """
    if not cache_path.exists():
        raise FileNotFoundError(
            f"Missing {cache_path}. Run an R refresh for that season to generate it."
        )

    df = pd.read_csv(cache_path)
    if not {"player_id", "position_bucket"}.issubset(df.columns):
        raise ValueError(f"{cache_path} must contain columns: player_id, position_bucket")

    df["player_id"] = (
        df["player_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )
    df["position_bucket"] = df["position_bucket"].astype(str).map(normalize_position)

    return df[["player_id", "position_bucket"]]


# ---------- Scoring rules ----------

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
    def_2pt_return: int = 2  # defense returns a 2pt try for score
    other_td: int = 6        # returns/defense/other non-pass non-rush TD


def _is_true(series: pd.Series) -> pd.Series:
    """Normalize various 1/0, True/False, strings to boolean series."""
    if pd.api.types.is_bool_dtype(series):
        return series.fillna(False)
    if pd.api.types.is_numeric_dtype(series):
        return series.fillna(0).astype(int).eq(1)
    return series.fillna("").astype(str).str.lower().isin({"1", "true", "t", "yes"})


# ---------- Core scoring ----------

def score_team_position_totals(
    scoring_plays: pd.DataFrame,
    rosters: pd.DataFrame,
    *,
    season: int,
    week_max: int | None = None,
    game_ids: Iterable[str] | None = None,
    rules: ScoreRules = ScoreRules(),
    strict_position_join: bool = False,  # set True if you want hard failures
) -> pd.DataFrame:
    """
    Returns: team, position, pts (cumulative within scope)
    """
    df = scoring_plays.copy()

    # ---- Scope filters ----
    if "season" in df.columns:
        df = df[df["season"].astype("Int64") == int(season)]

    if game_ids is not None:
        if "game_id" not in df.columns:
            raise ValueError("scoring_plays missing 'game_id' required for game_ids filtering.")
        gids = {str(g) for g in game_ids}
        df = df[df["game_id"].astype(str).isin(gids)]
    elif week_max is not None:
        if "week" not in df.columns:
            raise ValueError("scoring_plays missing 'week' required for week_max filtering.")
        df = df[df["week"].astype("Int64") <= int(week_max)]

    if df.empty:
        return pd.DataFrame(columns=["team", "position", "pts"])

    # ---- Required columns ----
    if "posteam" not in df.columns:
        raise ValueError("scoring_plays missing required column 'posteam'.")
    if "defteam" not in df.columns:
        # You can relax this if needed, but your data has it.
        raise ValueError("scoring_plays missing required column 'defteam'.")

    posteam = df["posteam"].fillna("").astype(str)
    defteam = df["defteam"].fillna("").astype(str)
    return_team = df.get("return_team", pd.Series("", index=df.index)).fillna("").astype(str)

    # ---- Flags ----
    is_td = _is_true(df.get("is_td", pd.Series(False, index=df.index)))
    is_fg = _is_true(df.get("is_fg", pd.Series(False, index=df.index)))
    is_xp = _is_true(df.get("is_xp", pd.Series(False, index=df.index)))
    is_2pt = _is_true(df.get("is_2pt", pd.Series(False, index=df.index)))
    is_safety = _is_true(df.get("is_safety", pd.Series(False, index=df.index)))

    is_int = _is_true(df.get("interception", pd.Series(False, index=df.index)))
    is_fumble_lost = _is_true(df.get("fumble_lost", pd.Series(False, index=df.index)))

    # Defense-scored variants
    turnover_return_td = is_td & (is_int | is_fumble_lost)
    is_def_2pt_return = _is_true(df.get("is_def_2pt_return", pd.Series(False, index=df.index)))
    def_2pt_return = is_def_2pt_return | (is_2pt & is_td)
    is_pat_def_2pt = _is_true(df.get("is_pat_def_2pt", pd.Series(False, index=df.index)))


    # ---- Helper columns for inference ----
    has_passer = df.get("passer_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")
    has_receiver = df.get("receiver_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")
    has_rusher = df.get("rusher_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")

    pass_flag = _is_true(df.get("pass", pd.Series(False, index=df.index)))
    rush_flag = _is_true(df.get("rush", pd.Series(False, index=df.index)))

    play_type = df.get("play_type", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()

    # ---- TD classification (exclude defense-scored and def-2pt-return cases) ----
    offense_td_block = is_td & (~turnover_return_td) & (~def_2pt_return)

    pass_td = offense_td_block & (pass_flag | play_type.str.contains("pass")) & has_passer
    rec_td = pass_td & has_receiver
    rush_td = offense_td_block & (~pass_td) & (rush_flag | play_type.str.contains(r"run|rush")) & has_rusher
    other_td = offense_td_block & (~pass_td) & (~rush_td)

    # ---- 2pt classification (offense only; def_2pt_return handled separately) ----
    pass_2pt = is_2pt & (~def_2pt_return) & (pass_flag | play_type.str.contains("pass")) & has_passer
    rec_2pt = pass_2pt & has_receiver
    rush_2pt = is_2pt & (~def_2pt_return) & (~pass_2pt) & (rush_flag | play_type.str.contains(r"run|rush")) & has_rusher

    events: list[pd.DataFrame] = []

    # Passing TD: QB gets 4; receiver gets 6
    if pass_td.any():
        qb = df.loc[pass_td, ["passer_player_id"]].copy()
        qb["team"] = posteam[pass_td].values
        qb["player_id"] = qb["passer_player_id"].astype(str)
        qb["pts"] = rules.passing_td
        qb["pos_hint"] = "QB"
        events.append(qb[["team", "player_id", "pts", "pos_hint"]])

    if rec_td.any():
        rec = df.loc[rec_td, ["receiver_player_id"]].copy()
        rec["team"] = posteam[rec_td].values
        rec["player_id"] = rec["receiver_player_id"].astype(str)
        rec["pts"] = rules.receiving_td
        rec["pos_hint"] = None
        events.append(rec[["team", "player_id", "pts", "pos_hint"]])

    # Rushing TD: rusher gets 6
    if rush_td.any():
        ru = df.loc[rush_td, ["rusher_player_id"]].copy()
        ru["team"] = posteam[rush_td].values
        ru["player_id"] = ru["rusher_player_id"].astype(str)
        ru["pts"] = rules.rushing_td
        ru["pos_hint"] = None
        events.append(ru[["team", "player_id", "pts", "pos_hint"]])

    # Other offensive TD: attribute to posteam OTH
    if other_td.any():
        events.append(pd.DataFrame(
            {
                "team": posteam[other_td].values,
                "player_id": "",
                "pts": rules.other_td,
                "pos_hint": POS_OTH,
            }
        ))

    # Turnover return TD: credit defense OTH (defteam)
    if turnover_return_td.any():
        events.append(pd.DataFrame(
            {
                "team": defteam[turnover_return_td].values,
                "player_id": "",
                "pts": rules.other_td,
                "pos_hint": POS_OTH,
            }
        ))

    # FG / XP: kicker gets points (posteam)
    if is_fg.any():
        fg = df.loc[is_fg, ["kicker_player_id"]].copy()
        fg["team"] = posteam[is_fg].values
        fg["player_id"] = fg["kicker_player_id"].fillna("").astype(str)
        fg["pts"] = rules.fg_made
        fg["pos_hint"] = POS_K
        events.append(fg[["team", "player_id", "pts", "pos_hint"]])

    if is_xp.any():
        xp = df.loc[is_xp, ["kicker_player_id"]].copy()
        xp["team"] = posteam[is_xp].values
        xp["player_id"] = xp["kicker_player_id"].fillna("").astype(str)
        xp["pts"] = rules.xp_made
        xp["pos_hint"] = POS_K
        events.append(xp[["team", "player_id", "pts", "pos_hint"]])

    # 2PT (offense): passer + receiver OR rusher
    if pass_2pt.any():
        p2 = df.loc[pass_2pt, ["passer_player_id"]].copy()
        p2["team"] = posteam[pass_2pt].values
        p2["player_id"] = p2["passer_player_id"].astype(str)
        p2["pts"] = rules.passing_2pt
        p2["pos_hint"] = "QB"
        events.append(p2[["team", "player_id", "pts", "pos_hint"]])

    if rec_2pt.any():
        r2 = df.loc[rec_2pt, ["receiver_player_id"]].copy()
        r2["team"] = posteam[rec_2pt].values
        r2["player_id"] = r2["receiver_player_id"].astype(str)
        r2["pts"] = rules.receiving_2pt
        r2["pos_hint"] = None
        events.append(r2[["team", "player_id", "pts", "pos_hint"]])

    if rush_2pt.any():
        ru2 = df.loc[rush_2pt, ["rusher_player_id"]].copy()
        ru2["team"] = posteam[rush_2pt].values
        ru2["player_id"] = ru2["rusher_player_id"].astype(str)
        ru2["pts"] = rules.rushing_2pt
        ru2["pos_hint"] = None
        events.append(ru2[["team", "player_id", "pts", "pos_hint"]])

    # Defensive 2pt return: credit defense OTH +2
    if def_2pt_return.any():
        scored_by = return_team.where(return_team.ne(""), defteam)
        events.append(pd.DataFrame(
            {
                "team": scored_by[def_2pt_return].values,
                "player_id": "",
                "pts": rules.def_2pt_return,
                "pos_hint": POS_OTH,
            }
        ))

    # Safety: credit defense OTH +2 (defteam)
    if is_safety.any():
        events.append(pd.DataFrame(
            {
                "team": defteam[is_safety].values,
                "player_id": "",
                "pts": rules.safety,
                "pos_hint": POS_OTH,
            }
        ))

    if is_pat_def_2pt.any():
        # blocked XP returned for 2 points goes to defense
        events.append(pd.DataFrame(
            {
                "team": defteam[is_pat_def_2pt].values,
                "player_id": "",
                "pts": rules.def_2pt_return,  # 2
                "pos_hint": POS_OTH,
            }
        ))


    ev = pd.concat(events, ignore_index=True) if events else pd.DataFrame(
        columns=["team", "player_id", "pts", "pos_hint"]
    )
    ev["team"] = ev["team"].fillna("").astype(str)
    ev["player_id"] = ev["player_id"].fillna("").astype(str)

    # ---- Join roster positions ----
    pos = rosters.copy()
    if not {"player_id", "position_bucket"}.issubset(pos.columns):
        raise ValueError("positions must have columns: player_id, position_bucket")

    pos["player_id"] = (
        pos["player_id"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )
    pos["position_bucket"] = pos["position_bucket"].astype(str).map(normalize_position)

    ev = ev.merge(pos, on="player_id", how="left")

    # Optional diagnostics / guard
    needs_join = ev["pos_hint"].isna() & ev["player_id"].ne("")  # receiver/rusher events
    miss = needs_join & ev["position_bucket"].isna()
    if miss.any():
        sample = ev.loc[miss, ["player_id", "team"]].head(10)
        msg = (
            "Position join failed for some player_ids (receiver/rusher likely). "
            f"Missing count={int(miss.sum())}. Sample:\n{sample.to_string(index=False)}"
        )
        if strict_position_join:
            raise ValueError(msg)
        # else: leave them as OTH via normalize_position(None)

    ev["position_final"] = ev["pos_hint"].fillna(ev["position_bucket"])
    ev["position_final"] = ev["position_final"].map(normalize_position)

    out = (
        ev.groupby(["team", "position_final"], as_index=False)["pts"]
        .sum()
        .rename(columns={"position_final": "position"})
    )

    # Stabilize output for UI: ensure every team has every bucket
    teams = sorted([t for t in out["team"].dropna().astype(str).unique().tolist() if t])
    if not teams:
        return pd.DataFrame(columns=["team", "position", "pts"])

    idx = pd.MultiIndex.from_product([teams, POSITION_BUCKETS], names=["team", "position"])
    out = out.set_index(["team", "position"]).reindex(idx, fill_value=0).reset_index()

    return out.sort_values(["team", "position"]).reset_index(drop=True)
