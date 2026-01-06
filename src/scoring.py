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
    df["player_id"] = df["player_id"].astype(str)
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
    def_2pt_return: int = 2
    def_td: int = 6


def _is_true(x) -> pd.Series:
    """
    Handles 1/0, True/False, strings, NaN.
    Always returns a boolean Series aligned to the caller's index when x is a Series.
    """
    if isinstance(x, pd.Series) and pd.api.types.is_bool_dtype(x):
        return x.fillna(False)
    if isinstance(x, pd.Series) and pd.api.types.is_numeric_dtype(x):
        return x.fillna(0).astype(int).eq(1)
    if not isinstance(x, pd.Series):
        x = pd.Series(x)
    return x.fillna("").astype(str).str.lower().isin({"1", "true", "t", "yes"})


def _clean_id(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )


# ---------- Core scoring ----------

def score_team_position_totals(
    scoring_plays: pd.DataFrame,
    rosters: pd.DataFrame,
    *,
    season: int,
    week_max: int | None = None,
    game_ids: Iterable[str] | None = None,
    rules: ScoreRules = ScoreRules(),
) -> pd.DataFrame:
    """
    BBB logic (matches Calculate BBB Points.R):
      - Offensive passing TD: QB +4, receiver +6   (is_td_off + pass_touchdown)
      - Offensive rushing TD: rusher +6            (is_td_off + rush_touchdown)
      - Defensive TD: defteam OTH +6               (is_td_def)
      - FG made: posteam K +3                      (is_fg)
      - XP made: posteam K +1                      (is_xp)
      - Offensive 2pt: passer +1, receiver +2 OR rusher +2   (is_2pt)
      - Defensive 2pt: defteam OTH +2              (is_def_two_pt or defensive_two_point_conv==1)
      - Safety: defteam OTH +2                     (is_safety)
    """
    df = scoring_plays.copy()

    # ---- Scope filters ----
    if "season" in df.columns:
        df = df[df["season"].astype("Int64") == int(season)]

    if game_ids is not None:
        gids = {str(g) for g in game_ids}
        if "game_id" not in df.columns:
            raise ValueError("scoring_plays missing 'game_id' for game_id filtering.")
        df = df[df["game_id"].astype(str).isin(gids)]
    elif week_max is not None:
        if "week" not in df.columns:
            raise ValueError("scoring_plays missing 'week' for week filtering.")
        df = df[df["week"].astype("Int64") <= int(week_max)]

    if df.empty:
        return pd.DataFrame(columns=["team", "position", "pts"])

    # ---- Required columns ----
    for c in ["posteam", "defteam"]:
        if c not in df.columns:
            raise ValueError(f"scoring_plays missing required column '{c}'.")

    posteam = df["posteam"].fillna("").astype(str)
    defteam = df["defteam"].fillna("").astype(str)

    # Normalize IDs so roster join works
    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in df.columns:
            df[c] = _clean_id(df[c])

    play_type = df.get("play_type", pd.Series("", index=df.index)).fillna("").astype(str).str.lower()

    # Canonical flags from scoring_plays.R
    is_fg = _is_true(df.get("is_fg", pd.Series(False, index=df.index)))
    is_xp = _is_true(df.get("is_xp", pd.Series(False, index=df.index)))
    is_safety = _is_true(df.get("is_safety", pd.Series(False, index=df.index)))

    # TD attribution flags (authoritative)
    is_td_off = _is_true(df.get("is_td_off", pd.Series(False, index=df.index)))
    is_td_def = _is_true(df.get("is_td_def", pd.Series(False, index=df.index)))
    pass_td_flag = _is_true(df.get("pass_touchdown", pd.Series(False, index=df.index)))
    rush_td_flag = _is_true(df.get("rush_touchdown", pd.Series(False, index=df.index)))

    # Defensive 2pt (authoritative)
    is_def_two_pt = _is_true(df.get("is_def_two_pt", pd.Series(False, index=df.index)))
    if "defensive_two_point_conv" in df.columns:
        is_def_two_pt = is_def_two_pt | (pd.to_numeric(df["defensive_two_point_conv"], errors="coerce").fillna(0).astype(int).eq(1))

    # Offensive 2pt success (authoritative in scoring_plays.R as is_2pt)
    is_2pt_off = _is_true(df.get("is_2pt", pd.Series(False, index=df.index))) & (~is_def_two_pt)

    has_passer = df.get("passer_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")
    has_receiver = df.get("receiver_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")
    has_rusher = df.get("rusher_player_id", pd.Series("", index=df.index)).fillna("").astype(str).ne("")

    # ---------- Build event rows ----------
    events: list[pd.DataFrame] = []

    # FG / XP -> K
    if is_fg.any():
        fg = pd.DataFrame(
            {
                "team": posteam[is_fg].values,
                "player_id": df.loc[is_fg, "kicker_player_id"].fillna("").astype(str).values,
                "pts": rules.fg_made,
                "pos_hint": POS_K,
            }
        )
        events.append(fg)

    if is_xp.any():
        xp = pd.DataFrame(
            {
                "team": posteam[is_xp].values,
                "player_id": df.loc[is_xp, "kicker_player_id"].fillna("").astype(str).values,
                "pts": rules.xp_made,
                "pos_hint": POS_K,
            }
        )
        events.append(xp)

    # Offensive passing TD -> QB +4 and receiver +6
    pass_td = is_td_off & pass_td_flag & has_passer
    if pass_td.any():
        qb = pd.DataFrame(
            {
                "team": posteam[pass_td].values,
                "player_id": df.loc[pass_td, "passer_player_id"].astype(str).values,
                "pts": rules.passing_td,
                "pos_hint": "QB",
            }
        )
        events.append(qb)

    pass_rec_td = is_td_off & pass_td_flag & has_receiver
    if pass_rec_td.any():
        rec = pd.DataFrame(
            {
                "team": posteam[pass_rec_td].values,
                "player_id": df.loc[pass_rec_td, "receiver_player_id"].astype(str).values,
                "pts": rules.receiving_td,
                "pos_hint": None,
            }
        )
        events.append(rec)

    # Offensive rushing TD -> rusher +6
    rush_td = is_td_off & rush_td_flag & has_rusher
    if rush_td.any():
        ru = pd.DataFrame(
            {
                "team": posteam[rush_td].values,
                "player_id": df.loc[rush_td, "rusher_player_id"].astype(str).values,
                "pts": rules.rushing_td,
                "pos_hint": None,
            }
        )
        events.append(ru)

    # Defensive TD -> defteam OTH +6
    if is_td_def.any():
        td_def = pd.DataFrame(
            {
                "team": defteam[is_td_def].values,
                "player_id": "",
                "pts": rules.def_td,
                "pos_hint": POS_OTH,
            }
        )
        events.append(td_def)

    # Safety -> defteam OTH +2  (this is the bug you almost certainly had)
    if is_safety.any():
        saf = pd.DataFrame(
            {
                "team": defteam[is_safety].values,
                "player_id": "",
                "pts": rules.safety,
                "pos_hint": POS_OTH,
            }
        )
        events.append(saf)

    # Defensive 2pt return -> defteam OTH +2
    if is_def_two_pt.any():
        d2 = pd.DataFrame(
            {
                "team": defteam[is_def_two_pt].values,
                "player_id": "",
                "pts": rules.def_2pt_return,
                "pos_hint": POS_OTH,
            }
        )
        events.append(d2)

    # Offensive 2pt success:
    # We cannot rely on df["pass"]/df["rush"] being filled, so infer:
    # - If receiver_player_id present OR play_type contains "pass" => passing 2pt
    # - Else if rusher_player_id present OR play_type contains "run/rush" => rushing 2pt
    pass_2pt = is_2pt_off & (has_receiver | play_type.str.contains("pass")) & has_passer
    if pass_2pt.any():
        p2 = pd.DataFrame(
            {
                "team": posteam[pass_2pt].values,
                "player_id": df.loc[pass_2pt, "passer_player_id"].astype(str).values,
                "pts": rules.passing_2pt,
                "pos_hint": "QB",
            }
        )
        events.append(p2)

    rec_2pt = is_2pt_off & (has_receiver | play_type.str.contains("pass")) & has_receiver
    if rec_2pt.any():
        r2 = pd.DataFrame(
            {
                "team": posteam[rec_2pt].values,
                "player_id": df.loc[rec_2pt, "receiver_player_id"].astype(str).values,
                "pts": rules.receiving_2pt,
                "pos_hint": None,
            }
        )
        events.append(r2)

    rush_2pt = is_2pt_off & (~(has_receiver | play_type.str.contains("pass"))) & (has_rusher | play_type.str.contains(r"run|rush"))
    if rush_2pt.any():
        ru2 = pd.DataFrame(
            {
                "team": posteam[rush_2pt].values,
                "player_id": df.loc[rush_2pt, "rusher_player_id"].astype(str).values,
                "pts": rules.rushing_2pt,
                "pos_hint": None,
            }
        )
        events.append(ru2)

    # ---- Combine events ----
    ev = pd.concat(events, ignore_index=True) if events else pd.DataFrame(columns=["team", "player_id", "pts", "pos_hint"])
    if ev.empty:
        return pd.DataFrame(columns=["team", "position", "pts"])

    ev["team"] = ev["team"].fillna("").astype(str)
    ev["player_id"] = ev["player_id"].fillna("").astype(str)

    # ---- Join positions for receiver/rusher events ----
    pos = rosters.copy()
    if not {"player_id", "position_bucket"}.issubset(pos.columns):
        raise ValueError("positions must have columns: player_id, position_bucket")

    pos["player_id"] = pos["player_id"].astype(str)
    pos["position_bucket"] = pos["position_bucket"].astype(str).map(normalize_position)

    ev = ev.merge(pos, on="player_id", how="left")

    # Final position:
    ev["position_final"] = ev["pos_hint"].fillna(ev["position_bucket"])
    ev["position_final"] = ev["position_final"].map(normalize_position)

    out = (
        ev.groupby(["team", "position_final"], as_index=False)["pts"]
        .sum()
        .rename(columns={"position_final": "position"})
    )

    # Stabilize output grid (every team x bucket)
    teams = sorted([t for t in out["team"].dropna().astype(str).unique().tolist() if t])
    if not teams:
        return pd.DataFrame(columns=["team", "position", "pts"])

    idx = pd.MultiIndex.from_product([teams, POSITION_BUCKETS], names=["team", "position"])
    out = out.set_index(["team", "position"]).reindex(idx, fill_value=0).reset_index()

    return out.sort_values(["team", "position"]).reset_index(drop=True)
