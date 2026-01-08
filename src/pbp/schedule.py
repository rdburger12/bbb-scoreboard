from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd
import nflreadpy as nfl


def _to_pandas(df) -> pd.DataFrame:
    """
    nflreadpy returns Polars DataFrames; convert safely to pandas.
    If it already is pandas, return as-is.
    """
    if df is None:
        return pd.DataFrame()
    if isinstance(df, pd.DataFrame):
        return df
    to_pandas = getattr(df, "to_pandas", None)
    if callable(to_pandas):
        return to_pandas()
    return pd.DataFrame(df)


def load_schedules(season: int) -> pd.DataFrame:
    """
    Load schedules for a given season via nflreadpy (nflverse repos).
    """
    sched_pl = nfl.load_schedules(seasons=season)
    sched = _to_pandas(sched_pl)

    if sched.empty:
        return sched

    # Normalize core columns we rely on
    if "game_id" in sched.columns:
        sched["game_id"] = sched["game_id"].astype("string")
    if "week" in sched.columns:
        sched["week"] = pd.to_numeric(sched["week"], errors="coerce").astype("Int64")
    if "season" in sched.columns:
        sched["season"] = pd.to_numeric(sched["season"], errors="coerce").astype("Int64")
    return sched


def resolve_game_ids_for_week(season: int, week: int) -> List[str]:
    """
    Return nflfastR-style game_id strings for a given season+week.
    """
    sched = load_schedules(season)
    if sched.empty:
        return []

    if "week" not in sched.columns or "game_id" not in sched.columns:
        return []

    sub = sched.loc[sched["week"] == int(week)].copy()
    if sub.empty:
        return []

    return sorted({str(x) for x in sub["game_id"].dropna().astype(str).tolist()})


def game_id_to_event_id_map(season: int) -> Dict[str, str]:
    """
    Build mapping from nflfastR-style game_id (e.g., 2023_19_CLE_HOU)
    to the 10-digit NFL GameCenter event id used by the GTD endpoint.

    In your schedule rows, this value is in 'old_game_id' (e.g., 2024011300).
    """
    sched = load_schedules(season)
    if sched.empty:
        return {}

    if "game_id" not in sched.columns or "old_game_id" not in sched.columns:
        return {}

    out: Dict[str, str] = {}
    for _, r in sched[["game_id", "old_game_id"]].dropna().iterrows():
        gid = str(r["game_id"]).strip()
        oid = r["old_game_id"]
        try:
            event_id = str(int(oid))
        except Exception:
            event_id = str(oid).strip()

        if gid and event_id:
            out[gid] = event_id
    return out


def event_id_for_game_id(season: int, game_id: str) -> Optional[str]:
    """
    Convenience wrapper: return 10-digit event id for a single nflfastR game_id.
    """
    m = game_id_to_event_id_map(season)
    return m.get(str(game_id).strip())


# Backwards-compatible aliases (so Step 2 file can call gsis_for_game_id)
# We now treat "gsis" as "event id" in this app context.
def game_id_to_gsis_map(season: int) -> Dict[str, str]:
    return game_id_to_event_id_map(season)


def gsis_for_game_id(season: int, game_id: str) -> Optional[str]:
    return event_id_for_game_id(season, game_id)
