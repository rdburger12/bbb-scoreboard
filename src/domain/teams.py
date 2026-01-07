from __future__ import annotations

from typing import Iterable

import pandas as pd

# Canonical BBB team abbreviations
_TEAM_ABBR_MAP: dict[str, str] = {
    "LA": "LAR",   # Rams
}

def canonicalize_team_abbr(team: str | None) -> str | None:
    """
    Canonicalize a single team abbreviation to BBB conventions.
    Returns None if input is None/empty.
    """
    if team is None:
        return None
    s = str(team).strip()
    if not s:
        return None
    s_up = s.upper()
    return _TEAM_ABBR_MAP.get(s_up, s_up)


def canonicalize_team_column(df: pd.DataFrame, col: str = "team") -> pd.DataFrame:
    """
    Returns a copy of df with df[col] canonicalized if col exists.
    """
    if col not in df.columns or df.empty:
        return df
    out = df.copy()
    out[col] = out[col].map(canonicalize_team_abbr)
    return out
