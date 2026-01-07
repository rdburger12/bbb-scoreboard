from __future__ import annotations

from pathlib import Path
import pandas as pd
import streamlit as st


@st.cache_data(show_spinner=False)
def read_csv_safe(path: Path) -> pd.DataFrame:
    """
    Read a CSV safely for Streamlit (cached). Returns an empty DF if missing/empty,
    or a DF with __read_error__ column if parsing fails.
    """
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, keep_default_na=True)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        # cannot call st.warning inside cached function reliably; return sentinel
        return pd.DataFrame({"__read_error__": [f"Could not read {path.name}: {e}"]})


@st.cache_data(show_spinner=False)
def load_playoff_game_ids(path: Path) -> set[str]:
    """
    Load playoff game_ids from config CSV (cached).
    Expected schema: game_id
    """
    if not path.exists():
        return set()

    df = pd.read_csv(path)
    if "game_id" not in df.columns:
        raise ValueError(f"{path} must have a 'game_id' column")

    gids = (
        df["game_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .unique()
        .tolist()
    )
    return set(gids)


def _clean_player_id(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )


def normalize_scoring_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize IDs once for stable merges/filtering.
    - game_id -> str
    - play_id -> Int64
    - player_id cols -> clean strings
    """
    if df.empty:
        return df

    out = df.copy()

    if "game_id" in out.columns:
        out["game_id"] = out["game_id"].astype(str)

    if "play_id" in out.columns:
        out["play_id"] = pd.to_numeric(out["play_id"], errors="coerce").astype("Int64")

    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in out.columns:
            out[c] = _clean_player_id(out[c])

    return out
