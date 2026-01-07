# src/scoring/io.py
from __future__ import annotations

from pathlib import Path
import pandas as pd

POSITIONS_CORE = {"QB", "RB", "WR", "TE"}
POS_K = "K"
POS_OTH = "OTH"


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


def clean_id(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )
