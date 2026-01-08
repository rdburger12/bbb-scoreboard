from __future__ import annotations

from pathlib import Path

import pandas as pd
import nflreadpy as nfl


def _to_pandas(df) -> pd.DataFrame:
    """
    nflreadpy returns Polars DataFrames; convert safely to pandas.
    """
    if df is None:
        return pd.DataFrame()
    if isinstance(df, pd.DataFrame):
        return df
    to_pandas = getattr(df, "to_pandas", None)
    if callable(to_pandas):
        return to_pandas()
    return pd.DataFrame(df)


def ensure_player_positions(season: int, pos_path: str | Path | None) -> None:
    """
    Replacement for the old nfl_data_py-based implementation.

    Writes player_positions_{season}.csv if missing.

    IMPORTANT: uses GSIS id as player_id to match your existing environment
    (mirrors your R logic using gsis_id). :contentReference[oaicite:0]{index=0}
    """
    if not pos_path:
        return

    pos_path = Path(pos_path)
    if pos_path.exists():
        return

    ro_raw_pl = nfl.load_rosters(seasons=season)
    ro_raw = _to_pandas(ro_raw_pl)
    if ro_raw.empty:
        raise RuntimeError(f"load_rosters(seasons={season}) returned no rows")

    # Your R code expects gsis_id. :contentReference[oaicite:1]{index=1}
    if "gsis_id" not in ro_raw.columns:
        raise RuntimeError(
            "load_rosters() did not return gsis_id. Columns found: "
            + ", ".join(map(str, ro_raw.columns))
        )

    ro = pd.DataFrame(
        {
            "player_id": ro_raw["gsis_id"].astype("string"),
            "position": ro_raw.get("position").astype("string"),
        }
    )

    ro["position"] = ro["position"].str.strip().str.upper()
    ro = ro.loc[ro["player_id"].notna() & (ro["player_id"] != "")].copy()

    # Match your R behavior: FB -> RB
    ro["position"] = ro["position"].str.replace(r"^FB$", "RB", regex=True)

    def bucket(pos: str) -> str:
        if pos in {"QB", "RB", "WR", "TE", "K"}:
            return pos
        return "OTH"

    ro["position_bucket"] = ro["position"].fillna("").map(bucket).astype("string")
    ro = ro[["player_id", "position_bucket"]].drop_duplicates()

    pos_path.parent.mkdir(parents=True, exist_ok=True)
    ro.to_csv(pos_path, index=False)
