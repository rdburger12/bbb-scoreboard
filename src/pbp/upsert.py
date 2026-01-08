from __future__ import annotations

import pandas as pd


def upsert_latest_wins(old: pd.DataFrame | None, new_scoring: pd.DataFrame) -> pd.DataFrame:
    """
    Python port of r/lib/upsert.R::upsert_latest_wins(). :contentReference[oaicite:7]{index=7}
    - Align schemas (union of columns)
    - Bind rows
    - Sort by (game_id, play_id, refreshed_at)
    - Keep the last row per (game_id, play_id)
    """
    old_exists = old is not None and not old.empty
    if not old_exists:
        return new_scoring.copy()

    old = old.copy()
    new_scoring = new_scoring.copy()

    # Align schemas
    for c in set(new_scoring.columns) - set(old.columns):
        old[c] = pd.NA
    for c in set(old.columns) - set(new_scoring.columns):
        new_scoring[c] = pd.NA

    # Match ordering
    old = old[new_scoring.columns]
    new_scoring = new_scoring[old.columns]

    combined = pd.concat([old, new_scoring], ignore_index=True)

    # Deterministic "latest wins"
    combined = combined.sort_values(["game_id", "play_id", "refreshed_at"], kind="mergesort")
    combined = combined.drop_duplicates(subset=["game_id", "play_id"], keep="last")

    return combined.reset_index(drop=True)
