# src/scoreboard.py
from __future__ import annotations

from typing import Iterable, Optional
from src.domain.teams import canonicalize_team_column

import pandas as pd


POSITIONS = ["QB", "RB", "WR", "TE", "K", "OTH"]


class ScoreboardBuildError(ValueError):
    """Raised when inputs violate the scoreboard dataset contract."""


def _require_columns(df: pd.DataFrame, required: Iterable[str], df_name: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ScoreboardBuildError(f"{df_name} missing required columns: {missing}")


def _assert_no_nulls(df: pd.DataFrame, cols: Iterable[str], df_name: str) -> None:
    bad = [c for c in cols if df[c].isna().any()]
    if bad:
        raise ScoreboardBuildError(f"{df_name} has nulls in required columns: {bad}")


def _assert_unique_key(df: pd.DataFrame, key_cols: list[str], df_name: str) -> None:
    dup = df.duplicated(subset=key_cols, keep=False)
    if dup.any():
        sample = df.loc[dup, key_cols].head(10).to_dict("records")
        raise ScoreboardBuildError(
            f"{df_name} has duplicate keys on {key_cols}. Sample duplicates: {sample}"
        )


def _coerce_pts_numeric(totals_df: pd.DataFrame) -> pd.DataFrame:
    out = totals_df.copy()
    out["pts"] = pd.to_numeric(out["pts"], errors="coerce")
    if out["pts"].isna().any():
        bad = out.loc[out["pts"].isna(), ["team", "position"]].head(10).to_dict("records")
        raise ScoreboardBuildError(f"totals_df has non-numeric pts. Sample bad rows: {bad}")
    return out


def _validate_positions(df: pd.DataFrame, col: str, df_name: str) -> None:
    unknown = sorted(set(df[col].unique()) - set(POSITIONS))
    if unknown:
        raise ScoreboardBuildError(f"{df_name} has unknown positions in '{col}': {unknown}")


def _optional_validate_owner_roster_shape(draft_df: pd.DataFrame) -> None:
    """
    Optional guardrail: each owner should have exactly one pick per position.
    This is a league rule, so it can catch malformed draft files early.
    """
    counts = (
        draft_df.groupby(["owner_id", "position"], as_index=False)
        .size()
        .rename(columns={"size": "n"})
    )
    bad = counts[counts["n"] != 1]
    if not bad.empty:
        sample = bad.head(20).to_dict("records")
        raise ScoreboardBuildError(
            "Draft picks violate 'one of each position per owner'. "
            f"Sample offending (owner_id, position, n): {sample}"
        )


def build_scoreboard_dataset(
    draft_df: pd.DataFrame,
    totals: pd.DataFrame,
    *,
    season: int,
    validate: bool = True,
) -> pd.DataFrame:
    # draft_df: owner_id, owner, round, slot, team, position, season, ...
    # totals: team, position, pts (may be empty)

    if draft_df.empty:
        return pd.DataFrame(columns=[
            "owner_id", "owner", "round", "slot", "team", "position", "pts", "unit"
        ])

    d = canonicalize_team_column(draft_df.copy(), "team")

    # Make a safe totals frame even if empty/missing cols
    if totals is None or totals.empty:
        t = pd.DataFrame(columns=["team", "position", "pts"])
    else:
        t = canonicalize_team_column(totals.copy(), "team")

        # If totals is non-empty but missing expected columns, degrade gracefully when validate=False
        if not validate:
            for col in ["team", "position", "pts"]:
                if col not in t.columns:
                    t[col] = pd.Series(dtype="object")

        # Standardize expected col names if needed
        if validate:
            t_missing = {"team", "position", "pts"} - set(t.columns)
            if t_missing:
                raise ValueError(f"totals missing required columns: {sorted(t_missing)}")

    # Left join so every draft pick remains
    out = d.merge(t[["team", "position", "pts"]], on=["team", "position"], how="left")

    # Default missing points to 0
    out["pts"] = pd.to_numeric(out["pts"], errors="coerce").fillna(0)

    # Derived display-only unit
    out["unit"] = out["team"].astype(str) + " " + out["position"].astype(str)

    # Slot-based sorting
    out = out.sort_values(["owner_id", "round", "slot"], kind="stable").reset_index(drop=True)

    return out[["owner_id", "owner", "round", "slot", "team", "position", "pts", "unit"]]

