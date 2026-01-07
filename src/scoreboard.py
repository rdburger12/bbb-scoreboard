# src/scoreboard.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

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
    totals_df: pd.DataFrame,
    *,
    season: Optional[int] = None,
    validate: bool = True,
    validate_owner_roster_shape: bool = True,
) -> pd.DataFrame:
    """
    Build the canonical scoreboard dataset by joining draft picks (immutable inventory)
    to cumulative scoring totals by (team, position).

    Returns columns:
        owner_id, owner, round, slot, team, position, pts, unit
    """
    draft_required = ["owner_id", "owner", "round", "slot", "team", "position"]
    totals_required = ["team", "position", "pts"]

    _require_columns(draft_df, draft_required, "draft_df")
    _require_columns(totals_df, totals_required, "totals_df")

    d = draft_df.copy()
    t = totals_df.copy()

    # Optional season filter if season column exists
    if season is not None and "season" in d.columns:
        d = d[d["season"] == season].copy()

    # Normalize dtypes for consistent merges/sorts
    # (owner_id and slot are often numeric but can be read as strings from CSV)
    d["owner_id"] = pd.to_numeric(d["owner_id"], errors="raise")
    d["round"] = pd.to_numeric(d["round"], errors="raise")
    d["slot"] = pd.to_numeric(d["slot"], errors="raise")

    # Basic trimming to avoid merge mismatches from whitespace
    d["team"] = d["team"].astype(str).str.strip()
    d["position"] = d["position"].astype(str).str.strip().str.upper()
    t["team"] = t["team"].astype(str).str.strip()
    t["position"] = t["position"].astype(str).str.strip().str.upper()

    if validate:
        _assert_no_nulls(d, ["owner_id", "owner", "round", "slot", "team", "position"], "draft_df")
        _assert_no_nulls(t, ["team", "position", "pts"], "totals_df")
        _validate_positions(d, "position", "draft_df")
        _validate_positions(t, "position", "totals_df")
        _assert_unique_key(d, ["owner_id", "position"], "draft_df (owner roster key)") if validate_owner_roster_shape else None
        _assert_unique_key(t, ["team", "position"], "totals_df (unit key)")

        if validate_owner_roster_shape:
            _optional_validate_owner_roster_shape(d)

    t = _coerce_pts_numeric(t)

    merged = d.merge(t, how="left", on=["team", "position"])

    # Missing totals -> 0
    merged["pts"] = merged["pts"].fillna(0.0)

    # Display-only
    merged["unit"] = merged["team"].astype(str) + " " + merged["position"].astype(str)

    # Canonical column order
    merged = merged[
        ["owner_id", "owner", "round", "slot", "team", "position", "pts", "unit"]
    ].copy()

    # Sorting: owners displayed by draft slot (owner_id); stable within owner by round then slot
    merged = merged.sort_values(by=["owner_id", "round", "slot", "position"], kind="mergesort").reset_index(drop=True)

    return merged
