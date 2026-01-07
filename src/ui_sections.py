from __future__ import annotations

from pathlib import Path
from typing import Iterable

import pandas as pd
import streamlit as st

from src.app_io import read_csv_safe


def _read_with_warning(path: Path, empty_info: str) -> pd.DataFrame:
    """
    Read a CSV via read_csv_safe and surface parse errors as Streamlit warnings.
    Returns an empty DF on read error.
    """
    df = read_csv_safe(path)
    if "__read_error__" in df.columns:
        st.warning(df.loc[0, "__read_error__"])
        return pd.DataFrame()
    if df.empty and empty_info:
        st.info(empty_info)
    return df


def section_refresh_status(status_path: Path) -> pd.DataFrame:
    st.subheader("Refresh status (latest)")
    df = _read_with_warning(status_path, "No refresh_status.csv yet. Click Refresh.")
    if not df.empty and "refreshed_at" in df.columns and len(df) == 1:
        st.caption(f"Last refresh: {df.loc[0, 'refreshed_at']}")
        st.dataframe(df, use_container_width=True)
    elif not df.empty:
        st.dataframe(df, use_container_width=True)
    return df


def section_refresh_log(log_path: Path, n: int = 20) -> pd.DataFrame:
    st.subheader(f"Refresh log (last {n} attempts)")
    df = _read_with_warning(log_path, "No refresh_log.csv yet. Click Refresh.")
    if not df.empty:
        st.dataframe(df.tail(n), use_container_width=True)
    return df


def section_latest_scoring_plays(latest_path: Path, n: int = 50) -> pd.DataFrame:
    st.subheader("Latest refresh scoring plays")
    df = _read_with_warning(latest_path, "No latest file yet, or latest refresh returned 0 scoring plays.")
    if not df.empty:
        st.write(f"Rows: {len(df)}")
        st.dataframe(df.head(n), use_container_width=True)
    return df


def section_cumulative_scoring_plays(scoring_path: Path, df_scoring: pd.DataFrame | None = None, n: int = 50) -> pd.DataFrame:
    """
    If df_scoring is provided, it will be displayed (no file read).
    Otherwise reads from scoring_path.

    Returns the DF that was displayed.
    """
    st.subheader("Cumulative scoring plays (upserted)")

    if df_scoring is None:
        df_scoring = _read_with_warning(scoring_path, "No cumulative scoring file yet.")

    if df_scoring is None or df_scoring.empty:
        return pd.DataFrame()

    if {"game_id", "play_id"}.issubset(df_scoring.columns):
        unique_keys = df_scoring[["game_id", "play_id"]].drop_duplicates().shape[0]
        st.write(f"Rows: {len(df_scoring)} | Unique keys: {unique_keys}")
    else:
        st.write(f"Rows: {len(df_scoring)}")

    st.dataframe(df_scoring.tail(n), use_container_width=True)
    return df_scoring


def section_totals_table(totals: pd.DataFrame) -> None:
    st.subheader("Cumulative Fantasy Totals (Team × Position)")
    if totals.empty:
        st.info("No totals in the selected scope.")
    else:
        st.dataframe(totals, use_container_width=True)


def section_event_feed(events: pd.DataFrame, *, team_filter: bool = True) -> None:
    st.subheader("Scoring Plays (event feed)")

    if events.empty:
        st.info("No scoring events in scope.")
        return

    view = events
    if team_filter and "team" in events.columns:
        teams = ["(All)"] + sorted([t for t in events["team"].unique().tolist() if t])
        sel_team = st.selectbox("Filter team", teams, index=0)
        view = events if sel_team == "(All)" else events[events["team"] == sel_team]

    st.dataframe(view, use_container_width=True, height=520)


def section_totals_tieout(totals: pd.DataFrame, events: pd.DataFrame) -> None:
    with st.expander("Diagnostics: totals tie-out", expanded=False):
        if totals.empty or events.empty:
            st.info("Need both totals and events to run tie-out.")
            return

        ev_totals = (
            events.groupby(["team", "position"], as_index=False)["pts"]
            .sum()
            .sort_values(["team", "position"])
            .reset_index(drop=True)
        )

        t = totals.rename(columns={"pts": "pts_totals"}).merge(
            ev_totals.rename(columns={"pts": "pts_events"}),
            on=["team", "position"],
            how="outer",
        ).fillna(0)

        t["diff"] = t["pts_totals"] - t["pts_events"]
        bad = t[t["diff"] != 0]

        if bad.empty:
            st.success("Totals match event aggregation.")
        else:
            st.error("Mismatch between totals and event aggregation (should never happen).")
            st.dataframe(bad, use_container_width=True)


def section_playoff_scoping_diag(
    *,
    playoff_games_path: Path,
    playoff_game_ids: set[str],
    df_scoring: pd.DataFrame,
) -> None:
    with st.expander("Diagnostics: playoff scoping", expanded=False):
        st.write("playoff_game_ids file:", playoff_games_path)
        st.write("playoff_game_ids count:", len(playoff_game_ids))
        if not df_scoring.empty and "game_id" in df_scoring.columns:
            st.write("games in scoring_plays.csv:", int(df_scoring["game_id"].nunique()))
