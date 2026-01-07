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


def section_scoreboard_table(scoreboard: pd.DataFrame) -> None:
    st.subheader("Scoreboard (Owners × Drafted Units)")
    if scoreboard.empty:
        st.info("Scoreboard dataset not available (missing draft picks or no totals in scope).")
    else:
        st.dataframe(scoreboard, use_container_width=True)

def section_scoreboard_round_grid(scoreboard: pd.DataFrame) -> None:
    st.subheader("Scoreboard")

    if scoreboard is None or scoreboard.empty:
        st.info("No scoreboard data available.")
        return

    required = {"owner_id", "owner", "round", "slot", "unit", "pts"}
    missing = sorted(required - set(scoreboard.columns))
    if missing:
        st.error(f"Scoreboard dataframe missing columns: {missing}")
        return

    # Owner order: draft slot
    owners = (
        scoreboard[["owner_id", "owner"]]
        .drop_duplicates()
        .sort_values("owner_id")
        .to_dict("records")
    )

    max_round = int(pd.to_numeric(scoreboard["round"], errors="coerce").max())

    # Build lookup: (owner_id, round) -> row
    lookup: dict[tuple[int, int], dict] = {}
    for _, r in scoreboard.iterrows():
        key = (int(r["owner_id"]), int(r["round"]))
        # If there are duplicates, keep the first but warn
        if key not in lookup:
            lookup[key] = {
                "slot": r["slot"],
                "unit": r["unit"],
                "pts": r["pts"],
            }

    # Totals per owner
    totals = (
        scoreboard.groupby(["owner_id", "owner"], as_index=False)["pts"]
        .sum()
        .rename(columns={"pts": "total_pts"})
    )
    totals_map = {int(r["owner_id"]): float(r["total_pts"]) for _, r in totals.iterrows()}

    st.markdown(
        """
        <style>
        /* === GLOBAL LAYOUT OVERRIDE (scoreboard only) === */
        .block-container {
            padding-left: 1.5rem;
            padding-right: 1.5rem;
            max-width: 100%;
        }

        /* === SCOREBOARD CELLS === */
        .bbb-cell {
            border: 1px solid rgba(49, 51, 63, 0.25);
            border-radius: 6px;
            padding: 8px 8px 6px 8px;
            background: rgba(255,255,255,0.02);
            min-height: 76px;
            margin-bottom: 7px;
        }

        .bbb-slot {
            font-size: 12px;
            opacity: 0.75;
        }

        .bbb-unit {
            font-size: 18px;
            font-weight: 750;
            line-height: 1.1;
            margin-top: 2px;
            text-align: center;
        }

        .bbb-pts {
            font-size: 16px;
            font-weight: 800;
            text-align: right;
            margin-top: 2px;
        }

        .bbb-total {
            border: 2px solid rgba(49, 51, 63, 0.35);
            border-radius: 6px;
            padding: 10px 8px;
            font-size: 24px;
            font-weight: 900;
            text-align: center;
            background: rgba(255,255,255,0.02);
        }

        .bbb-round-label {
            font-size: 12px;
            opacity: 0.75;
            padding-top: 18px;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    # Header row: blank label column + owner headers
    header_cols = st.columns(
        [1] + [5] * len(owners),
        gap="small",
    )

    with header_cols[0]:
        st.write("")  # spacer for round labels
    for i, ow in enumerate(owners, start=1):
        with header_cols[i]:
            st.markdown(
                f"<div style='text-align: center; font-weight: 800;'>{ow['owner']}</div>",
                unsafe_allow_html=True,
)


    # Round rows
    for rnd in range(1, max_round + 1):
        row_cols = st.columns([1] + [5] * len(owners), gap="small")

        # Owner cells
        for i, ow in enumerate(owners, start=1):
            owner_id = int(ow["owner_id"])
            cell = lookup.get((owner_id, rnd))
            with row_cols[i]:
                if cell is None:
                    st.markdown("<div class='bbb-cell'></div>", unsafe_allow_html=True)
                else:
                    slot = int(cell["slot"])
                    unit = str(cell["unit"])
                    pts = float(cell["pts"])
                    st.markdown(
                        f"""
                        <div class="bbb-cell">
                            <div class="bbb-slot">{slot}</div>
                            <div class="bbb-unit">{unit}</div>
                            <div class="bbb-pts">{pts:.0f}</div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

    # Totals row
    tot_cols = st.columns([1] + [5] * len(owners), gap="small")

    for i, ow in enumerate(owners, start=1):
        owner_id = int(ow["owner_id"])
        total = totals_map.get(owner_id, 0.0)
        with tot_cols[i]:
            st.markdown(f"<div class='bbb-total'>{total:.0f}</div>", unsafe_allow_html=True)
