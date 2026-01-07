from __future__ import annotations

import pandas as pd
import streamlit as st


def section_event_feed(
    events: pd.DataFrame,
    draft_df: pd.DataFrame,
    *,
    team_filter: bool = True,  # keep param for compatibility; we'll use new filters regardless
) -> None:

    view = events.copy()

    # -------------------------
    # Join owner via (team, position)
    # -------------------------
    if {"team", "position"}.issubset(view.columns) and {"team", "position", "owner"}.issubset(draft_df.columns):
        owners = draft_df[["team", "position", "owner"]].drop_duplicates()
        view = view.merge(owners, on=["team", "position"], how="left")

    # -------------------------
    # Three dropdown filters (same row)
    # -------------------------
    cols = st.columns(3)

    # Drafter filter
    with cols[0]:
        if "owner" in view.columns:
            drafters = ["(All)"] + sorted(
                [x for x in view["owner"].dropna().unique().tolist() if x]
            )
            sel_drafter = st.selectbox("Drafter", drafters, index=0)
        else:
            sel_drafter = "(All)"

    # NFL team filter
    with cols[1]:
        if "team" in view.columns:
            teams = ["(All)"] + sorted(
                [x for x in view["team"].dropna().unique().tolist() if x]
            )
            sel_team = st.selectbox("NFL Team", teams, index=0)
        else:
            sel_team = "(All)"

    # Position filter
    with cols[2]:
        if "position" in view.columns:
            positions = ["(All)"] + sorted(
                [x for x in view["position"].dropna().unique().tolist() if x]
            )
            sel_pos = st.selectbox("Position", positions, index=0)
        else:
            sel_pos = "(All)"

    # Apply filters AFTER all selections are made
    if sel_drafter != "(All)":
        view = view[view["owner"] == sel_drafter]

    if sel_team != "(All)":
        view = view[view["team"] == sel_team]

    if sel_pos != "(All)":
        view = view[view["position"] == sel_pos]

    if events.empty:
        return
    elif view.empty:
        st.info("No scoring events match the selected filters.")
        return

    # -------------------------
    # Build derived display fields on the unit rows
    # -------------------------
    # Play description
    view["Play Description"] = view.get("desc", "")

    # Game date
    view["Game Date"] = view.get("game_date", "")

    # Time: "Q3 00:43"
    if "qtr" in view.columns and "time" in view.columns:
        view["Time"] = "Q" + view["qtr"].astype(str) + " " + view["time"].astype(str)
    else:
        view["Time"] = ""

    # Unit-score string: "KC QB: 4 (Brianna)"
    # pts might be float; normalize to int if it looks integral
    if "pts" in view.columns:
        pts_numeric = pd.to_numeric(view["pts"], errors="coerce")
        pts_display = pts_numeric.map(lambda x: "" if pd.isna(x) else (str(int(x)) if float(x).is_integer() else str(x)))
    else:
        pts_display = pd.Series([""] * len(view), index=view.index)

    team_series = view["team"].astype(str) if "team" in view.columns else ""
    pos_series = view["position"].astype(str) if "position" in view.columns else ""
    owner_series = view["owner"].fillna("").astype(str) if "owner" in view.columns else ""

    view["UnitScore"] = (
        team_series + " " + pos_series
        + ": " + pts_display.astype(str)
        + " (" + owner_series + ")"
    )

    # -------------------------
    # Identify a play key so we can aggregate to one row per play
    # Prefer game_id+play_id; fall back to a weaker key if play_id isn't present.
    # -------------------------
    if {"game_id", "play_id"}.issubset(view.columns):
        view["_play_key"] = view["game_id"].astype(str) + "|" + view["play_id"].astype(str)
    else:
        # Fallback: may merge distinct plays if desc/time duplicates; acceptable until play_id is present
        fallback_cols = []
        for c in ["game_id", "game_date", "qtr", "time", "desc"]:
            if c in view.columns:
                fallback_cols.append(view[c].astype(str))
        view["_play_key"] = fallback_cols[0]
        for s in fallback_cols[1:]:
            view["_play_key"] = view["_play_key"] + "|" + s

    # -------------------------
    # Aggregate: one row per play with Score 1 / Score 2
    # Deterministic order: sort by team, position (and owner) within a play
    # -------------------------
    sort_within = [c for c in ["team", "position", "owner"] if c in view.columns]
    if sort_within:
        view = view.sort_values(sort_within)

    def _scores_to_two(values: list[str]) -> tuple[str, str]:
        vals = [v for v in values if v]
        if len(vals) == 0:
            return ("", "")
        if len(vals) == 1:
            return (vals[0], "")
        return (vals[0], vals[1])

    agg = (
        view.groupby("_play_key", as_index=False)
        .agg(
            {
                "Game Date": "first",
                "Time": "first",
                "Play Description": "first",
                "UnitScore": lambda s: list(s),
                # For sorting newest-first later if available
                **({c: "first" for c in ["game_id", "play_id", "game_date"] if c in view.columns}),
            }
        )
    )

    scores = agg["UnitScore"].apply(_scores_to_two)
    agg["Score 1"] = scores.apply(lambda x: x[0])
    agg["Score 2"] = scores.apply(lambda x: x[1])
    agg = agg.drop(columns=["UnitScore"])

    # -------------------------
    # Sort newest-first at the play level
    # -------------------------
    sort_cols = [c for c in ["game_date", "game_id", "play_id"] if c in agg.columns]
    if sort_cols:
        agg = agg.sort_values(sort_cols, ascending=False)

    # Final display
    view_df = agg[["Game Date", "Time", "Play Description", "Score 1", "Score 2"]]
    st.dataframe(view_df, width="stretch", height=520)


def section_scoreboard_round_grid(scoreboard: pd.DataFrame) -> None:
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
            font-size: 13px;
            opacity: 0.75;
        }

        .bbb-unit {
            font-size: 20px;
            font-weight: 750;
            line-height: 1.1;
            margin-top: 2px;
            text-align: center;
        }

        .bbb-pts {
            font-size: 18px;
            font-weight: 800;
            text-align: right;
            margin-top: 2px;
        }

        .bbb-total {
            border: 2px solid rgba(49, 51, 63, 0.35);
            border-radius: 6px;
            padding: 10px 8px;
            font-size: 28px;
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
