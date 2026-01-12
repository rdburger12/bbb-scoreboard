from __future__ import annotations

import pandas as pd
import streamlit as st
from html import escape


def section_event_feed(
    events: pd.DataFrame,
    draft_df: pd.DataFrame,
    *,
    team_filter: bool = True,
) -> None:
    view = events.copy()

    # Join owner via (team, position)
    if {"team", "position"}.issubset(view.columns) and {"team", "position", "owner"}.issubset(draft_df.columns):
        owners = draft_df[["team", "position", "owner"]].drop_duplicates()
        view = view.merge(owners, on=["team", "position"], how="left")

    # Filters
    cols = st.columns(3)
    with cols[0]:
        drafters = ["(All)"] + sorted(view["owner"].dropna().unique().tolist()) if "owner" in view.columns else ["(All)"]
        sel_drafter = st.selectbox("Drafter", drafters)
    with cols[1]:
        teams = ["(All)"] + sorted(view["team"].dropna().unique().tolist()) if "team" in view.columns else ["(All)"]
        sel_team = st.selectbox("NFL Team", teams)
    with cols[2]:
        positions = ["(All)"] + sorted(view["position"].dropna().unique().tolist()) if "position" in view.columns else ["(All)"]
        sel_pos = st.selectbox("Position", positions)

    if sel_drafter != "(All)":
        view = view[view["owner"] == sel_drafter]
    if sel_team != "(All)":
        view = view[view["team"] == sel_team]
    if sel_pos != "(All)":
        view = view[view["position"] == sel_pos]

    if events.empty:
        return
    if view.empty:
        st.info("No scoring events match the selected filters.")
        return

    # Derived display fields
    view["Play Description"] = view.get("desc", "")
    view["Game Date"] = view.get("game_date", "")

    # Game column from game_id: YYYY_WW_AWAY_HOME
    if "game_id" in view.columns:

        def _game_from_gid(gid: str) -> str:
            gid = str(gid or "")
            parts = gid.split("_")
            if len(parts) >= 4:
                away, home = parts[-2], parts[-1]
                return f"{away} vs {home}"
            return ""

        view["Game"] = view["game_id"].map(_game_from_gid)
    else:
        view["Game"] = ""

    if {"qtr", "time"}.issubset(view.columns):
        view["Time"] = "Q" + view["qtr"].astype(str) + " " + view["time"].astype(str)
    else:
        view["Time"] = ""

    pts_numeric = pd.to_numeric(view.get("pts"), errors="coerce")
    pts_display = pts_numeric.map(lambda x: "" if pd.isna(x) else str(int(x)))

    view["UnitScore"] = (
        view.get("team", "").astype(str)
        + " "
        + view.get("position", "").astype(str)
        + ": "
        + pts_display.astype(str)
        + " ("
        + view.get("owner", "").fillna("").astype(str)
        + ")"
    )

    # Play key
    if {"game_id", "play_id"}.issubset(view.columns):
        view["_play_key"] = view["game_id"].astype(str) + "|" + view["play_id"].astype(str)
    else:
        view["_play_key"] = view.index.astype(str)

    sort_cols = [c for c in ["team", "position", "owner"] if c in view.columns]
    if sort_cols:
        view = view.sort_values(sort_cols)

    # Aggregate to one row per play
    agg = (
        view.groupby("_play_key", as_index=False)
        .agg(
            {
                "Game Date": "first",
                "Game": "first",
                "Time": "first",
                "Play Description": "first",
                "UnitScore": lambda s: list(s),
                **({c: "first" for c in ["game_id", "play_id", "game_date"] if c in view.columns}),
            }
        )
    )

    def _scores_to_two(vals: list[str]) -> tuple[str, str]:
        vals = [v for v in vals if v]
        return (vals + ["", ""])[:2]

    scores = agg["UnitScore"].apply(_scores_to_two)
    agg["Score 1"] = scores.map(lambda x: x[0])
    agg["Score 2"] = scores.map(lambda x: x[1])
    agg = agg.drop(columns=["UnitScore"])

    sort_cols = [c for c in ["game_date", "game_id", "play_id"] if c in agg.columns]
    if sort_cols:
        agg = agg.sort_values(sort_cols, ascending=False)

    view_df = agg[["Game Date", "Game", "Time", "Play Description", "Score 1", "Score 2"]]
    st.dataframe(view_df, width="stretch", height=520)


def section_scoreboard_round_grid(
    scoreboard: pd.DataFrame,
    *,
    is_mobile: bool = False,
    eliminated_teams: set[str] | None = None,
) -> None:
    eliminated_teams = eliminated_teams or set()

    if scoreboard is None or scoreboard.empty:
        st.info("No scoreboard data available.")
        return

    required = {"owner_id", "owner", "round", "slot", "unit", "pts"}
    missing = sorted(required - set(scoreboard.columns))
    if missing:
        st.error(f"Scoreboard dataframe missing columns: {missing}")
        return

    owners = (
        scoreboard[["owner_id", "owner"]]
        .drop_duplicates()
        .sort_values("owner_id")
        .to_dict("records")
    )

    lookup: dict[tuple[int, int], dict] = {}
    for _, row in scoreboard.iterrows():
        lookup[(int(row["owner_id"]), int(row["round"]))] = row.to_dict()

    totals_map = scoreboard.groupby("owner_id", as_index=False)["pts"].sum().set_index("owner_id")["pts"].to_dict()
    max_round = int(scoreboard["round"].max())

    # -------------------------
    # MOBILE: owners as rows, rounds as columns, totals at end
    # -------------------------
    if is_mobile:
        rounds = list(range(1, min(6, max_round) + 1))

        st.markdown(
            """
            <style>
            .bbb-m-wrap { width: 100%; overflow-x: auto; -webkit-overflow-scrolling: touch; }
            .bbb-m-grid { display: flex; flex-direction: column; gap: 6px; padding-bottom: 2px; }

            .bbb-m-row {
                display: grid;
                grid-template-columns: 70px repeat(6, minmax(44px, 1fr)) 34px;
                gap: 4px;
                align-items: center;
                min-width: 350px;
            }

            .bbb-m-owner {
                font-size: 13px;
                font-weight: 750;
                text-align: right;
                padding-right: 4px;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                line-height: 1.0;
            }

            .bbb-m-chip {
                border: 1px solid rgba(49, 51, 63, 0.22);
                border-radius: 8px;
                padding: 4px;
                background: rgba(255,255,255,0.02);
                height: 52px;
                display: flex;
                flex-direction: column;
                justify-content: center;
                align-items: center;
                text-align: center;
                overflow: hidden;
            }

            .bbb-m-label {
                font-size: 11px;
                opacity: 0.78;
                line-height: 1.05;
                white-space: nowrap;
                overflow: hidden;
                text-overflow: ellipsis;
                max-width: 100%;
            }

            .bbb-m-points {
                font-size: 16px;
                font-weight: 900;
                line-height: 1.05;
                margin-top: 2px;
            }

            .bbb-m-total {
                font-size: 17px;
                font-weight: 900;
                text-align: right;
                padding-right: 2px;
                line-height: 1.0;
                opacity: 0.95;
            }

            .bbb-elim {
                opacity: 0.35;
            }

            .bbb-elim { opacity: 0.35; }

            @media (max-width: 420px) {
                .bbb-m-row { grid-template-columns: 62px repeat(6, minmax(40px, 1fr)) 30px; gap: 3px; }
                .bbb-m-chip { height: 50px; }
                .bbb-m-label { font-size: 8px; }
                .bbb-m-points { font-size: 15px; }
                .bbb-m-total { font-size: 16px; }
            }
            </style>
            """,
            unsafe_allow_html=True,
        )

        rows_html: list[str] = []
        for ow in owners:
            owner_id = int(ow["owner_id"])
            owner_name = escape(str(ow.get("owner", "")))

            chips: list[str] = []
            for rnd in rounds:
                cell = lookup.get((owner_id, rnd))
                if cell is None:
                    chips.append("<div class='bbb-m-chip'></div>")
                    continue

                unit = escape(str(cell.get("unit", "")))
                pts = float(cell.get("pts", 0.0))
                team = str(cell.get("team", "")).strip()
                elim_class = " bbb-elim" if team and team in eliminated_teams else ""

                chips.append(
                    f"<div class='bbb-m-chip{elim_class}'><div class='bbb-m-label'>{unit}</div><div class='bbb-m-points'>{pts:.0f}</div></div>"
                )

            total = float(totals_map.get(owner_id, 0.0))
            rows_html.append(
                "<div class='bbb-m-row'>"
                f"<div class='bbb-m-owner'>{owner_name}</div>"
                + "".join(chips)
                + f"<div class='bbb-m-total'>{total:.0f}</div>"
                + "</div>"
            )

        st.markdown("<div class='bbb-m-wrap'><div class='bbb-m-grid'>" + "".join(rows_html) + "</div></div>", unsafe_allow_html=True)
        return

    # -------------------------
    # DESKTOP: rounds as rows, owners as columns
    # -------------------------
    header_cols = st.columns([1] + [5] * len(owners), gap="small")
    for i, ow in enumerate(owners, start=1):
        with header_cols[i]:
            st.markdown(
                f"<div style='text-align:center;font-size:20px;font-weight:800'>{ow['owner']}</div>",
                unsafe_allow_html=True,
            )

    st.markdown(
        """
        <style>
        .bbb-cell {
            border: 1px solid rgba(49,51,63,0.25);
            border-radius: 6px;
            padding: 8px;
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
            border: 1px solid rgba(49, 51, 63, 0.35);
            border-radius: 8px;
            padding: 10px 10px 9px 10px;
            background: rgba(255,255,255,0.06);
            font-weight: 900;
            text-align: center;
            font-size: 24px
        }

        .bbb-elim {
            opacity: 0.35;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    for rnd in range(1, max_round + 1):
        row_cols = st.columns([1] + [5] * len(owners), gap="small")
        for i, ow in enumerate(owners, start=1):
            cell = lookup.get((int(ow["owner_id"]), rnd))
            with row_cols[i]:
                if not cell:
                    st.markdown("<div class='bbb-cell'></div>", unsafe_allow_html=True)
                else:
                    slot = int(cell["slot"])
                    unit = str(cell["unit"])
                    pts = float(cell["pts"])
                    team = str(cell.get("team", "")).strip()
                    elim_class = " bbb-elim" if team and team in eliminated_teams else ""

                    st.markdown(
                        f"""
                        <div class="bbb-cell{elim_class}">
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
        total = float(totals_map.get(owner_id, 0.0))
        with tot_cols[i]:
            st.markdown(f"<div class='bbb-total'>{total:.0f}</div>", unsafe_allow_html=True)
