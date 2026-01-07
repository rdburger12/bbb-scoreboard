# --- bootstrap import path (REQUIRED for Streamlit) ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root: bbb_scoreboard/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# -----------------------------------------------------

import os
import subprocess

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.scoring import load_player_positions, score_team_position_totals, score_events
from src.app_io import read_csv_safe, load_playoff_game_ids, normalize_scoring_df
from src.ingest import run_refresh
from src.ui_sections import (
    section_refresh_status,
    section_refresh_log,
    section_latest_scoring_plays,
    section_cumulative_scoring_plays,
    section_totals_table,
    section_event_feed,
    section_totals_tieout,
    section_playoff_scoping_diag,
)





# -------------------------
# Global configuration
# -------------------------
load_dotenv(ROOT / ".env")
BBB_SEASON = int(os.environ["BBB_SEASON"])


# -------------------------
# Paths
# -------------------------
R_SCRIPT = ROOT / "r" / "refresh_pbp.R"

CONFIG = ROOT / "data" / "config"
PROCESSED = ROOT / "data" / "processed"

SCORING = PROCESSED / "scoring_plays.csv"
LATEST = PROCESSED / "scoring_plays_latest.csv"
STATUS = PROCESSED / "refresh_status.csv"
LOG = PROCESSED / "refresh_log.csv"

PLAYOFF_GAMES = CONFIG / f"playoff_game_ids_{BBB_SEASON}.csv"
POS_CACHE = PROCESSED / f"player_positions_{BBB_SEASON}.csv"


# -------------------------
# Small helpers (cached)
# -------------------------
@st.cache_data(show_spinner=False)
def load_positions(cache_path: Path) -> pd.DataFrame:
    return load_player_positions(cache_path)

# -------------------------
# UI: Header + controls
# -------------------------
st.title(f"Big Burger Bet {BBB_SEASON}")
st.caption(f"Playoff season: {BBB_SEASON}")

week = st.number_input(
    "Playoff week to refresh",
    value=19,
    step=1,
    min_value=19,
    max_value=22,
    help=(
        "Used only for ingestion (schedule -> game_ids -> pbp). "
        "Scoring scope is controlled by playoff_game_ids_*.csv."
    ),
)

playoff_game_ids = load_playoff_game_ids(PLAYOFF_GAMES)
st.caption(f"Playoff games listed: {len(playoff_game_ids)}")

col_a, _ = st.columns([1, 3])
with col_a:
    refresh_week = st.button("Refresh This Week", type="primary")


# -------------------------
# Action: refresh
# -------------------------
if refresh_week:
    with st.spinner(f"Refreshing week {week}..."):
        res = run_refresh(root=ROOT, r_script=R_SCRIPT, season=BBB_SEASON, week=int(week))

    if res.returncode != 0:
        st.error("Refresh failed")
        if res.stdout.strip():
            st.subheader("stdout")
            st.code(res.stdout)
        if res.stderr.strip():
            st.subheader("stderr")
            st.code(res.stderr)
        st.stop()

    st.success(f"Refresh complete (week {week})")
    if res.stdout.strip():
        st.subheader("R output")
        st.code(res.stdout)
    if res.stderr.strip():
        st.subheader("R warnings (stderr)")
        st.code(res.stderr)


df_status = section_refresh_status(STATUS)
df_log = section_refresh_log(LOG, n=20)
df_latest = section_latest_scoring_plays(LATEST, n=50)

# read + normalize scoring plays once, then display
df_scoring = read_csv_safe(SCORING)
if "__read_error__" in df_scoring.columns:
    st.warning(df_scoring.loc[0, "__read_error__"])
    df_scoring = pd.DataFrame()
df_scoring = normalize_scoring_df(df_scoring)

section_cumulative_scoring_plays(SCORING, df_scoring=df_scoring, n=50)

if df_scoring.empty:
    st.info("No scoring plays loaded yet (scoring_plays.csv is empty).")
    st.stop()

if not playoff_game_ids:
    st.warning(f"No playoff game_ids found in {PLAYOFF_GAMES.name}. Add game_ids to enable playoff scoring scope.")
    st.stop()

if not POS_CACHE.exists():
    st.error(f"Missing {POS_CACHE.name}. Run a refresh once for season {BBB_SEASON} to generate player positions.")
    st.stop()

positions = load_positions(POS_CACHE)

totals = score_team_position_totals(
    df_scoring,
    positions,
    season=BBB_SEASON,
    week_max=None,
    game_ids=playoff_game_ids,
)

events = score_events(
    df_scoring,
    positions,
    season=BBB_SEASON,
    week_max=None,
    game_ids=playoff_game_ids,
)

section_totals_table(totals)
section_playoff_scoping_diag(
    playoff_games_path=PLAYOFF_GAMES,
    playoff_game_ids=playoff_game_ids,
    df_scoring=df_scoring,
)
section_event_feed(events, team_filter=True)
section_totals_tieout(totals, events)
