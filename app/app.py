# --- bootstrap import path (REQUIRED for Streamlit) ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # bbb_scoreboard/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# -----------------------------------------------------

import os
import subprocess

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# -------------------------
# Global configuration
# -------------------------
load_dotenv(ROOT / ".env")
BBB_SEASON = int(os.environ["BBB_SEASON"])

# -------------------------
# App imports
# -------------------------
from src.gameset import GameSet, load_game_ids
from src.scoring import load_player_positions, score_team_position_totals

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
DRAFT_PICKS = CONFIG / "draft_picks.csv"
POS_CACHE = PROCESSED / f"player_positions_{BBB_SEASON}.csv"

if not DRAFT_PICKS.exists():
    st.error("Missing draft_picks.csv. Run scripts/transform_draft_csv.py.")
    st.stop()
draft = pd.read_csv(DRAFT_PICKS)





def run_refresh(season: int, week: int) -> subprocess.CompletedProcess[str]:
    """Run the R refresh for a single week."""
    return subprocess.run(
        ["Rscript", str(R_SCRIPT), "--season", str(season), "--week", str(week)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )

@st.cache_data(show_spinner=False)
def read_csv_safe(path_str: str) -> pd.DataFrame:
    path = Path(path_str)
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, keep_default_na=True)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        # cannot call st.warning inside cached function reliably; return sentinel
        return pd.DataFrame({"__read_error__": [f"Could not read {path.name}: {e}"]})


st.title(f"Big Burger Bet {BBB_SEASON}")

st.caption(f"Playoff season: {BBB_SEASON}")

week = st.number_input(
    "Playoff week to refresh",
    value=19,
    step=1,
    min_value=19,
    max_value=22,
    help="Used only for ingestion (schedule -> game_ids -> pbp). Scoring scope is controlled by playoff_game_ids_*.csv.",
)

# Playoff-only scope
gs = GameSet(mode="playoffs", season=BBB_SEASON, playoff_game_ids_path=PLAYOFF_GAMES)

col_a, col_b = st.columns(2)

with col_a:
    refresh_week = st.button("Refresh This Week", type="primary")

# -------------------------
# Actions
# -------------------------
if refresh_week:
    with st.spinner(f"Refreshing week {week}..."):
        res = run_refresh(BBB_SEASON, int(week))

    if res.returncode != 0:
        st.error("Refresh failed")
        if res.stdout.strip():
            st.subheader("stdout")
            st.code(res.stdout)
        if res.stderr.strip():
            st.subheader("stderr")
            st.code(res.stderr)
    else:
        st.success(f"Refresh complete (week {week})")
        if res.stdout.strip():
            st.subheader("R output")
            st.code(res.stdout)
        if res.stderr.strip():
            st.subheader("R warnings (stderr)")
            st.code(res.stderr)

# -------------------------
# Outputs
# -------------------------
st.subheader("Refresh status (latest)")
df_status = read_csv_safe(str(STATUS))
if "__read_error__" in df_status.columns:
    st.warning(df_status.loc[0, "__read_error__"])
    df_status = pd.DataFrame()
if df_status.empty:
    st.info("No refresh_status.csv yet. Click Refresh.")
else:
    if "refreshed_at" in df_status.columns and len(df_status) == 1:
        st.caption(f"Last refresh: {df_status.loc[0, 'refreshed_at']}")
    st.dataframe(df_status, use_container_width=True)

st.subheader("Refresh log (last 20 attempts)")
df_log = read_csv_safe(str(LOG))
if "__read_error__" in df_log.columns:
    st.warning(df_log.loc[0, "__read_error__"])
    df_log = pd.DataFrame()
if df_log.empty:
    st.info("No refresh_log.csv yet. Click Refresh.")
else:
    st.dataframe(df_log.tail(20), use_container_width=True)

st.subheader("Latest refresh scoring plays")
df_latest = read_csv_safe(str(LATEST))
if "__read_error__" in df_latest.columns:
    st.warning(df_latest.loc[0, "__read_error__"])
    df_latest = pd.DataFrame()
if df_latest.empty:
    st.info("No latest file yet, or latest refresh returned 0 scoring plays.")
else:
    st.write(f"Rows: {len(df_latest)}")
    st.dataframe(df_latest.head(50), use_container_width=True)

st.subheader("Cumulative scoring plays (upserted)")
df_scoring = read_csv_safe(str(SCORING))

if "__read_error__" in df_scoring.columns:
    st.warning(df_scoring.loc[0, "__read_error__"])
    df_scoring = pd.DataFrame()

def _clean_player_id(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
         .astype(str)
         .str.strip()
         .str.replace(r"\.0$", "", regex=True)
         .replace({"nan": "", "None": ""})
    )

if df_scoring.empty:
    st.info("No cumulative scoring file yet.")
else:
    # Normalize key columns for consistent downstream behavior (merges, filters, diagnostics)
    if "game_id" in df_scoring.columns:
        df_scoring["game_id"] = df_scoring["game_id"].astype(str)

    if "play_id" in df_scoring.columns:
        df_scoring["play_id"] = pd.to_numeric(df_scoring["play_id"], errors="coerce").astype("Int64")

    # Normalize player id columns to string to avoid roster merge mismatches
    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in df_scoring.columns:
            df_scoring[c] = df_scoring[c].fillna("").astype(str)

    if {"game_id", "play_id"}.issubset(df_scoring.columns):
        unique_keys = df_scoring[["game_id", "play_id"]].drop_duplicates().shape[0]
        st.write(f"Rows: {len(df_scoring)} | Unique keys: {unique_keys}")
    else:
        st.write(f"Rows: {len(df_scoring)}")

    st.dataframe(df_scoring.tail(50), use_container_width=True)

st.subheader("Cumulative Fantasy Totals (Team × Position)")

if df_scoring.empty:
    st.info("No scoring plays loaded yet (scoring_plays.csv is empty).")
else:
    pos_cache = PROCESSED / f"player_positions_{BBB_SEASON}.csv"

    if not pos_cache.exists():
        st.error(
            f"Missing {pos_cache.name}. Run a refresh once for season {BBB_SEASON} to generate player positions."
        )
        st.stop()

    @st.cache_data(show_spinner=False)
    def _load_positions_cached(cache_path_str: str) -> pd.DataFrame:
        # local file produced by R; no network / no season arg needed
        return load_player_positions(Path(cache_path_str))

    positions = _load_positions_cached(str(pos_cache))


    # Resolve game_ids from the selected scope
    try:
        game_ids = load_game_ids(df_scoring, gs)
    except Exception as e:
        st.error(f"Scope error: {e}")
        game_ids = set()

    # Compute totals using explicit game_ids (this is the key architecture shift)
    totals = score_team_position_totals(
        df_scoring,
        positions,
        season=BBB_SEASON,
        week_max=None,                 # playoffs: do not use week_max
        game_ids=game_ids,             # playoffs: explicit game_id scope
    )

    if totals is None:
        st.error("Scoring returned None (expected a DataFrame). Check src/scoring.py for a missing return or an exception.")
        st.stop()

    # Optional diagnostics (cheap correctness guard)
    with st.expander("Diagnostics", expanded=False):
        st.write(f"Scoring plays rows in scope (pre-score): {len(df_scoring)}")

    if totals.empty:
        st.info("No totals in the selected scope.")
    else:
        st.dataframe(totals, use_container_width=True)
