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
def run_refresh(season: int, week: int) -> subprocess.CompletedProcess[str]:
    """Run the R refresh for a single week."""
    return subprocess.run(
        ["Rscript", str(R_SCRIPT), "--season", str(season), "--week", str(week)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )


@st.cache_data(show_spinner=False)
def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    try:
        return pd.read_csv(path, keep_default_na=True)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        # cannot call st.warning inside cached function reliably; return sentinel
        return pd.DataFrame({"__read_error__": [f"Could not read {path.name}: {e}"]})


@st.cache_data(show_spinner=False)
def load_playoff_game_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()

    df = pd.read_csv(path)
    if "game_id" not in df.columns:
        raise ValueError(f"{path} must have a 'game_id' column")

    gids = (
        df["game_id"]
        .dropna()
        .astype(str)
        .str.strip()
        .loc[lambda s: s.ne("")]
        .unique()
        .tolist()
    )
    return set(gids)


@st.cache_data(show_spinner=False)
def load_positions(cache_path: Path) -> pd.DataFrame:
    return load_player_positions(cache_path)


def _clean_player_id(s: pd.Series) -> pd.Series:
    return (
        s.fillna("")
        .astype(str)
        .str.strip()
        .str.replace(r"\.0$", "", regex=True)
        .replace({"nan": "", "None": ""})
    )


def normalize_scoring_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize IDs once for stable merges/filtering."""
    if df.empty:
        return df

    out = df.copy()

    if "game_id" in out.columns:
        out["game_id"] = out["game_id"].astype(str)

    if "play_id" in out.columns:
        out["play_id"] = pd.to_numeric(out["play_id"], errors="coerce").astype("Int64")

    for c in ["passer_player_id", "receiver_player_id", "rusher_player_id", "kicker_player_id"]:
        if c in out.columns:
            out[c] = _clean_player_id(out[c])

    return out


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
        res = run_refresh(BBB_SEASON, int(week))

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


# -------------------------
# Outputs: status/log/latest/scoring plays
# -------------------------
st.subheader("Refresh status (latest)")
df_status = read_csv_safe(STATUS)
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
df_log = read_csv_safe(LOG)
if "__read_error__" in df_log.columns:
    st.warning(df_log.loc[0, "__read_error__"])
    df_log = pd.DataFrame()

if df_log.empty:
    st.info("No refresh_log.csv yet. Click Refresh.")
else:
    st.dataframe(df_log.tail(20), use_container_width=True)

st.subheader("Latest refresh scoring plays")
df_latest = read_csv_safe(LATEST)
if "__read_error__" in df_latest.columns:
    st.warning(df_latest.loc[0, "__read_error__"])
    df_latest = pd.DataFrame()

if df_latest.empty:
    st.info("No latest file yet, or latest refresh returned 0 scoring plays.")
else:
    st.write(f"Rows: {len(df_latest)}")
    st.dataframe(df_latest.head(50), use_container_width=True)

st.subheader("Cumulative scoring plays (upserted)")
df_scoring = read_csv_safe(SCORING)
if "__read_error__" in df_scoring.columns:
    st.warning(df_scoring.loc[0, "__read_error__"])
    df_scoring = pd.DataFrame()

df_scoring = normalize_scoring_df(df_scoring)

if df_scoring.empty:
    st.info("No cumulative scoring file yet.")
else:
    if {"game_id", "play_id"}.issubset(df_scoring.columns):
        unique_keys = df_scoring[["game_id", "play_id"]].drop_duplicates().shape[0]
        st.write(f"Rows: {len(df_scoring)} | Unique keys: {unique_keys}")
    else:
        st.write(f"Rows: {len(df_scoring)}")

    st.dataframe(df_scoring.tail(50), use_container_width=True)


# -------------------------
# Scoring outputs (playoff-only scope)
# -------------------------
st.subheader("Cumulative Fantasy Totals (Team × Position)")

if df_scoring.empty:
    st.info("No scoring plays loaded yet (scoring_plays.csv is empty).")
    st.stop()

if not playoff_game_ids:
    st.warning(
        f"No playoff game_ids found in {PLAYOFF_GAMES.name}. "
        "Add game_ids to enable playoff scoring scope."
    )
    st.stop()

if not POS_CACHE.exists():
    st.error(
        f"Missing {POS_CACHE.name}. Run a refresh once for season {BBB_SEASON} "
        "to generate player positions."
    )
    st.stop()

positions = load_positions(POS_CACHE)

totals = score_team_position_totals(
    df_scoring,
    positions,
    season=BBB_SEASON,
    week_max=None,
    game_ids=playoff_game_ids,
)

if totals.empty:
    st.info("No totals in the selected scope.")
else:
    st.dataframe(totals, use_container_width=True)


with st.expander("Diagnostics: playoff scoping", expanded=False):
    st.write("playoff_game_ids file:", PLAYOFF_GAMES)
    st.write("playoff_game_ids count:", len(playoff_game_ids))
    st.write("games in scoring_plays.csv:", df_scoring["game_id"].nunique() if "game_id" in df_scoring.columns else 0)

st.subheader("Scoring Plays (event feed)")
ev = score_events(
    df_scoring,
    positions,
    season=BBB_SEASON,
    week_max=None,
    game_ids=playoff_game_ids,
)

if ev.empty:
    st.info("No scoring events in scope.")
else:
    teams = ["(All)"] + sorted([t for t in ev["team"].unique().tolist() if t])
    sel_team = st.selectbox("Filter team", teams, index=0)
    view = ev if sel_team == "(All)" else ev[ev["team"] == sel_team]
    st.dataframe(view, use_container_width=True, height=520)

with st.expander("Diagnostics: totals tie-out", expanded=False):
    ev_totals = (
        ev.groupby(["team", "position"], as_index=False)["pts"]
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
