import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

# Anchor everything to repo root regardless of how Streamlit is launched
ROOT = Path(__file__).resolve().parents[1]  # bbb_scoreboard/
R_SCRIPT = ROOT / "r" / "refresh_pbp.R"

PROCESSED = ROOT / "data" / "processed"
SCORING = PROCESSED / "scoring_plays.csv"
LATEST = PROCESSED / "scoring_plays_latest.csv"
STATUS = PROCESSED / "refresh_status.csv"
LOG = PROCESSED / "refresh_log.csv"


def read_csv_safe(path: Path) -> pd.DataFrame:
    """Read CSV safely; return empty DF if missing/empty/unreadable."""
    if not path.exists():
        return pd.DataFrame()
    try:
        # keep_default_na=False avoids pandas converting empty strings to NaN in some cases
        return pd.read_csv(path, keep_default_na=True)
    except pd.errors.EmptyDataError:
        return pd.DataFrame()
    except Exception as e:
        st.warning(f"Could not read {path.name}: {e}")
        return pd.DataFrame()


st.title("BBB Scoreboard - Incremental Loading Test")

season = st.number_input("Season", value=2025, step=1)
week = st.number_input("Week", value=18, step=1)

refresh_clicked = st.button("Refresh Data", type="primary")

if refresh_clicked:
    with st.spinner("Refreshing data (running Rscript)..."):
        res = subprocess.run(
            ["Rscript", str(R_SCRIPT), "--season", str(season), "--week", str(week)],
            cwd=str(ROOT),
            capture_output=True,
            text=True,
        )

    if res.returncode != 0:
        st.error("Refresh failed")
        if res.stdout.strip():
            st.subheader("stdout")
            st.code(res.stdout)
        if res.stderr.strip():
            st.subheader("stderr")
            st.code(res.stderr)
    else:
        st.success("Refresh complete")
        if res.stdout.strip():
            st.subheader("R output")
            st.code(res.stdout)
        # nflfastR/future may emit warnings to stderr even on success
        if res.stderr.strip():
            st.subheader("R warnings (stderr)")
            st.code(res.stderr)


st.subheader("Refresh status (latest)")
df_status = read_csv_safe(STATUS)
if df_status.empty:
    st.info("No refresh_status.csv yet. Click Refresh Data.")
else:
    # Nice-to-have: show last refresh timestamp prominently if present
    if "refreshed_at" in df_status.columns and len(df_status) == 1:
        st.caption(f"Last refresh: {df_status.loc[0, 'refreshed_at']}")
    st.dataframe(df_status, use_container_width=True)

st.subheader("Refresh log (last 20 attempts)")
df_log = read_csv_safe(LOG)
if df_log.empty:
    st.info("No refresh_log.csv yet. Click Refresh Data.")
else:
    st.dataframe(df_log.tail(20), use_container_width=True)

st.subheader("Latest refresh scoring plays")
df_latest = read_csv_safe(LATEST)
if df_latest.empty:
    st.info("No latest file yet, or latest refresh returned 0 scoring plays.")
else:
    st.write(f"Rows: {len(df_latest)}")
    st.dataframe(df_latest.head(50), use_container_width=True)

st.subheader("Cumulative scoring plays (upserted)")
df_scoring = read_csv_safe(SCORING)
if df_scoring.empty:
    st.info("No cumulative scoring file yet.")
else:
    if {"game_id", "play_id"}.issubset(df_scoring.columns):
        unique_keys = df_scoring[["game_id", "play_id"]].drop_duplicates().shape[0]
        st.write(f"Rows: {len(df_scoring)} | Unique keys: {unique_keys}")
    else:
        st.write(f"Rows: {len(df_scoring)}")
    st.dataframe(df_scoring.tail(50), use_container_width=True)
