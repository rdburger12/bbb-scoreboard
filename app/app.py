import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

# Anchor everything to repo root regardless of how Streamlit is launched
ROOT = Path(__file__).resolve().parents[1]  # bbb_scoreboard/
R_SCRIPT = ROOT / "r" / "refresh_pbp.R"

SCORING = ROOT / "data" / "processed" / "scoring_plays.csv"
LATEST = ROOT / "data" / "processed" / "scoring_plays_latest.csv"
STATUS = ROOT / "data" / "processed" / "refresh_status.csv"
LOG = ROOT / "data" / "processed" / "refresh_log.csv"

st.title("BBB Scoreboard - Incremental Loading Test")

season = st.number_input("Season", value=2025, step=1)
week = st.number_input("Week", value=18, step=1)

if st.button("Refresh Data", type="primary"):
    res = subprocess.run(
        ["Rscript", str(R_SCRIPT), "--season", str(season), "--week", str(week)],
        cwd=str(ROOT),
        capture_output=True,
        text=True,
    )
    if res.returncode != 0:
        st.error("Refresh failed")
        st.code(res.stderr)
    else:
        st.success("Refresh complete")
        if res.stdout.strip():
            st.code(res.stdout)

st.subheader("Refresh status (latest)")
if STATUS.exists():
    st.dataframe(pd.read_csv(STATUS), use_container_width=True)
else:
    st.info("No refresh_status.csv yet. Click Refresh Data.")

st.subheader("Refresh log (last 20 attempts)")
if LOG.exists():
    df_log = pd.read_csv(LOG)
    st.dataframe(df_log.tail(20), use_container_width=True)
else:
    st.info("No refresh_log.csv yet. Click Refresh Data.")

st.subheader("Latest refresh scoring plays")
if LATEST.exists():
    df_latest = pd.read_csv(LATEST)
