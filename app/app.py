from __future__ import annotations

import subprocess
from pathlib import Path

import pandas as pd
import streamlit as st

st.title("BBB Playoff Scoreboard")

out_path = Path("data/processed/scoring_plays.csv")

game_ids_str = st.text_input(
    "Game IDs (space-separated)",
    value="",
    help="Example: 2024010100 2024010101 (use NFL.com game ids you plan to refresh)",
)

cols = st.columns([1, 3])
with cols[0]:
    refresh = st.button("Refresh Data", type="primary")

with cols[1]:
    st.caption("Refresh runs an R script (nflfastR) and writes data/processed/scoring_plays.csv")

if refresh:
    game_ids = [x.strip() for x in game_ids_str.split() if x.strip()]
    if not game_ids:
        st.error("Enter at least one game id.")
        st.stop()

    cmd = ["Rscript", "r/refresh_pbp.R", *game_ids]
    res = subprocess.run(cmd, capture_output=True, text=True)

    if res.returncode != 0:
        st.error("Refresh failed")
        st.code(res.stderr)
    else:
        st.success("Refresh complete")
        if res.stdout:
            st.code(res.stdout)

if out_path.exists():
    st.subheader("Scoring plays (latest refresh)")
    df = pd.read_csv(out_path)
    st.dataframe(df, use_container_width=True)
else:
    st.info("No scoring plays file yet. Click Refresh Data.")
