# --- bootstrap import path (REQUIRED for Streamlit) ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root: bbb_scoreboard/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# -----------------------------------------------------

import os  # noqa: E402
import subprocess

import pandas as pd
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
import streamlit as st
from streamlit_js_eval import streamlit_js_eval
from dotenv import load_dotenv

from src.scoring import load_player_positions, score_team_position_totals, score_events
from src.domain.teams import canonicalize_team_column
from src.app_io import read_csv_safe, load_playoff_game_ids, normalize_scoring_df
from src.ingest import run_refresh
from src.scoreboard import build_scoreboard_dataset
from src.refresh import refresh_playoff_games, RefreshInProgress
from src.ui_sections import (
    section_event_feed,
    section_scoreboard_round_grid
)





# -------------------------
# Global configuration
# -------------------------
load_dotenv(ROOT / ".env")
BBB_SEASON = int(os.environ["BBB_SEASON"])

st.set_page_config(
    layout="wide",
    page_title="BBB Scoreboard",
)


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
DRAFT_PICKS = CONFIG / f"draft_picks_{BBB_SEASON}.csv"
POS_CACHE = PROCESSED / f"player_positions_{BBB_SEASON}.csv"

REFRESH_LOCK = PROCESSED / ".refresh.lock"
REFRESH_METRICS = PROCESSED / f"pbp_metrics_latest_{BBB_SEASON}.csv"
REFRESH_STATE = PROCESSED / f"game_refresh_state_{BBB_SEASON}.csv"

SCORING_PLAYS_PATH = PROCESSED / "scoring_plays.csv"

DEFAULT_TZ = "America/Chicago"  # fallback if detection fails

# -------------------------
# Small helpers (cached)
# -------------------------
@st.cache_data(show_spinner=False)
def load_positions(cache_path: Path) -> pd.DataFrame:
    return load_player_positions(cache_path)

st.markdown(
    """
    <style>
    .bbb-toast-wrap {
        position: fixed;
        top: 18px;
        left: 50%;
        transform: translateX(-50%);
        z-index: 999999;
        pointer-events: none; /* don't block clicks */
    }
    .bbb-toast {
        min-width: 360px;
        max-width: 760px;
        padding: 10px 14px;
        border-radius: 10px;
        border: 1px solid rgba(49, 51, 63, 0.25);
        background: rgba(20, 20, 20, 0.92);
        color: white;
        font-weight: 650;
        box-shadow: 0 10px 30px rgba(0,0,0,0.25);
        opacity: 0;
        animation: bbbFadeInOut 6s ease-in-out forwards;
        text-align: center;
    }
    .bbb-toast.success { background: rgba(19, 132, 70, 0.95); }
    .bbb-toast.info    { background: rgba(30, 64, 175, 0.95); }
    .bbb-toast.warning { background: rgba(180, 83, 9, 0.95); }
    .bbb-toast.error   { background: rgba(185, 28, 28, 0.95); }

    @keyframes bbbFadeInOut {
        0%   { opacity: 0; transform: translateY(-8px); }
        8%   { opacity: 1; transform: translateY(0); }
        85%  { opacity: 1; transform: translateY(0); }
        100% { opacity: 0; transform: translateY(-8px); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)


def bbb_toast(message: str, *, level: str = "info") -> None:
    """
    level: info|success|warning|error
    """
    st.session_state["bbb_toast_n"] = st.session_state.get("bbb_toast_n", 0) + 1
    n = st.session_state["bbb_toast_n"]

    st.markdown(
        f"""
        <div class="bbb-toast-wrap">
          <div class="bbb-toast {level}" id="bbb-toast-{n}">
            {message}
          </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_pending_toast() -> None:
    """
    If a toast was queued before st.rerun(), render it once on the next run.
    """
    pending = st.session_state.pop("bbb_pending_toast", None)
    if not pending:
        return
    msg, level = pending
    bbb_toast(msg, level=level)


def queue_toast(message: str, *, level: str = "info") -> None:
    """
    Queue a toast to be shown after st.rerun().
    """
    st.session_state["bbb_pending_toast"] = (message, level)


def _get_user_timezone() -> str:
    # If we have a non-default cached tz, trust it
    cached = st.session_state.get("user_tz")
    if cached and cached != DEFAULT_TZ:
        return cached

    tz = streamlit_js_eval(
        js_expressions="Intl.DateTimeFormat().resolvedOptions().timeZone",
        key="detect_tz",
    )

    tz_str = str(tz).strip() if tz else ""
    if tz_str and tz_str.lower() != "none":
        # Only cache when we got a real timezone from the browser
        st.session_state["user_tz"] = tz_str
        return tz_str

    # Do NOT cache fallback; just return it
    return DEFAULT_TZ


def _format_utc_iso_to_tz(ts_utc: str | None, tz_name: str) -> str | None:
    """
    ts_utc is expected to be ISO-8601 UTC like '2026-01-07T02:14:05Z'.
    Returns formatted local time: YYYY-MM-DD h:mmam/pm (no timezone suffix).
    """
    if not ts_utc:
        return None

    s = str(ts_utc).strip()
    try:
        dt_utc = datetime.fromisoformat(s.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(ZoneInfo(tz_name))
        return dt_local.strftime("%Y-%m-%d %-I:%M%p").lower()
    except Exception:
        return s


def _format_timestamp(ts: str | None) -> str | None:
    if not ts:
        return None

    s = str(ts).strip()

    # Common cleanup: drop trailing timezone tokens like " UTC"
    # and drop fractional seconds like ".123"
    s = s.replace("T", " ").replace("Z", "").strip()
    if "." in s:
        s = s.split(".", 1)[0].strip()
    if s.endswith(" UTC"):
        s = s[:-4].strip()

    # Try a couple known formats
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d %-I:%M%p").lower()
        except ValueError:
            continue

    # If parsing fails, return the original string so the UI still shows something
    return str(ts)


def _get_last_refresh_at(refresh_state_path: Path) -> str | None:
    """
    Returns the most recent successful refresh timestamp, or None.
    """
    if not refresh_state_path.exists():
        return None

    try:
        state = pd.read_csv(refresh_state_path)
        if "last_success_at" not in state.columns or state.empty:
            return None

        return (
            state["last_success_at"]
            .dropna()
            .astype(str)
            .max()
        )
    except Exception:
        return None

# --- Top bar: simple refresh control (stable) ---
raw_refresh_at = _get_last_refresh_at(REFRESH_STATE)
user_tz = _get_user_timezone()
formatted_refresh_at = _format_utc_iso_to_tz(raw_refresh_at, user_tz)

playoff_game_ids = load_playoff_game_ids(PLAYOFF_GAMES)

# -------------------------
# UI: Header + controls
# -------------------------
render_pending_toast()

left, right = st.columns([7, 3], vertical_alignment="top")
with left:
    st.title("Big Burger Bet")

sub_text = (
    f"Last refreshed at {formatted_refresh_at}"
    if formatted_refresh_at
    else "Press button to populate scores"
)

with right:
    msg_box = st.empty()  # stable place for refresh messages

    if st.button("Refresh Scores", type="primary", key="refresh_scores"):
        msg_box.empty()

        with st.spinner("Refreshing scores… may take up to 10 seconds"):
            try:
                result = refresh_playoff_games(
                    season=BBB_SEASON,
                    playoff_game_ids=playoff_game_ids,
                    cumulative_out_path=SCORING_PLAYS_PATH,
                    metrics_out_path=REFRESH_METRICS,
                    state_path=REFRESH_STATE,
                    lock_path=REFRESH_LOCK,
                    inactive_seconds=60 * 60,
                )
            except RefreshInProgress as e:
                msg_box.warning(str(e))
                st.stop()
            except Exception as e:
                msg_box.error(f"Refresh failed: {e}")
                st.stop()

        # Hard failure (refresh returned ok=False)
        if not result.ok:
            msg_box.error(result.message)
            st.stop()

        # Case 1: no eligible games
        if getattr(result, "eligible_games", None) == 0:
            bbb_toast("Nothing to refresh - scoreboard reflects final scores", level="info")
            # IMPORTANT: do NOT stop; allow the rest of the app to render
        # Case 2: checked games but nothing new
        elif getattr(result, "changed", False) is False:
            bbb_toast("Up to date — no new scoring plays found.", level="info")
            # IMPORTANT: do NOT stop; allow the rest of the app to render
        # Case 3: updated
        else:
            queue_toast("Scores updated", level="success")
            st.cache_data.clear()
            st.rerun()


    # Smaller text under the button
    st.caption(sub_text)


# -------------------------
# Load draft (scoreboard must render even if no scoring yet)
# -------------------------
draft_df = read_csv_safe(DRAFT_PICKS)
if "__read_error__" in draft_df.columns:
    st.warning(draft_df.loc[0, "__read_error__"])
    draft_df = pd.DataFrame()

# Initialize outputs (always defined)
totals = pd.DataFrame(columns=["team", "position", "pts"])
events = pd.DataFrame()

if draft_df.empty:
    st.warning(
        f"No draft picks loaded from {DRAFT_PICKS.name}. "
        "Scoreboard dataset will be unavailable."
    )
    scoreboard = pd.DataFrame()
else:
    scoreboard = pd.DataFrame()  # will be built after we decide totals/events

# -------------------------
# Read + normalize scoring plays
# -------------------------
df_scoring = read_csv_safe(SCORING)
if "__read_error__" in df_scoring.columns:
    st.warning(df_scoring.loc[0, "__read_error__"])
    df_scoring = pd.DataFrame()
df_scoring = normalize_scoring_df(df_scoring)

# If no scoring plays yet: show scoreboard (0s) + empty event feed, then stop
if df_scoring.empty:
    if not draft_df.empty:
        scoreboard = build_scoreboard_dataset(
            draft_df,
            totals,  # empty -> pts=0
            season=BBB_SEASON,
            validate=True,
        )
        section_scoreboard_round_grid(scoreboard)

    section_event_feed(events, draft_df=draft_df, team_filter=True)
    st.stop()

# -------------------------
# Now that we have scoring plays, ensure playoff scope + positions exist
# -------------------------
if not playoff_game_ids:
    st.warning(
        f"No playoff game_ids found in {PLAYOFF_GAMES.name}. "
        "Add game_ids to enable playoff scoring scope."
    )
    if not draft_df.empty:
        scoreboard = build_scoreboard_dataset(draft_df, totals, season=BBB_SEASON, validate=True)
        section_scoreboard_round_grid(scoreboard)
    section_event_feed(events, draft_df=draft_df, team_filter=True)
    st.stop()

if not POS_CACHE.exists():
    st.error(f"Missing {POS_CACHE.name}. Run a refresh once for season {BBB_SEASON} to generate player positions.")
    if not draft_df.empty:
        scoreboard = build_scoreboard_dataset(draft_df, totals, season=BBB_SEASON, validate=True)
        section_scoreboard_round_grid(scoreboard)
    section_event_feed(events, draft_df=draft_df, team_filter=True)
    st.stop()

positions = load_positions(POS_CACHE)

# -------------------------
# Compute totals + events
# -------------------------
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

# Canonicalize team abbreviations for consistent joins/display
events = canonicalize_team_column(events, "team")

# -------------------------
# Build + render scoreboard ONCE (now with points)
# -------------------------
if not draft_df.empty:
    scoreboard = build_scoreboard_dataset(
        draft_df,
        totals,
        season=BBB_SEASON,
        validate=True,
    )
    section_scoreboard_round_grid(scoreboard)

section_event_feed(events, draft_df=draft_df, team_filter=True)
