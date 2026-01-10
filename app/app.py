# --- bootstrap import path (REQUIRED for Streamlit) ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root: bbb_scoreboard/
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
# -----------------------------------------------------

import os 

import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import streamlit as st
from streamlit_js_eval import streamlit_js_eval
from dotenv import load_dotenv

from src.scoring import load_player_positions, score_team_position_totals, score_events
from src.domain.teams import canonicalize_team_column
from src.app_io import read_csv_safe, load_playoff_game_ids, normalize_scoring_df
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

# --- Toast + layout CSS (inject once) ---
st.markdown(
    """
    <style>
    /* Tighten overall page top padding */
    section.main > div {
        padding-top: 0.5rem !important;
    }

    /* Reduce space above the main title */
    h1 {
        margin-top: 0.25rem !important;
        margin-bottom: 0.75rem !important;
    }

    /* Mobile tweaks: tighter gutters + slightly smaller title */
    @media (max-width: 768px) {
        .block-container {
            padding-left: 0.6rem !important;
            padding-right: 0.6rem !important;
        }
        h1 {
            font-size: 2.1rem !important;
            line-height: 1.05 !important;
        }
    }

    .bbb-toast-wrap {
        display: flex;
        justify-content: center;
        align-items: center;
        width: 100%;
    }
    .bbb-toast {
        width: 100%;
        max-width: 560px;
        padding: 10px 14px;
        border-radius: 10px;
        border: 1px solid rgba(49, 51, 63, 0.25);
        background: rgba(20, 20, 20, 0.92);
        color: white;
        font-weight: 650;
        box-shadow: 0 10px 30px rgba(0,0,0,0.20);
        opacity: 0;
        animation: bbbFadeInOut 6s ease-in-out forwards;
        text-align: center;
    }
    .bbb-toast.success { background: rgba(19, 132, 70, 0.95); }
    .bbb-toast.info    { background: rgba(30, 64, 175, 0.95); }
    .bbb-toast.warning { background: rgba(180, 83, 9, 0.95); }
    .bbb-toast.error   { background: rgba(185, 28, 28, 0.95); }

    @keyframes bbbFadeInOut {
        0%   { opacity: 0; transform: translateY(-6px); }
        8%   { opacity: 1; transform: translateY(0); }
        85%  { opacity: 1; transform: translateY(0); }
        100% { opacity: 0; transform: translateY(-6px); }
    }
    </style>
    """,
    unsafe_allow_html=True,
)

def queue_toast(message: str, *, level: str = "info") -> None:
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



def _get_viewport_width() -> int | None:
    cached = st.session_state.get('viewport_width')
    if isinstance(cached, int) and cached > 0:
        return cached

    vw = streamlit_js_eval(
        js_expressions='window.innerWidth',
        key='detect_viewport_width',
    )

    try:
        vw_int = int(vw) if vw is not None else None
    except Exception:
        vw_int = None

    if vw_int is not None and vw_int > 0:
        st.session_state['viewport_width'] = vw_int
        return vw_int

    return None

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
VIEWPORT_WIDTH = _get_viewport_width()
if VIEWPORT_WIDTH is None:
    # On first load, browser-provided width is often unavailable.
    # Stop so Streamlit reruns once the JS value arrives.
    st.stop()
IS_MOBILE = VIEWPORT_WIDTH < 768

formatted_refresh_at = _format_utc_iso_to_tz(raw_refresh_at, user_tz)

playoff_game_ids = load_playoff_game_ids(PLAYOFF_GAMES)

# -------------------------
# UI: Header + controls
# -------------------------
sub_text = (
    f"Last refreshed at {formatted_refresh_at}"
    if formatted_refresh_at
    else "Press button to populate scores"
)

topbar = st.container()
with topbar:
    left, mid, right = st.columns([4, 6, 4], vertical_alignment="center")

    with left:
        st.markdown(
            "<h1 style='margin:0; padding:0; line-height:1.05;'>BBB Scoreboard</h1>",
            unsafe_allow_html=True,
        )

    with mid:
        TOAST_SLOT = st.empty()

        def bbb_toast(message: str, *, level: str = "info") -> None:
            st.session_state["bbb_toast_n"] = st.session_state.get("bbb_toast_n", 0) + 1
            n = st.session_state["bbb_toast_n"]

            TOAST_SLOT.markdown(
                f"""
                <div class="bbb-toast-wrap">
                  <div class="bbb-toast {level}" id="bbb-toast-{n}">
                    {message}
                  </div>
                </div>
                """,
                unsafe_allow_html=True,
            )

        # Render any pending toast queued before st.rerun()
        pending = st.session_state.pop("bbb_pending_toast", None)
        if pending:
            msg, level = pending
            bbb_toast(msg, level=level)
        else:
            # keep the slot height-neutral when empty
            TOAST_SLOT.markdown("<div style='height:0'></div>", unsafe_allow_html=True)

    with right:
        st.markdown("<div style='padding-top:4px;'></div>", unsafe_allow_html=True)
        # Nested columns to force right alignment
        spacer, btn_col = st.columns([3, 2])

        with btn_col:
            if st.button("Refresh Scores", type="primary", key="refresh_scores"):
                result = None

                with st.spinner("Refreshing scores…"):
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
                    except RefreshInProgress:
                        bbb_toast(
                            "Refresh already in progress. Try again in a moment.",
                            level="warning",
                        )
                        result = None
                    except Exception as e:
                        bbb_toast(f"Refresh failed: {e}", level="error")
                        result = None

                if result is not None:
                    if not result.ok:
                        bbb_toast(result.message, level="error")
                    else:
                        if result.eligible_games == 0:
                            queue_toast(
                                "Nothing to refresh - scoreboard reflects final scores",
                                level="info",
                            )
                        elif result.changed is False:
                            queue_toast(
                                "Up to date — no new scoring plays found.",
                                level="info",
                            )
                        else:
                            queue_toast("Scores updated", level="success")

                        st.cache_data.clear()
                        st.rerun()

            # Right-aligned caption under the button
            st.markdown(
    f"""
    <div style='text-align:right;
                font-size:0.85rem;
                opacity:0.75;
                white-space:nowrap;'>
        {sub_text}
    </div>
    """,
    unsafe_allow_html=True,
)


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

    # Render the scoreboard even when there are no scoring plays yet.
    section_scoreboard_round_grid(scoreboard, is_mobile=IS_MOBILE)

    # The play feed is desktop-only.
    if not IS_MOBILE:
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
    section_scoreboard_round_grid(scoreboard, is_mobile=IS_MOBILE)
    if not IS_MOBILE:
        section_event_feed(events, draft_df=draft_df, team_filter=True)
    st.stop()

if not POS_CACHE.exists():
    #st.error(f"Missing {POS_CACHE.name}. Run a refresh once for season {BBB_SEASON} to generate player positions.")
    if not draft_df.empty:
        scoreboard = build_scoreboard_dataset(draft_df, totals, season=BBB_SEASON, validate=True)
    section_scoreboard_round_grid(scoreboard, is_mobile=IS_MOBILE)
    if not IS_MOBILE:
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
    section_scoreboard_round_grid(scoreboard, is_mobile=IS_MOBILE)

st.markdown("<div style='height: 24px;'></div>", unsafe_allow_html=True)

if not IS_MOBILE:
    section_event_feed(events, draft_df=draft_df, team_filter=True)