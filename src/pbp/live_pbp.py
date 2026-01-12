from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests

from .schedule import gsis_for_game_id


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nfl.com/",
}


# Use www.nfl.com because static.nfl.com does not resolve in your environment.
_GTD_URL = "https://www.nfl.com/liveupdate/game-center/{eid}/{eid}_gtd.json"


@dataclass(frozen=True)
class LiveGameMetrics:
    game_id: str
    event_id: str
    pbp_rows: int
    max_play_id: Optional[int]
    is_final: bool
    refreshed_at: str
    status: str  # "ok" or "not_loaded_yet" or "error"
    detail: str = ""


def _now_utc_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None or (isinstance(x, float) and pd.isna(x)) or pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def _safe_str(x: Any) -> Optional[str]:
    if x is None:
        return None
    try:
        return str(x)
    except Exception:
        return None


def _to_bool(x: Any) -> bool:
    if isinstance(x, bool):
        return x
    if x is None:
        return False
    try:
        if isinstance(x, (int, float)):
            return int(x) == 1
    except Exception:
        pass
    try:
        s = str(x).strip().lower()
        return s in {"1", "true", "t", "yes", "y"}
    except Exception:
        return False


def fetch_gtd_json(event_id: str) -> dict:
    url = f"https://www.nfl.com/liveupdate/game-center/{event_id}/{event_id}_gtd.json"
    try:
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=15)
    except Exception as e:
        raise RuntimeError(f"Request failed for {url}: {e}") from e

    if r.status_code == 404:
        return {}

    if r.status_code != 200:
        raise RuntimeError(f"GTD HTTP {r.status_code} for {url}: {r.text[:200]}")

    try:
        return r.json() or {}
    except Exception as e:
        raise RuntimeError(f"Invalid JSON from {url}: {e}. Body head: {r.text[:200]}") from e


def _extract_game_blob(gtd: Dict[str, Any], event_id: str) -> Dict[str, Any]:
    """
    GTD JSON is typically: { "<event_id>": { ...game... } }
    """
    if not gtd:
        return {}
    if event_id in gtd and isinstance(gtd[event_id], dict):
        return gtd[event_id]
    if "drives" in gtd or "home" in gtd or "away" in gtd:
        return gtd
    return {}


def _infer_is_final(game: Dict[str, Any]) -> bool:
    """
    Conservative final detection.
    """
    for k in ("phase", "gameStatus", "status"):
        v = game.get(k)
        if v is None:
            continue
        s = str(v).strip().lower()
        if s in {"final", "finished", "complete", "completed"}:
            return True

    for k in ("final", "isFinal", "is_final"):
        if k in game:
            return _to_bool(game.get(k))

    return False


def _normalize_scoring_fields_from_play(play: Dict[str, Any]) -> Dict[str, Any]:
    desc = _safe_str(play.get("desc")) or ""

    spt = (play.get("scoringPlayType") or play.get("scoring_play_type") or "")
    spt_s = str(spt).strip().lower()

    touchdown = 1 if ("touchdown" in spt_s or spt_s == "td") else 0
    safety = 1 if ("safety" in spt_s) else 0

    field_goal_result = None
    extra_point_result = None
    two_point_conv_result = None

    if "field goal" in spt_s or spt_s in {"fg", "fieldgoal"}:
        if "no good" in desc.lower() or "missed" in desc.lower() or "blocked" in desc.lower():
            field_goal_result = "missed"
        else:
            field_goal_result = "made"

    if "extra point" in spt_s or spt_s in {"xp", "pat"}:
        if "no good" in desc.lower() or "missed" in desc.lower() or "blocked" in desc.lower():
            extra_point_result = "no good"
        else:
            extra_point_result = "good"

    if "two-point" in spt_s or "two point" in spt_s or spt_s in {"2pt", "two_point"}:
        if "conversion succeeds" in desc.lower() or "is good" in desc.lower() or "successful" in desc.lower():
            two_point_conv_result = "success"
        elif "fails" in desc.lower() or "no good" in desc.lower() or "unsuccessful" in desc.lower():
            two_point_conv_result = "fail"
        else:
            two_point_conv_result = None

    pass_td = False
    rush_td = False
    if touchdown == 1:
        d = desc.lower()
        if "pass" in d and ("to " in d or "complete" in d or "incomplete" in d):
            pass_td = True
        if "left end" in d or "right end" in d or "up the middle" in d or "run" in d or "rush" in d:
            rush_td = True

    defensive_two_point_conv = 1 if ("defensive two-point" in spt_s or "defensive 2pt" in spt_s) else 0

    return {
        "touchdown": touchdown,
        "safety": safety,
        "field_goal_result": field_goal_result,
        "extra_point_result": extra_point_result,
        "two_point_conv_result": two_point_conv_result,
        "pass_touchdown": pass_td,
        "rush_touchdown": rush_td,
        "defensive_two_point_conv": defensive_two_point_conv,
    }


def gtd_game_to_pbp_df(game_id: str, event_id: str, gtd: Dict[str, Any], refreshed_at: str) -> pd.DataFrame:
    game = _extract_game_blob(gtd, event_id)
    if not game:
        return pd.DataFrame()

    drives = game.get("drives") or {}
    rows: List[Dict[str, Any]] = []

    def iter_drive_objs(dr: Any) -> Iterable[Dict[str, Any]]:
        if isinstance(dr, dict):
            for _, v in dr.items():
                if isinstance(v, dict) and "plays" in v:
                    yield v

    for drive in iter_drive_objs(drives):
        plays = drive.get("plays")
        if not isinstance(plays, dict):
            continue

        for _, play in plays.items():
            if not isinstance(play, dict):
                continue

            play_id = _safe_int(play.get("playId") or play.get("play_id"))
            desc = _safe_str(play.get("desc"))

            qtr = _safe_int(play.get("qtr"))
            clock = _safe_str(play.get("time")) or _safe_str(play.get("clock"))

            posteam = _safe_str(play.get("possessionTeam")) or _safe_str(play.get("posteam"))
            defteam = _safe_str(play.get("defensiveTeam")) or _safe_str(play.get("defteam"))

            play_type = _safe_str(play.get("playType")) or _safe_str(play.get("play_type"))
            drive_num = _safe_int(drive.get("driveNum") or drive.get("drive_num") or drive.get("drive"))

            base = {
                "refreshed_at": refreshed_at,
                "season": pd.NA,
                "week": pd.NA,
                "game_id": game_id,
                "game_date": _safe_str(game.get("gameDate")) or _safe_str(game.get("startTime")),

                "posteam": posteam,
                "defteam": defteam,

                "qtr": qtr,
                "time": clock,
                "drive": drive_num,

                "play_id": play_id,
                "desc": desc,

                "play_type": play_type,
                "pass": pd.NA,
                "rush": pd.NA,
                "qb_dropback": pd.NA,
                "sack": pd.NA,
                "interception": pd.NA,
                "fumble_lost": pd.NA,

                "return_team": pd.NA,

                "passer_player_id": pd.NA,
                "passer_player_name": pd.NA,
                "receiver_player_id": pd.NA,
                "receiver_player_name": pd.NA,
                "rusher_player_id": pd.NA,
                "rusher_player_name": pd.NA,
                "kicker_player_id": pd.NA,
                "kicker_player_name": pd.NA,
            }

            base.update(_normalize_scoring_fields_from_play(play))
            rows.append(base)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "game_id" in df.columns:
        df["game_id"] = df["game_id"].astype("string")
    if "play_id" in df.columns:
        df["play_id"] = pd.to_numeric(df["play_id"], errors="coerce").astype("Int64")
    if "qtr" in df.columns:
        df["qtr"] = pd.to_numeric(df["qtr"], errors="coerce").astype("Int64")
    if "drive" in df.columns:
        df["drive"] = pd.to_numeric(df["drive"], errors="coerce").astype("Int64")

    for c in ("touchdown", "safety", "defensive_two_point_conv"):
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("Int64")

    for c in ("field_goal_result", "extra_point_result", "two_point_conv_result"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    for c in ("pass_touchdown", "rush_touchdown"):
        if c in df.columns:
            df[c] = df[c].fillna(False).astype(bool)

    return df


def fetch_live_pbp_for_game_ids(
    *,
    season: int,
    game_ids: List[str],
    timeout_s: float = 12.0,
    retries: int = 2,
) -> Tuple[pd.DataFrame, List[LiveGameMetrics]]:
    refreshed_at = _now_utc_iso()
    all_rows: List[pd.DataFrame] = []
    metrics: List[LiveGameMetrics] = []

    for gid in game_ids:
        gid_s = str(gid).strip()
        event_id = gsis_for_game_id(season, gid_s)

        if not event_id:
            metrics.append(
                LiveGameMetrics(
                    game_id=gid_s,
                    event_id="",
                    pbp_rows=0,
                    max_play_id=None,
                    is_final=False,
                    refreshed_at=refreshed_at,
                    status="error",
                    detail="Could not map game_id to old_game_id via schedules",
                )
            )
            continue

        try:
            gtd = fetch_gtd_json(event_id, timeout_s=timeout_s, retries=retries)
            if not gtd:
                metrics.append(
                    LiveGameMetrics(
                        game_id=gid_s,
                        event_id=event_id,
                        pbp_rows=0,
                        max_play_id=None,
                        is_final=False,
                        refreshed_at=refreshed_at,
                        status="not_loaded_yet",
                        detail="GTD not available yet",
                    )
                )
                continue

            game_blob = _extract_game_blob(gtd, event_id)
            is_final = _infer_is_final(game_blob)

            df = gtd_game_to_pbp_df(gid_s, event_id, gtd, refreshed_at)
            pbp_rows = int(len(df))

            max_play_id: Optional[int] = None
            if pbp_rows > 0 and "play_id" in df.columns:
                mx = pd.to_numeric(df["play_id"], errors="coerce").max(skipna=True)
                if pd.notna(mx):
                    max_play_id = int(mx)

            metrics.append(
                LiveGameMetrics(
                    game_id=gid_s,
                    event_id=event_id,
                    pbp_rows=pbp_rows,
                    max_play_id=max_play_id,
                    is_final=is_final,
                    refreshed_at=refreshed_at,
                    status="ok",
                    detail="",
                )
            )

            if not df.empty:
                all_rows.append(df)

        except Exception as e:
            metrics.append(
                LiveGameMetrics(
                    game_id=gid_s,
                    event_id=event_id,
                    pbp_rows=0,
                    max_play_id=None,
                    is_final=False,
                    refreshed_at=refreshed_at,
                    status="error",
                    detail=str(e),
                )
            )

    pbp_all = pd.concat(all_rows, ignore_index=True) if all_rows else pd.DataFrame()
    return pbp_all, metrics


def metrics_to_dataframe(metrics: List[LiveGameMetrics]) -> pd.DataFrame:
    if not metrics:
        return pd.DataFrame(
            columns=[
                "game_id",
                "event_id",
                "pbp_rows",
                "max_play_id",
                "is_final",
                "refreshed_at",
                "status",
                "detail",
            ]
        )

    df = pd.DataFrame(
        [
            {
                "game_id": m.game_id,
                "event_id": m.event_id,
                "pbp_rows": m.pbp_rows,
                "max_play_id": m.max_play_id,
                "is_final": m.is_final,
                "refreshed_at": m.refreshed_at,
                "status": m.status,
                "detail": m.detail,
            }
            for m in metrics
        ]
    )
    df["game_id"] = df["game_id"].astype("string")
    df["event_id"] = df["event_id"].astype("string")
    return df
