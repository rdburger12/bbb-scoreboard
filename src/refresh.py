from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pandas as pd

from src.pbp.live_pbp import fetch_live_pbp_for_game_ids, metrics_to_dataframe
from src.pbp.refresh_pbp import refresh_pbp


@dataclass(frozen=True)
class RefreshResult:
    ok: bool
    message: str
    games_requested: int = 0
    games_refreshed: int = 0
    games_frozen: int = 0

    # signals for UI
    eligible_games: int = 0
    changed: bool = False
    new_rows: int = 0


class RefreshInProgress(RuntimeError):
    pass


class FileLock:
    """
    Minimal cross-process lock using O_EXCL.
    Works well for Streamlit shared deployments on a single host.
    """

    def __init__(self, lock_path: Path, stale_seconds: int = 60 * 30):
        self.lock_path = lock_path
        self.stale_seconds = stale_seconds
        self._fd: int | None = None

    def acquire(self) -> None:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

        # If lock exists but is stale (e.g., process died), remove it.
        if self.lock_path.exists():
            age_s = time.time() - self.lock_path.stat().st_mtime
            if age_s > self.stale_seconds:
                try:
                    self.lock_path.unlink()
                except OSError:
                    pass

        try:
            self._fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.write(self._fd, f"pid={os.getpid()} time={time.time()}\n".encode("utf-8"))
        except FileExistsError as e:
            raise RefreshInProgress(
                "Refresh already in progress. Wait ~10 seconds then hit refresh on your browser"
            ) from e

    def release(self) -> None:
        try:
            if self._fd is not None:
                os.close(self._fd)
        finally:
            self._fd = None
            try:
                if self.lock_path.exists():
                    self.lock_path.unlink()
            except OSError:
                pass

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()


def _atomic_write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    df.to_csv(tmp, index=False)
    tmp.replace(path)  # atomic rename on typical filesystems


def _read_state(state_path: Path) -> pd.DataFrame:
    if not state_path.exists():
        return pd.DataFrame(
            columns=[
                "season",
                "game_id",
                "first_seen_at",
                "last_attempt_at",
                "last_success_at",
                "last_max_play_id",
                "last_new_pbp_at",
                "no_new_pbp_streak",
                "is_frozen",
                "freeze_reason",
            ]
        )
    return pd.read_csv(state_path, dtype={"game_id": "string"})


def _now_utc_iso() -> str:
    # Example: 2026-01-07T02:14:05Z
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _to_int_or_none(x) -> int | None:
    try:
        if pd.isna(x):
            return None
        return int(x)
    except Exception:
        return None


def _parse_utc_iso(ts: str | None) -> float | None:
    if not ts:
        return None
    s = str(ts).strip()
    try:
        # Handle trailing Z
        if s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(s)
        return dt.timestamp()
    except Exception:
        return None


def _should_freeze_inactive(last_new_pbp_at: str | None, inactive_seconds: int) -> bool:
    last_ts = _parse_utc_iso(last_new_pbp_at)
    if last_ts is None:
        return False
    return (time.time() - last_ts) >= inactive_seconds


def _select_games_to_refresh(
    playoff_game_ids: list[str],
    state_df: pd.DataFrame,
) -> list[str]:
    if state_df.empty:
        return playoff_game_ids

    frozen = set(
        state_df.loc[state_df["is_frozen"].fillna(False).astype(bool), "game_id"]
        .astype("string")
        .tolist()
    )
    return [gid for gid in playoff_game_ids if gid not in frozen]


def refresh_playoff_games(
    *,
    season: int,
    playoff_game_ids: Iterable[str],
    # kept for backward compatibility with existing call sites; unused
    rscript_path: str = "Rscript",
    refresh_script_path: str = "r/refresh_pbp.R",
    cumulative_out_path: Path,
    metrics_out_path: Path,
    state_path: Path,
    lock_path: Path,
    inactive_seconds: int = 60 * 60,  # 1 hour
) -> RefreshResult:
    """
    Refresh scoring plays for a subset of playoff games:
      - skips games frozen by prior runs
      - freezes games when is_final==TRUE
      - freezes games after inactive_seconds without PBP advance (max_play_id)

    "changed" is determined by pbp advance (max_play_id), not by row-counting the CSV.

    Live mode:
      - per-game metrics are computed from live GameCenter GTD
      - scoring plays are refreshed via src.pbp.refresh_pbp.refresh_pbp when any game has live PBP loaded
    """

    playoff_game_ids = [str(x).strip() for x in playoff_game_ids if str(x).strip()]
    games_requested = len(playoff_game_ids)

    if games_requested == 0:
        return RefreshResult(
            ok=True,
            message="No playoff game_ids provided.",
            games_requested=0,
            games_refreshed=0,
            games_frozen=0,
            eligible_games=0,
            changed=False,
            new_rows=0,
        )

    with FileLock(lock_path):
        state_df = _read_state(state_path)
        if not state_df.empty and "game_id" in state_df.columns:
            state_df["game_id"] = state_df["game_id"].astype("string")

        # Baseline for "did anything advance?"
        prev_max_by_gid: dict[str, int | None] = {}
        if (not state_df.empty) and {"game_id", "last_max_play_id"}.issubset(state_df.columns):
            for _, r in state_df.iterrows():
                gid = str(r["game_id"])
                prev_max_by_gid[gid] = _to_int_or_none(r.get("last_max_play_id"))

        to_refresh = _select_games_to_refresh(playoff_game_ids, state_df)
        eligible_games = len(to_refresh)

        if eligible_games == 0:
            games_frozen = (
                int(state_df["is_frozen"].fillna(False).astype(bool).sum())
                if (not state_df.empty and "is_frozen" in state_df.columns)
                else 0
            )
            return RefreshResult(
                ok=True,
                message="Nothing to refresh - scoreboard reflects final scores",
                games_requested=games_requested,
                games_refreshed=0,
                games_frozen=games_frozen,
                eligible_games=0,
                changed=False,
                new_rows=0,
            )

        # Prepare metrics output
        metrics_out_path.parent.mkdir(parents=True, exist_ok=True)

        t0 = time.time()
        now = _now_utc_iso()

        # 1) Fetch live PBP (normalized) + per-game metrics
        pbp_df, metrics_list = fetch_live_pbp_for_game_ids(season=season, game_ids=to_refresh)
        metrics = metrics_to_dataframe(metrics_list)

        # Force refreshed_at to "now" (so state uses a consistent timestamp)
        if not metrics.empty and "refreshed_at" in metrics.columns:
            metrics["refreshed_at"] = now

        # Write metrics_out for the app/state machine
        metrics.to_csv(metrics_out_path, index=False)

        # Determine whether any live PBP is loaded for these games
        any_loaded = (pbp_df is not None) and (not pbp_df.empty)

        # 2) Only run the scoring refresh engine if any game has PBP loaded.
        #    This avoids overwriting outputs with empties on pre-game/404.
        if any_loaded:
            try:
                refresh_pbp(
                    season=season,
                    week=None,
                    game_ids=to_refresh,
                    out_path=cumulative_out_path,
                    metrics_out_path=None,  # per-game metrics are authored here
                )
            except Exception as e:
                dt = time.time() - t0
                return RefreshResult(
                    ok=False,
                    message=f"Refresh failed.\n\nElapsed: {dt:.1f}s\n\n{e}",
                    games_requested=games_requested,
                    games_refreshed=0,
                    games_frozen=0,
                    eligible_games=eligible_games,
                    changed=False,
                    new_rows=0,
                )

        dt = time.time() - t0

        # Validate metrics schema
        required = {"game_id", "max_play_id", "is_final", "refreshed_at"}
        missing = sorted(required - set(metrics.columns))
        if missing:
            return RefreshResult(
                ok=False,
                message=f"metrics_out missing columns: {missing}",
                games_requested=games_requested,
                games_refreshed=eligible_games,
                games_frozen=0,
                eligible_games=eligible_games,
                changed=False,
                new_rows=0,
            )

        # Determine "changed" by PBP advance
        advanced_any = False
        for _, row in metrics.iterrows():
            gid = str(row["game_id"])
            new_max = _to_int_or_none(row["max_play_id"])
            prev_max = prev_max_by_gid.get(gid)

            if new_max is None:
                continue
            if prev_max is None or new_max > prev_max:
                advanced_any = True
                break

        changed = advanced_any
        new_rows = 0

        # Ensure state has expected columns
        state_df = state_df.copy()
        if state_df.empty:
            state_df = _read_state(state_path)

        state_df["game_id"] = state_df["game_id"].astype("string")
        idx = {gid: i for i, gid in enumerate(state_df["game_id"].tolist())}

        games_frozen_now = 0

        for _, row in metrics.iterrows():
            gid = str(row["game_id"])
            max_play_id = _to_int_or_none(row["max_play_id"])
            is_final = bool(row["is_final"]) if not pd.isna(row["is_final"]) else False

            status = str(row.get("status", "")).strip().lower()
            not_loaded_yet = status in {"not_loaded_yet", "not_loaded", "unavailable"}

            refreshed_at_utc = now

            if gid not in idx:
                state_df.loc[len(state_df)] = {
                    "season": season,
                    "game_id": gid,
                    "first_seen_at": refreshed_at_utc,
                    "last_attempt_at": now,
                    "last_success_at": refreshed_at_utc,
                    "last_max_play_id": max_play_id if max_play_id is not None else None,
                    "last_new_pbp_at": refreshed_at_utc if max_play_id is not None else None,
                    "no_new_pbp_streak": 0,
                    "is_frozen": False,
                    "freeze_reason": "",
                }
                idx[gid] = len(state_df) - 1

                if not_loaded_yet:
                    # Don’t streak/freeze games that aren't loaded yet
                    continue

            i = idx[gid]
            state_df.at[i, "season"] = season
            state_df.at[i, "last_attempt_at"] = now
            state_df.at[i, "last_success_at"] = refreshed_at_utc

            prev_max = _to_int_or_none(state_df.at[i, "last_max_play_id"])
            streak = _to_int_or_none(state_df.at[i, "no_new_pbp_streak"]) or 0

            advanced = False
            if max_play_id is not None and (prev_max is None or max_play_id > prev_max):
                advanced = True

            if advanced:
                state_df.at[i, "last_max_play_id"] = max_play_id
                state_df.at[i, "last_new_pbp_at"] = refreshed_at_utc
                state_df.at[i, "no_new_pbp_streak"] = 0
            elif not_loaded_yet:
                # Do not penalize games with no pbp loaded yet
                pass
            else:
                state_df.at[i, "no_new_pbp_streak"] = streak + 1

            if not_loaded_yet:
                continue

            # Freeze logic
            if not bool(state_df.at[i, "is_frozen"]):
                if is_final:
                    state_df.at[i, "is_frozen"] = True
                    state_df.at[i, "freeze_reason"] = "final"
                    games_frozen_now += 1
                else:
                    last_new = state_df.at[i, "last_new_pbp_at"]
                    streak2 = _to_int_or_none(state_df.at[i, "no_new_pbp_streak"]) or 0
                    if streak2 >= 2 and _should_freeze_inactive(
                        last_new_pbp_at=str(last_new) if not pd.isna(last_new) else None,
                        inactive_seconds=inactive_seconds,
                    ):
                        state_df.at[i, "is_frozen"] = True
                        state_df.at[i, "freeze_reason"] = "inactive_timeout"
                        games_frozen_now += 1

        _atomic_write_csv(state_df, state_path)

        # If all games are not_loaded_yet, treat as "no new plays"
        all_not_loaded = False
        if "status" in metrics.columns and not metrics.empty:
            s = metrics["status"].astype(str).str.lower().fillna("")
            all_not_loaded = bool((s == "not_loaded_yet").all())

        if all_not_loaded and not changed:
            msg = "Up to date — no play-by-play available yet."
        else:
            msg = f"Refresh complete: attempted {eligible_games} games in {dt:.1f}s; froze {games_frozen_now} games."

        return RefreshResult(
            ok=True,
            message=msg,
            games_requested=games_requested,
            games_refreshed=eligible_games,
            games_frozen=games_frozen_now,
            eligible_games=eligible_games,
            changed=changed,
            new_rows=new_rows,
        )
