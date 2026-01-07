from __future__ import annotations

import os
import subprocess
import time
from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import pandas as pd

@dataclass(frozen=True)
class RefreshResult:
    ok: bool
    message: str
    games_requested: int = 0
    games_refreshed: int = 0
    games_frozen: int = 0

    # NEW: signals for UI
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
            raise RefreshInProgress("Refresh already in progress. Wait ~10 seconds then hit refresh on your browser") from e

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

def _safe_rowcount_csv(path: Path) -> int:
    if not path.exists():
        return 0
    try:
        return len(pd.read_csv(path))
    except Exception:
        return 0

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
        if not state_df.empty and {"game_id", "last_max_play_id"}.issubset(state_df.columns):
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
        if metrics_out_path.exists():
            metrics_out_path.unlink()

        cmd = [
            rscript_path,
            refresh_script_path,
            "--game_ids",
            ",".join(to_refresh),
            "--out",
            str(cumulative_out_path),
            "--metrics_out",
            str(metrics_out_path),
        ]

        t0 = time.time()
        proc = subprocess.run(cmd, capture_output=True, text=True)
        dt = time.time() - t0

        if proc.returncode != 0:
            msg = (
                "Refresh failed.\n\n"
                f"Command: {' '.join(cmd)}\n"
                f"Elapsed: {dt:.1f}s\n\n"
                f"STDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
            )
            return RefreshResult(
                ok=False,
                message=msg,
                games_requested=games_requested,
                games_refreshed=0,
                games_frozen=0,
                eligible_games=eligible_games,
                changed=False,
                new_rows=0,
            )

        if not metrics_out_path.exists():
            msg = (
                "Refresh succeeded, but metrics_out was not written.\n\n"
                f"Command: {' '.join(cmd)}\n"
                f"Elapsed: {dt:.1f}s\n\n"
                f"STDOUT:\n{proc.stdout}\n\nSTDERR:\n{proc.stderr}"
            )
            return RefreshResult(
                ok=False,
                message=msg,
                games_requested=games_requested,
                games_refreshed=eligible_games,
                games_frozen=0,
                eligible_games=eligible_games,
                changed=False,
                new_rows=0,
            )

        metrics = pd.read_csv(metrics_out_path, dtype={"game_id": "string"})

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
        for _, m in metrics.iterrows():
            gid = str(m["game_id"])
            new_max = _to_int_or_none(m["max_play_id"])
            prev_max = prev_max_by_gid.get(gid)

            if new_max is None:
                continue
            if prev_max is None or new_max > prev_max:
                advanced_any = True
                break

        changed = advanced_any
        new_rows = 0  # we no longer rely on counting CSV rows

        now = _now_utc_iso()

        # Ensure state has the expected columns
        state_df = state_df.copy()
        if state_df.empty:
            state_df = _read_state(state_path)

        state_df["game_id"] = state_df["game_id"].astype("string")
        idx = {gid: i for i, gid in enumerate(state_df["game_id"].tolist())}

        games_frozen_now = 0

        for _, m in metrics.iterrows():
            gid = str(m["game_id"])
            max_play_id = _to_int_or_none(m["max_play_id"])
            is_final = bool(m["is_final"]) if not pd.isna(m["is_final"]) else False

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
            else:
                i = idx[gid]
                state_df.at[i, "season"] = season
                state_df.at[i, "last_attempt_at"] = now
                state_df.at[i, "last_success_at"] = refreshed_at_utc

                prev_max = _to_int_or_none(state_df.at[i, "last_max_play_id"])
                streak = _to_int_or_none(state_df.at[i, "no_new_pbp_streak"]) or 0

                advanced = False
                if max_play_id is not None:
                    if prev_max is None or max_play_id > prev_max:
                        advanced = True

                if advanced:
                    state_df.at[i, "last_max_play_id"] = max_play_id
                    state_df.at[i, "last_new_pbp_at"] = refreshed_at_utc
                    state_df.at[i, "no_new_pbp_streak"] = 0
                else:
                    state_df.at[i, "no_new_pbp_streak"] = streak + 1

            # Freeze logic
            i = idx[gid]
            if not bool(state_df.at[i, "is_frozen"]):
                if is_final:
                    state_df.at[i, "is_frozen"] = True
                    state_df.at[i, "freeze_reason"] = "final"
                    games_frozen_now += 1
                else:
                    last_new = state_df.at[i, "last_new_pbp_at"]
                    streak = _to_int_or_none(state_df.at[i, "no_new_pbp_streak"]) or 0
                    if streak >= 2 and _should_freeze_inactive(
                        last_new_pbp_at=str(last_new) if not pd.isna(last_new) else None,
                        inactive_seconds=inactive_seconds,
                    ):
                        state_df.at[i, "is_frozen"] = True
                        state_df.at[i, "freeze_reason"] = "inactive_timeout"
                        games_frozen_now += 1

        _atomic_write_csv(state_df, state_path)

        return RefreshResult(
            ok=True,
            message=f"Refresh complete: attempted {eligible_games} games in {dt:.1f}s; froze {games_frozen_now} games.",
            games_requested=games_requested,
            games_refreshed=eligible_games,
            games_frozen=games_frozen_now,
            eligible_games=eligible_games,
            changed=changed,
            new_rows=new_rows,
        )



