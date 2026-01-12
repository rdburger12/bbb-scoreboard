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
    inactive_seconds: int = 60 * 60,  # 1 hour (unused in daily mode)
) -> RefreshResult:
    """
    Daily mode refresh (nflreadpy / nflverse PBP):

    - Loads nflfastR-style PBP via src.pbp.refresh_pbp.refresh_pbp (which now uses nflreadpy)
    - Derives scoring plays and upserts into cumulative scoring_plays output
    - "changed" is determined by output row count changing (not by max_play_id advance)

    Note: state/freeze logic is not meaningful for this daily model; it is retained only
    to preserve the call signature used by the Streamlit app.
    """
    import time
    from datetime import datetime, timezone

    import pandas as pd

    from src.pbp.refresh_pbp import refresh_pbp
    from src.pbp.refresh_pbp import RefreshResult as PbpRefreshResult  # for type clarity
    from src.refresh import RefreshInProgress  # if your file defines it here; otherwise remove

    playoff_game_ids = [str(x).strip() for x in playoff_game_ids if str(x).strip()]
    games_requested = len(playoff_game_ids)

    t0 = time.time()

    # --- Locking (keep existing behavior) ---
    try:
        lock = FileLock(lock_path, stale_seconds=60 * 5)
        lock.acquire()
    except Exception:
        raise RefreshInProgress()

    try:
        # Count rows before
        old_rows_out = 0
        if cumulative_out_path.exists():
            try:
                old_df = pd.read_csv(cumulative_out_path)
                old_rows_out = int(len(old_df))
            except Exception:
                old_rows_out = 0

        # Run the scoring refresh engine (nflreadpy-backed)
        pbp_result = refresh_pbp(
            season=season,
            week=None,
            game_ids=playoff_game_ids,
            out_path=cumulative_out_path,
            metrics_out_path=None,
        )

        # Determine changed/new_rows based on output row count delta
        rows_out = int(pbp_result.rows_out)
        changed = rows_out != old_rows_out
        new_rows = max(0, rows_out - old_rows_out)

        dt = time.time() - t0

        # Write a lightweight metrics row (keeps your app artifacts intact)
        metrics_out_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
                {
                    "refreshed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
                    "season": season,
                    "games_requested": games_requested,
                    "rows_in": int(pbp_result.rows_in),
                    "rows_scoring": int(pbp_result.rows_scoring),
                    "rows_out": rows_out,
                    "changed": bool(changed),
                    "elapsed_seconds": round(dt, 3),
                }
            ]
        ).to_csv(metrics_out_path, index=False)

        return RefreshResult(
            ok=True,
            message=f"Refresh complete: processed {games_requested} games in {dt:.1f}s.",
            games_requested=games_requested,
            games_refreshed=games_requested,
            games_frozen=0,
            eligible_games=games_requested,
            changed=changed,
            new_rows=new_rows,
        )
    finally:
        try:
            lock.release()
        except Exception:
            pass

