#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv

PICK_RE = re.compile(r"^[A-Za-z]{2,4}\s+(QB|RB|WR|TE|K|OTH)$")
NUM_RE = re.compile(r"^\d+(\.0)?$")


def is_pick_cell(x: object) -> bool:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return False
    return bool(PICK_RE.match(str(x).strip()))


def is_numeric_cell(x: object) -> bool:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return False
    return bool(NUM_RE.match(str(x).strip()))


def row_pick_score(row: pd.Series) -> int:
    return int(sum(is_pick_cell(v) for v in row.values))


def row_numeric_score(row: pd.Series) -> int:
    return int(sum(is_numeric_cell(v) for v in row.values))


def parse_team_pos(cell: str) -> tuple[str, str]:
    s = str(cell).strip()
    if not PICK_RE.match(s):
        raise ValueError(f"Invalid pick format '{cell}' (expected 'TEAM POS' like 'BUF QB')")
    team, pos = s.split()
    return team.upper(), pos.upper()


def main() -> None:
    ROOT = Path(__file__).resolve().parents[1]  # bbb_scoreboard/
    load_dotenv(ROOT / ".env")

    parser = argparse.ArgumentParser(description="Transform BBB draft CSV into normalized draft_picks table")
    parser.add_argument("--season", type=int, help="Season year (overrides BBB_SEASON)")
    parser.add_argument("--in", dest="in_path", type=str, default=None, help="Optional input CSV path")
    args = parser.parse_args()

    season = args.season
    if season is None:
        env_season = os.getenv("BBB_SEASON")
        season = int(env_season) if env_season else None
    if season is None:
        raise RuntimeError("Season not specified. Provide --season or set BBB_SEASON in .env")

    RAW = ROOT / "data" / "raw" / "drafts"
    CONFIG = ROOT / "data" / "config"
    RAW.mkdir(parents=True, exist_ok=True)
    CONFIG.mkdir(parents=True, exist_ok=True)

    default_in = RAW / f"Big Burger Bet {season}-{season+1}.csv"
    in_path = Path(args.in_path) if args.in_path else default_in
    out_path = CONFIG / f"draft_picks_{season}.csv"

    if not in_path.exists():
        raise FileNotFoundError(f"Missing raw draft file: {in_path}")

    df = pd.read_csv(in_path)
    owners = df.columns.tolist()

    # Detect (slot_row -> pick_row) pairs: numeric-heavy row followed by pick-heavy row.
    pairs: list[tuple[int, int]] = []
    for i in range(len(df) - 1):
        if row_numeric_score(df.iloc[i]) >= 10 and row_pick_score(df.iloc[i + 1]) >= 10:
            pairs.append((i, i + 1))

    if not pairs:
        raise RuntimeError(
            "Could not detect slot/pick row pairs. Expected numeric row followed by pick row."
        )

    first_slot_row_idx = pairs[0][0]

    records: list[dict] = []
    for round_no, (slot_row_idx, pick_row_idx) in enumerate(pairs, start=1):
        slot_row = df.iloc[slot_row_idx]
        pick_row = df.iloc[pick_row_idx]

        for owner in owners:
            cell = pick_row[owner]
            if pd.isna(cell) or str(cell).strip() == "":
                continue

            # Skip any non-pick garbage defensively (should not happen if detection worked)
            if not is_pick_cell(cell):
                continue

            team, position = parse_team_pos(str(cell))

            owner_id_raw = df.iloc[first_slot_row_idx][owner]
            slot_raw = slot_row[owner]

            if not is_numeric_cell(owner_id_raw):
                raise ValueError(f"owner_id not numeric for owner='{owner}': {owner_id_raw}")
            if not is_numeric_cell(slot_raw):
                raise ValueError(f"slot not numeric for owner='{owner}', round={round_no}: {slot_raw}")

            records.append(
                {
                    "season": season,
                    "owner_id": int(float(str(owner_id_raw).strip())),
                    "owner": owner,
                    "round": round_no,
                    "slot": int(float(str(slot_raw).strip())),
                    "team": team,
                    "position": position,
                }
            )

    out = pd.DataFrame(records)

    expected = len(owners) * len(pairs)
    if len(out) != expected:
        print(f"WARNING: expected {expected} rows (owners*rounds), got {len(out)}")

    out.to_csv(out_path, index=False)
    print(f"Wrote {len(out)} rows → {out_path}")


if __name__ == "__main__":
    main()
