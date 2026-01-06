import sys
import pandas as pd
from pathlib import Path

INPUT = Path("data/raw/drafts/Big Burger Bet 2024-2025.csv")
OUTPUT = Path("data/config/draft_picks.csv")

if not INPUT.exists():
    raise FileNotFoundError(f"Input file not found: {INPUT}")

df = pd.read_csv(INPUT)

# Expect exactly 14 owners
owners = list(df.columns[:14])
if len(owners) != 14:
    raise ValueError(f"Expected 14 owners, found {len(owners)}")

# Layout assumptions (documented + validated)
pick_rows = [1, 4, 7, 10, 13, 16]
slot_rows = [0, 3, 6, 9, 12, 15]

records = []

for round_no, (pick_row, slot_row) in enumerate(zip(pick_rows, slot_rows), start=1):
    for owner in owners:
        cell = df.loc[pick_row, owner]

        if pd.isna(cell):
            continue

        parts = str(cell).strip().split()
        if len(parts) != 2:
            raise ValueError(
                f"Invalid pick format at owner='{owner}', round={round_no}: '{cell}'"
            )

        team, position = parts
        team = team.upper()
        position = position.upper()

        records.append({
            "owner_id": int(df.loc[0, owner]),
            "owner": owner,
            "round": round_no,
            "slot": int(df.loc[slot_row, owner]),
            "team": team,
            "position": position,
        })

draft_picks = pd.DataFrame(records)

# Validation checks
expected_rows = 14 * 6
if len(draft_picks) != expected_rows:
    raise ValueError(
        f"Expected {expected_rows} picks, found {len(draft_picks)}"
    )

required_cols = {"owner_id", "owner", "round", "slot", "team", "position"}
if not required_cols.issubset(draft_picks.columns):
    raise ValueError(f"Missing required columns: {required_cols - set(draft_picks.columns)}")

OUTPUT.parent.mkdir(parents=True, exist_ok=True)
draft_picks.to_csv(OUTPUT, index=False)

print(f"Wrote {len(draft_picks)} draft picks → {OUTPUT}")
