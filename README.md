# Streamlit Scoreboard App

## What this app is

This is a Streamlit application for viewing a fantasy football scoreboard and related event data. The app is read-only and is intended to display results that have already been computed and written to disk.

The app does not perform scoring, simulations, or data refreshes at runtime. Its responsibility is limited to loading prepared data artifacts and presenting them in a clear, consistent way.

---

## Draft data prerequisite

This app assumes the draft has already been completed.

A draft board CSV must be placed in:

data/raw/drafts/

The app does not read raw draft files directly. The draft data must first be transformed into the format expected by the rest of the system.

---

## Preparing draft data

Before running the app, you must run the draft transformation script:

python scripts/transform_draft_csv.py

This script reads the raw draft board CSV from data/raw/drafts/ and produces the processed draft data used by the app and downstream scoring logic.

The app assumes this step has already been completed successfully.

---

## Typical workflow

1. Complete the draft.
2. Place the draft CSV in data/raw/drafts/.
3. Run scripts/transform_draft_csv.py.
4. Set the season environment variable.
5. Start the Streamlit app.

---

## Environment configuration

The app expects an environment variable named `BBB_SEASON` to be set to the starting year of the season.

The value should be the four-digit year in which the season begins. For example:

- The 2025–2026 season uses `BBB_SEASON=2025`

Example:

export BBB_SEASON=2025

This value is used to determine which season’s data artifacts the app loads.

---

## Running the app

From the project root:

pip install -r requirements.txt  
streamlit run app/app.py

The app will load available data artifacts from disk and render the scoreboard UI.
