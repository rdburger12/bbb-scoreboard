# Scripts

## Purpose of the `scripts/` directory

The `scripts/` directory contains command-line utilities used to prepare and refresh data consumed by the Streamlit app. These scripts are intended to be run manually and write their outputs to disk.

If the app appears empty or incomplete, it usually means one or more of these scripts has not been run, or the expected output files are missing.

---

## Core scripts

### transform_draft_csv.py

What it does  
Transforms the raw post-draft CSV into processed draft and configuration data used by the rest of the system.

When to run it  
Run this after the draft is complete and the draft board CSV has been placed in:

data/raw/drafts/

Season selection  
By default, the script determines the season using the `BBB_SEASON` environment variable. You can override this using a command-line argument.

What it reads  
- Draft board CSV files from data/raw/drafts/

What it writes  
- data/config/draft_picks_{BBB_SEASON}.csv

Re-run behavior  
Safe to re-run. Existing generated outputs will be overwritten.

Command-line arguments  

--season <YEAR>  
Overrides the season derived from `BBB_SEASON`.

--in <PATH>  
Explicitly specifies the input draft CSV file. If not provided, the script looks for a default file name based on the season.

Examples  

Run using the environment variable:
python scripts/transform_draft_csv.py

Run for a specific season:
python scripts/transform_draft_csv.py --season 2025

Run with an explicit input file:
python scripts/transform_draft_csv.py --season 2025 --in path/to/draft.csv

---

## Helper scripts

### build_playoff_game_ids.R

What it does  
Generates the set of playoff game IDs for the specified season.

When it’s useful  
Use this script if you do not already know the playoff game IDs for the current year. It is a helper and is not required for normal, non-playoff app usage.

Season selection  
The script determines the season using the following precedence:
1. The --season command-line argument
2. The `BBB_SEASON` environment variable

What it writes  
- data/config/playoff_game_ids_{BBB_SEASON}.csv

Re-run behavior  
Safe to re-run. Existing output files will be overwritten.

Command-line arguments  

--season <YEAR>  
Specifies the season for which playoff game IDs should be generated.

Examples  

Run using the environment variable:
Rscript build_playoff_game_ids.R

Run for a specific season:
Rscript build_playoff_game_ids.R --season 2025

---

## General notes

Run scripts from the project root so relative paths resolve correctly.

Scripts write outputs into the data/ directory and do not interact with the Streamlit app directly.

The app assumes these scripts have already completed successfully and that the expected output files exist on disk.
