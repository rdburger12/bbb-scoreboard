# `src/` module guide

This directory contains the Python modules that power the Streamlit app and the supporting data-refresh / scoring utilities. Think of `src/` as “the code the app imports.”

The sections below describe what each file is responsible for, which other `src/` modules it references, and where there is non-trivial logic that is easy to break later.

---

## Top-level modules (`src/*.py`)

### `src/app_io.py`

**Responsibility**
- Streamlit-friendly I/O helpers for reading CSVs and normalizing common columns.
- Provides “safe read” behavior (missing file → empty dataframe; parse issues → a dataframe that carries error context).

**What to look for / complexity**
- Uses Streamlit caching (`st.cache_data`) to avoid re-reading the same file repeatedly.
- Includes column normalization that the UI and scoring logic implicitly rely on (IDs as strings, numeric columns coerced cleanly, etc.). If you change column names or ID formats upstream, this is a common place to adjust.

**References (internal)**
- None.

---

### `src/gameset.py`

**Responsibility**
- Defines the `GameSet` concept (mode + season + optional week bound).
- Provides `load_game_ids(...)` to derive the set of game IDs for a given operating mode:
  - `playoffs`: uses an explicit list of playoff game IDs
  - `regular_season_dev`: derives game IDs from scoring plays filtered by season/week

**What to look for / complexity**
- The “mode” switch is a core control point: if you introduce new modes (e.g., full regular season, preseason), this is the natural home for that decision logic.
- Validations here are intentionally strict (required columns, week bounds). Most runtime errors in “which games are included?” flow through this module.

**References (internal)**
- None.

---

### `src/ingest.py`

**Responsibility**
- Thin wrapper for running the R refresh pipeline (`r/refresh_pbp.R`) for a single week via `subprocess`.

**What to look for / complexity**
- This is one of the few places in `src/` that shells out to external tooling (`Rscript`). If you change how refresh is orchestrated (move to Python-only, add Docker, change CLI args), this file will likely change.
- Captures stdout/stderr so calling code can surface refresh failures cleanly.

**References (internal)**
- None.

---

### `src/refresh.py`

**Responsibility**
- Higher-level refresh orchestration for playoff data:
  - selects which games to refresh
  - prevents concurrent runs (file lock)
  - maintains per-game “frozen” state (final games stop refreshing)
  - writes cumulative outputs + metrics outputs atomically

**What to look for / complexity**
- This is one of the most complex modules in the repo.
- It implements operational safety mechanisms:
  - lock files with stale-lock cleanup
  - “freeze” semantics (final games and inactive games)
  - “changed” detection based on play-by-play advancement (not row counts)
  - atomic CSV writes to avoid partial reads by the app
- If you change refresh frequency, data sources, or add new output artifacts, review this module carefully.

**References (internal)**
- `src.pbp.live_pbp`
- `src.pbp.refresh_pbp`

---

### `src/scoreboard.py`

**Responsibility**
- Builds the core “scoreboard dataset” used by the UI:
  - validates required columns
  - canonicalizes team abbreviations
  - merges team/position totals into draft structure
  - enforces stable sorting (owner/round/slot)

**What to look for / complexity**
- This file encodes the “dataset contract” between processed draft data and computed totals.
- Validation helpers (`_require_columns`, `_assert_unique_key`, etc.) are the guardrails that prevent silent UI corruption.
- If you change the shape of draft picks, roster slots, or position rules, this is a common update point.

**References (internal)**
- `src.domain.teams`

---

### `src/ui_sections.py`

**Responsibility**
- Streamlit rendering helpers for major UI sections:
  - scoreboard grid/table rendering
  - event feed rendering
- Centralizes display logic so `app/app.py` can stay relatively small.

**What to look for / complexity**
- This module is UI-heavy and includes:
  - filtering and presentation logic
  - data reshaping for display
  - CSS / layout overrides
- Most “why does the UI look wrong?” or “why is this view slow?” issues land here.
- If performance becomes a concern, this is a primary target for caching, pre-aggregation, or reducing expensive per-render transforms.

**References (internal)**
- None (UI code operates on dataframes passed in).

---

### `src/__init__.py`

**Responsibility**
- Package marker for `src`.

**References (internal)**
- None.

---

## Domain modules (`src/domain/*`)

### `src/domain/teams.py`

**Responsibility**
- Canonicalization utilities for team abbreviations and team columns.

**What to look for / complexity**
- Small but high-impact: team naming mismatches propagate everywhere (draft data, PBP, totals).
- If you see “team not found” or duplicated team rows due to naming drift, this is the first place to check.

**References (internal)**
- None.

---

## Play-by-play modules (`src/pbp/*`)

### `src/pbp/live_pbp.py`

**Responsibility**
- Fetches and normalizes live play-by-play data from NFL endpoints.
- Converts raw game blobs into structured PBP dataframes.
- Produces game-level metrics in a consistent format.

**What to look for / complexity**
- This is one of the most complex modules in the repo.
- It handles:
  - HTTP requests and response validation
  - schema drift (fields missing or renamed)
  - conservative “final” detection
  - normalization of nested play structures
- If you see refresh failures due to upstream feed changes, this is the likely root cause.

**References (internal)**
- `src.pbp.schedule`

---

### `src/pbp/refresh_pbp.py`

**Responsibility**
- CLI-friendly refresh entry point for pulling PBP and writing outputs.
- Loads environment variables (via dotenv), resolves output paths, and optionally writes metrics outputs.
- Calls into:
  - live PBP fetch
  - scoring play derivation
  - logging/status writing
  - position enrichment

**What to look for / complexity**
- This is the “wiring” module for a full PBP refresh run.
- It includes `argparse` handling (season, week, game IDs, output locations).
- If you change where outputs land or how refresh is invoked, changes often start here.

**References (internal)**
- `src.pbp.live_pbp`
- `src.pbp.logging`
- `src.pbp.paths`
- `src.pbp.positions`
- `src.pbp.scoring_plays`
- `src.pbp.upsert`

---

### `src/pbp/schedule.py`

**Responsibility**
- Resolves mappings between various game identifiers (game_id, event_id, gsis, etc.).
- Provides helpers to resolve game IDs for a given week from schedule data.

**What to look for / complexity**
- ID mapping is subtle: if live fetches fail because the wrong event ID is used, this is a prime suspect.
- Keep this module consistent with whatever upstream schedule source you are using.

**References (internal)**
- None.

---

### `src/pbp/scoring_plays.py` and `src/pbp/scoring.py`

**Responsibility**
- Derives “scoring plays” from PBP inputs using a shared `ScoringPlaysConfig`.

**What to look for / complexity**
- These files are the core of “how plays become events.”
- If scoring semantics change, or you need additional event types, these are the critical modules.
- The transformation logic is easy to break if upstream PBP fields change (see `live_pbp.py`).

**References (internal)**
- `src.pbp.utils`

---

### `src/pbp/positions.py`

**Responsibility**
- Ensures player position data is present and normalized for downstream scoring / display.

**What to look for / complexity**
- Position normalization is often “quietly wrong” when upstream IDs or data sources change.
- If you see players falling into unexpected buckets, this is where the normalization is applied.

**References (internal)**
- None.

---

### `src/pbp/paths.py`

**Responsibility**
- Centralizes filesystem path conventions for PBP refresh outputs via a `Paths` object.

**What to look for / complexity**
- This is a coordination point between refresh scripts and the app. If you reorganize outputs under `data/processed`, update this module so all callers stay consistent.

**References (internal)**
- None.

---

### `src/pbp/logging.py`

**Responsibility**
- Writes refresh logs and status records (used to track refresh outcomes and freshness).

**What to look for / complexity**
- Small but operationally important: this is what you read when diagnosing “did refresh run?” and “what failed?”.

**References (internal)**
- None.

---

### `src/pbp/upsert.py`

**Responsibility**
- “Upsert” utilities to merge the latest refresh outputs into an existing dataset (e.g., latest wins / latest rows).

**What to look for / complexity**
- Pay attention to key columns and sort order assumptions. Upserts typically fail silently when keys drift.

**References (internal)**
- None.

---

### `src/pbp/utils.py`

**Responsibility**
- Small dataframe / column helpers used throughout the PBP modules (type coercion, default columns, etc.).

**What to look for / complexity**
- Low complexity, but heavily reused. Changes here can have wide impact.

**References (internal)**
- None.

---

### `src/pbp/__init___.py`

**Responsibility**
- Intended to mark `src/pbp` as a package.

**Important note**
- This file is named `__init___.py` (three underscores). Python expects `__init__.py`.
- If you want `pbp` to behave like a standard package, rename this to `__init__.py`.

**References (internal)**
- None.

---

## Scoring modules (`src/scoring/*`)

### `src/scoring/io.py`

**Responsibility**
- I/O and normalization helpers for scoring inputs:
  - position normalization
  - player ID cleanup
  - loading player positions

**What to look for / complexity**
- This is where raw identifiers are cleaned up for consistent joins. If joins start failing, this is a common fix point.

**References (internal)**
- None.

---

### `src/scoring/engine.py`

**Responsibility**
- Core scoring engine:
  - defines scoring rules (`ScoreRules`)
  - converts events into points
  - aggregates totals by team/position and other scopes

**What to look for / complexity**
- This is a critical, high-impact module.
- It contains the “rules of the world” for how events map to points and how scopes are applied.
- If you change scoring rules or add new event types, treat edits here as high risk and validate carefully.

**References (internal)**
- `src.scoring.io`

---

### `src/scoring/__init__.py`

**Responsibility**
- Package marker for scoring.
- Re-exports commonly used scoring entry points for convenience.

**References (internal)**
- `src.scoring.engine`
- `src.scoring.io`

---
