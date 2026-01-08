#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(nflfastR)
  library(future)
  library(dotenv)
  library(stringr)
})

# -------------------------
# Bootstrap
# -------------------------
dotenv::load_dot_env()

season <- as.integer(Sys.getenv("BBB_SEASON"))
if (is.na(season)) {
  stop("BBB_SEASON not set. Check .env file.")
}

# -------------------------
# CLI args
# -------------------------
args <- commandArgs(trailingOnly = TRUE)

get_flag_value <- function(flag) {
  idx <- which(args == flag)
  if (length(idx) == 0 || idx[1] >= length(args)) {
    return(NA)
  }
  args[idx[1] + 1]
}

game_ids_arg <- get_flag_value("--game_ids")
metrics_out <- get_flag_value("--metrics_out")

week <- as.integer(get_flag_value("--week"))
game_ids_arg <- get_flag_value("--game_ids")
out_path <- get_flag_value("--out")
metrics_out <- get_flag_value("--metrics_out")

if (is.na(week) && (is.na(game_ids_arg) || game_ids_arg == "")) {
  stop(
    "Usage: Rscript r/refresh_pbp.R (--week <WEEK> | --game_ids <id1,id2,...>) [--out <PATH>] [--metrics_out <PATH>]"
  )
}

# -------------------------
# Load modules
# -------------------------
source("r/lib/paths.R")
source("r/lib/logging.R")
source("r/lib/schedule.R")
source("r/lib/scoring_plays.R")
source("r/lib/upsert.R")
source("r/lib/positions.R")

# -------------------------
# Paths & setup
# -------------------------
paths <- get_paths(out_path = out_path, season = season)

plan(multisession, workers = 4)

refreshed_at <- as.character(Sys.time())
t0 <- Sys.time()

cat("BBB refresh | season:", season, "| week:", week, "\n")
cat("Output:", paths$out_path, "\n")

# -------------------------
# Resolve game_ids
# -------------------------
t_schedule0 <- Sys.time()

if (!is.na(game_ids_arg) && game_ids_arg != "") {
  game_ids <- strsplit(game_ids_arg, ",")[[1]]
  game_ids <- stringr::str_trim(game_ids)
  games_requested <- length(game_ids)
  t_schedule_s <- as.numeric(difftime(Sys.time(), t_schedule0, units = "secs"))
  cat("Using provided game_ids:", games_requested, "\n")
} else {
  game_ids <- resolve_game_ids_for_week(season, week)
  games_requested <- length(game_ids)
  t_schedule_s <- as.numeric(difftime(Sys.time(), t_schedule0, units = "secs"))
  cat("Resolved game_ids:", games_requested, "\n")
}


# -------------------------
# Load existing cumulative scoring plays
# -------------------------
old_exists <- file.exists(paths$out_path)
old <- NULL
old_keys <- character()
old_games <- character()
scoring_plays_before <- 0L

if (old_exists) {
  old <- read.csv(paths$out_path, stringsAsFactors = FALSE)
  scoring_plays_before <- nrow(old)

  if (all(c("game_id", "play_id") %in% names(old)) && nrow(old) > 0) {
    old_keys <- paste(old$game_id, old$play_id, sep = "::")
    old_games <- unique(as.character(old$game_id))
  }
}

new_games <- if (games_requested == 0) 0L else sum(!(game_ids %in% old_games))

# -------------------------
# Early exit: no games
# -------------------------
if (games_requested == 0) {
  row <- make_log_row(
    refreshed_at,
    season,
    week,
    games_requested,
    pbp_rows = 0L,
    scoring_plays_this_refresh = 0L,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_before,
    new_games = new_games,
    new_scoring_plays = 0L,
    existing_scoring_plays_seen = 0L,
    changed_scoring_plays = 0L,
    overwritten_keys = 0L,
    t_schedule_s,
    t_scrape_s = 0,
    t_decode_s = 0,
    t_transform_s = 0,
    t_upsert_s = 0,
    t_total_s = as.numeric(difftime(Sys.time(), t0, units = "secs"))
  )

  write_log_and_status(row, paths$log_path, paths$status_path)
  cat("No games found. Exiting.\n")
  quit(status = 0)
}

# -------------------------
# Fetch pbp
# -------------------------
t_scrape0 <- Sys.time()
pbp <- tryCatch(
  nflfastR::fast_scraper(game_ids = game_ids),
  error = function(e) NULL
)
t_scrape_s <- as.numeric(difftime(Sys.time(), t_scrape0, units = "secs"))

# -------------------------
# Decode player IDs (optional)
# -------------------------
decode_ids <- TRUE

if (decode_ids) {
  t_decode0 <- Sys.time()
  pbp <- nflfastR::decode_player_ids(pbp)
  t_decode_s <- as.numeric(difftime(Sys.time(), t_decode0, units = "secs"))
} else {
  t_decode_s <- 0
}


if (is.null(pbp) || nrow(pbp) == 0) {
  row <- make_log_row(
    refreshed_at,
    season,
    week,
    games_requested,
    pbp_rows = 0L,
    scoring_plays_this_refresh = 0L,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_before,
    new_games = new_games,
    new_scoring_plays = 0L,
    existing_scoring_plays_seen = 0L,
    changed_scoring_plays = 0L,
    overwritten_keys = 0L,
    t_schedule_s,
    t_scrape_s,
    t_decode_s = 0,
    t_transform_s = 0,
    t_upsert_s = 0,
    t_total_s = as.numeric(difftime(Sys.time(), t0, units = "secs"))
  )

  write_log_and_status(row, paths$log_path, paths$status_path)
  cat("No pbp returned. Exiting.\n")
  quit(status = 0)
}

pbp_rows <- nrow(pbp)

infer_is_final <- function(df) {
  # Prefer an explicit game_end column if it exists
  if ("game_end" %in% names(df)) {
    return(any(df$game_end == 1, na.rm = TRUE))
  }

  # Fallback: if we can see game_seconds_remaining hit 0 in 4th/OT
  if (all(c("qtr", "game_seconds_remaining") %in% names(df))) {
    min_gsr <- suppressWarnings(min(df$game_seconds_remaining, na.rm = TRUE))
    max_qtr <- suppressWarnings(max(df$qtr, na.rm = TRUE))
    if (is.finite(min_gsr) && is.finite(max_qtr)) {
      return(min_gsr == 0 && max_qtr >= 4)
    }
  }

  # Last fallback: unknown -> not final
  return(FALSE)
}

pbp_watermarks <- pbp %>%
  dplyr::group_by(game_id) %>%
  dplyr::summarise(
    refreshed_at = refreshed_at,
    pbp_rows = dplyr::n(),
    max_play_id = suppressWarnings(max(play_id, na.rm = TRUE)),
    is_final = infer_is_final(dplyr::cur_data_all()),
    .groups = "drop"
  )

# If no pbp returned at all, write a metrics_out with requested games and exit cleanly
if (nrow(pbp) == 0) {
  cat("No pbp returned. Exiting.\n")

  if (!is.na(metrics_out_path) && metrics_out_path != "") {
    metrics <- data.frame(
      game_id = game_ids,
      pbp_rows = 0L,
      max_play_id = NA_integer_,
      is_final = FALSE,
      refreshed_at = format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
      status = "not_loaded_yet",
      stringsAsFactors = FALSE
    )
    readr::write_csv(metrics, metrics_out_path)
  }

  quit(status = 0)
}


# -------------------------
# Derive scoring plays
# -------------------------
t_transform0 <- Sys.time()
new_scoring <- derive_scoring_plays(
  pbp,
  refreshed_at = refreshed_at,
  season = season,
  week_default = week
)
t_transform_s <- as.numeric(difftime(Sys.time(), t_transform0, units = "secs"))

scoring_plays_this_refresh <- nrow(new_scoring)

write.csv(new_scoring, paths$latest_path, row.names = FALSE)

if (scoring_plays_this_refresh == 0) {
  row <- make_log_row(
    refreshed_at,
    season,
    week,
    games_requested,
    pbp_rows = pbp_rows,
    scoring_plays_this_refresh = 0L,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_before,
    new_games = new_games,
    new_scoring_plays = 0L,
    existing_scoring_plays_seen = 0L,
    changed_scoring_plays = 0L,
    overwritten_keys = 0L,
    t_schedule_s,
    t_scrape_s,
    t_decode_s = 0,
    t_transform_s,
    t_upsert_s = 0,
    t_total_s = as.numeric(difftime(Sys.time(), t0, units = "secs"))
  )

  write_log_and_status(row, paths$log_path, paths$status_path)
  cat("No scoring plays found. Exiting.\n")
  quit(status = 0)
}

# -------------------------
# Delta metrics
# -------------------------
new_keys <- paste(new_scoring$game_id, new_scoring$play_id, sep = "::")
new_scoring_plays <- sum(!(new_keys %in% old_keys))
existing_scoring_plays_seen <- sum(new_keys %in% old_keys)
changed_scoring_plays <- 0L
overwritten_keys <- existing_scoring_plays_seen

# -------------------------
# Upsert cumulative
# -------------------------
t_upsert0 <- Sys.time()
combined <- upsert_latest_wins(old, new_scoring)
t_upsert_s <- as.numeric(difftime(Sys.time(), t_upsert0, units = "secs"))

scoring_plays_after <- nrow(combined)
write.csv(combined, paths$out_path, row.names = FALSE)

# -------------------------
# Ensure player positions
# -------------------------
ensure_player_positions(season, paths$positions_path)

# -------------------------
# Log + status
# -------------------------
row <- make_log_row(
  refreshed_at,
  season,
  week,
  games_requested,
  pbp_rows = pbp_rows,
  scoring_plays_this_refresh = scoring_plays_this_refresh,
  scoring_plays_before = scoring_plays_before,
  scoring_plays_after = scoring_plays_after,
  new_games = new_games,
  new_scoring_plays = new_scoring_plays,
  existing_scoring_plays_seen = existing_scoring_plays_seen,
  changed_scoring_plays = changed_scoring_plays,
  overwritten_keys = overwritten_keys,
  t_schedule_s,
  t_scrape_s,
  t_decode_s = t_decode_s,
  t_transform_s,
  t_upsert_s,
  t_total_s = as.numeric(difftime(Sys.time(), t0, units = "secs"))
)

write_log_and_status(row, paths$log_path, paths$status_path)

cat(
  "Wrote cumulative scoring plays:",
  paths$out_path,
  "rows:",
  scoring_plays_after,
  "\n"
)
cat("Updated refresh status + log\n")
