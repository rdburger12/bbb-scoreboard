#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(nflfastR)
})

args <- commandArgs(trailingOnly = TRUE)

get_flag_value <- function(flag) {
  idx <- which(args == flag)
  if (length(idx) == 0) {
    return(NA)
  }
  if (idx[1] >= length(args)) {
    return(NA)
  }
  args[idx[1] + 1]
}

season_val <- get_flag_value("--season")
week_val <- get_flag_value("--week")
out_path <- get_flag_value("--out")

if (is.na(season_val) || is.na(week_val)) {
  stop(
    "Usage: Rscript r/refresh_pbp.R --season <SEASON> --week <WEEK> [--out <PATH>]"
  )
}

season <- as.integer(season_val)
week <- as.integer(week_val)

dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

if (is.na(out_path)) {
  out_path <- "data/processed/scoring_plays.csv"
}

log_path <- "data/processed/refresh_log.csv" # append-only
status_path <- "data/processed/refresh_status.csv" # overwrite each run
latest_path <- "data/processed/scoring_plays_latest.csv"

refreshed_at <- as.character(Sys.time())

cat("Incremental refresh | season:", season, "| week:", week, "\n")
cat("Output:", out_path, "\n")

# -------------------------
# Resolve game_ids from schedule
# -------------------------
sched <- fast_scraper_schedules(seasons = season) %>% filter(week == !!week)
game_ids <- unique(as.character(sched$game_id))
games_requested <- length(game_ids)

cat("Resolved game_ids:", games_requested, "\n")

# Load existing cumulative scoring plays (if any)
old_exists <- file.exists(out_path)
old <- NULL
old_keys <- character()
old_games <- character()
scoring_plays_before <- 0L

if (old_exists) {
  old <- read.csv(out_path, stringsAsFactors = FALSE)
  scoring_plays_before <- nrow(old)
  if (all(c("game_id", "play_id") %in% names(old)) && nrow(old) > 0) {
    old_keys <- paste(old$game_id, old$play_id, sep = "::")
    old_games <- unique(as.character(old$game_id))
  }
}

new_games <- if (games_requested == 0) 0L else sum(!(game_ids %in% old_games))

# Helper for schema-safe access
col_or <- function(df, name, default) {
  if (name %in% names(df)) df[[name]] else default
}

# Defaults for this attempt
pbp_rows <- 0L
scoring_plays_this_refresh <- 0L
new_scoring_plays <- 0L
updated_scoring_plays <- 0L
duplicates_removed <- 0L
scoring_plays_after <- scoring_plays_before

# -------------------------
# Helper: write log row (append) and status row (overwrite)
# -------------------------
write_log_and_status <- function(row_df) {
  # append to log
  if (file.exists(log_path)) {
    write.table(
      row_df,
      log_path,
      sep = ",",
      row.names = FALSE,
      col.names = FALSE,
      append = TRUE
    )
  } else {
    write.csv(row_df, log_path, row.names = FALSE)
  }
  # overwrite status
  write.csv(row_df, status_path, row.names = FALSE)
}

# If no games, log attempt and exit
if (games_requested == 0) {
  row <- data.frame(
    refreshed_at = refreshed_at,
    season = season,
    week = week,
    games_requested = games_requested,
    pbp_rows = pbp_rows,
    scoring_plays_this_refresh = scoring_plays_this_refresh,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_after,
    new_games = new_games,
    new_scoring_plays = new_scoring_plays,
    updated_scoring_plays = updated_scoring_plays,
    duplicates_removed = duplicates_removed,
    stringsAsFactors = FALSE
  )
  write_log_and_status(row)
  cat("No games found. Logged + updated status. Exiting.\n")
  quit(status = 0)
}

# -------------------------
# Build pbp
# -------------------------
pbp <- build_nflfastR_pbp(game_ids = game_ids)

if (is.null(pbp) || nrow(pbp) == 0) {
  row <- data.frame(
    refreshed_at = refreshed_at,
    season = season,
    week = week,
    games_requested = games_requested,
    pbp_rows = 0L,
    scoring_plays_this_refresh = 0L,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_before,
    new_games = new_games,
    new_scoring_plays = 0L,
    updated_scoring_plays = 0L,
    duplicates_removed = 0L,
    stringsAsFactors = FALSE
  )
  write_log_and_status(row)
  cat("No pbp returned. Logged + updated status. Exiting.\n")
  quit(status = 0)
}

pbp_rows <- nrow(pbp)

# decode player ids (you have gsisdecoder installed)
pbp <- decode_player_ids(pbp)

# -------------------------
# Derive scoring plays
# -------------------------
pbp2 <- pbp %>%
  mutate(
    touchdown = col_or(., "touchdown", rep(0L, n())),
    safety = col_or(., "safety", rep(0L, n())),
    field_goal_result = col_or(., "field_goal_result", rep(NA_character_, n())),
    extra_point_result = col_or(
      .,
      "extra_point_result",
      rep(NA_character_, n())
    ),
    two_point_conv_result = col_or(
      .,
      "two_point_conv_result",
      rep(NA_character_, n())
    ),

    is_td = !is.na(touchdown) & touchdown == 1,
    is_fg = !is.na(field_goal_result) & tolower(field_goal_result) == "made",
    is_xp = !is.na(extra_point_result) &
      tolower(extra_point_result) %in% c("good", "made"),
    is_2pt = !is.na(two_point_conv_result) &
      tolower(two_point_conv_result) %in% c("success", "good"),
    is_safety = !is.na(safety) & safety == 1,
    is_scoring_play = is_td | is_fg | is_xp | is_2pt | is_safety
  )

new_scoring <- pbp2 %>%
  filter(is_scoring_play) %>%
  transmute(
    refreshed_at = refreshed_at,
    season,
    week = col_or(., "week", rep(week, n())),
    game_id,
    game_date = col_or(., "game_date", rep(NA_character_, n())),
    posteam = col_or(., "posteam", rep(NA_character_, n())),
    defteam = col_or(., "defteam", rep(NA_character_, n())),
    qtr = col_or(., "qtr", rep(NA_integer_, n())),
    time = col_or(., "time", rep(NA_character_, n())),
    drive = col_or(., "drive", rep(NA_integer_, n())),
    play_id,
    desc = col_or(., "desc", rep(NA_character_, n())),

    touchdown,
    field_goal_result,
    extra_point_result,
    two_point_conv_result,
    safety,

    is_td,
    is_fg,
    is_xp,
    is_2pt,
    is_safety,

    play_type = col_or(., "play_type", rep(NA_character_, n())),
    pass = col_or(., "pass", rep(NA_integer_, n())),
    rush = col_or(., "rush", rep(NA_integer_, n())),
    qb_dropback = col_or(., "qb_dropback", rep(NA_integer_, n())),
    sack = col_or(., "sack", rep(NA_integer_, n())),
    interception = col_or(., "interception", rep(NA_integer_, n())),
    fumble_lost = col_or(., "fumble_lost", rep(NA_integer_, n())),
    return_team = col_or(., "return_team", rep(NA_character_, n())),

    passer_player_id = col_or(., "passer_player_id", rep(NA_character_, n())),
    passer_player_name = col_or(
      .,
      "passer_player_name",
      rep(NA_character_, n())
    ),
    receiver_player_id = col_or(
      .,
      "receiver_player_id",
      rep(NA_character_, n())
    ),
    receiver_player_name = col_or(
      .,
      "receiver_player_name",
      rep(NA_character_, n())
    ),
    rusher_player_id = col_or(., "rusher_player_id", rep(NA_character_, n())),
    rusher_player_name = col_or(
      .,
      "rusher_player_name",
      rep(NA_character_, n())
    ),
    kicker_player_id = col_or(., "kicker_player_id", rep(NA_character_, n())),
    kicker_player_name = col_or(
      .,
      "kicker_player_name",
      rep(NA_character_, n())
    )
  )

scoring_plays_this_refresh <- nrow(new_scoring)

# Always write latest snapshot (even if empty)
write.csv(new_scoring, latest_path, row.names = FALSE)

# If no scoring plays, log attempt and exit (no change to cumulative)
if (scoring_plays_this_refresh == 0) {
  row <- data.frame(
    refreshed_at = refreshed_at,
    season = season,
    week = week,
    games_requested = games_requested,
    pbp_rows = pbp_rows,
    scoring_plays_this_refresh = 0L,
    scoring_plays_before = scoring_plays_before,
    scoring_plays_after = scoring_plays_before,
    new_games = new_games,
    new_scoring_plays = 0L,
    updated_scoring_plays = 0L,
    duplicates_removed = 0L,
    stringsAsFactors = FALSE
  )
  write_log_and_status(row)
  cat("No scoring plays found. Logged + updated status.\n")
  quit(status = 0)
}

# Delta metrics vs old
new_keys <- paste(new_scoring$game_id, new_scoring$play_id, sep = "::")
new_scoring_plays <- sum(!(new_keys %in% old_keys))
updated_scoring_plays <- sum(new_keys %in% old_keys)

# Upsert (latest-wins)
if (old_exists) {
  missing_in_old <- setdiff(names(new_scoring), names(old))
  for (c in missing_in_old) {
    old[[c]] <- NA
  }

  missing_in_new <- setdiff(names(old), names(new_scoring))
  for (c in missing_in_new) {
    new_scoring[[c]] <- NA
  }

  old <- old[, names(new_scoring)]
  new_scoring <- new_scoring[, names(old)]

  combined_pre <- bind_rows(old, new_scoring)

  combined <- combined_pre %>%
    arrange(game_id, play_id, refreshed_at) %>%
    group_by(game_id, play_id) %>%
    #slice_tail(n = 1) %>%
    ungroup()

  duplicates_removed <- nrow(combined_pre) - nrow(combined)
} else {
  combined <- new_scoring
  duplicates_removed <- 0L
}

scoring_plays_after <- nrow(combined)

write.csv(combined, out_path, row.names = FALSE)

# Log + status
row <- data.frame(
  refreshed_at = refreshed_at,
  season = season,
  week = week,
  games_requested = games_requested,
  pbp_rows = pbp_rows,
  scoring_plays_this_refresh = scoring_plays_this_refresh,
  scoring_plays_before = scoring_plays_before,
  scoring_plays_after = scoring_plays_after,
  new_games = new_games,
  new_scoring_plays = new_scoring_plays,
  updated_scoring_plays = updated_scoring_plays,
  duplicates_removed = duplicates_removed,
  stringsAsFactors = FALSE
)

write_log_and_status(row)

cat(
  "Wrote cumulative scoring plays:",
  out_path,
  "rows:",
  scoring_plays_after,
  "\n"
)
cat("Appended refresh log row:", log_path, "\n")
cat("Updated refresh status:", status_path, "\n")
cat("Wrote latest snapshot:", latest_path, "\n")
