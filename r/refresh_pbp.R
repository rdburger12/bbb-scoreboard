#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(nflfastR)
})

args <- commandArgs(trailingOnly = TRUE)

has_flag <- function(flag) any(args == flag)

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

# Modes:
# 1) --mode week --season 2025 --week 18
# 2) --mode game_ids --game_ids <id1> <id2> ...
mode <- get_flag_value("--mode")
if (is.na(mode)) {
  mode <- "week"
}

season_val <- get_flag_value("--season")
week_val <- get_flag_value("--week")
out_path <- get_flag_value("--out")
if (is.na(out_path)) {
  out_path <- "data/processed/scoring_plays.csv"
}

dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

# -------------------------
# Resolve game_ids
# -------------------------
game_ids <- character()

if (mode == "game_ids") {
  if (!has_flag("--game_ids")) {
    stop("Mode game_ids requires --game_ids <id1> <id2> ...")
  }
  idx <- which(args == "--game_ids")[1]
  if (idx < length(args)) {
    game_ids <- args[(idx + 1):length(args)]
  }
  if (length(game_ids) == 0) stop("No game_ids provided.")
} else if (mode == "week") {
  if (is.na(season_val) || is.na(week_val)) {
    stop("Mode week requires --season <SEASON> --week <WEEK>")
  }
  season <- as.integer(season_val)
  week <- as.integer(week_val)

  cat(
    "Resolving game_ids from schedule for season:",
    season,
    "week:",
    week,
    "\n"
  )
  sched <- fast_scraper_schedules(seasons = season) %>% filter(week == !!week)
  game_ids <- sched$game_id %>% unique() %>% as.character()
} else {
  stop(paste("Unknown mode:", mode))
}

cat("Resolved game_ids:", length(game_ids), "\n")
if (length(game_ids) == 0) {
  cat("No games found. Exiting.\n")
  quit(status = 0)
}

cat("Incremental refresh for game_ids:", paste(game_ids, collapse = ", "), "\n")
cat("Output:", out_path, "\n")

# -------------------------
# Build pbp
# -------------------------
pbp <- build_nflfastR_pbp(game_ids = game_ids)

if (is.null(pbp) || nrow(pbp) == 0) {
  cat("No play-by-play returned. Exiting.\n")
  quit(status = 0)
}

pbp <- decode_player_ids(pbp)

# -------------------------
# Derive scoring plays
# -------------------------
col_or <- function(df, name, default) {
  if (name %in% names(df)) df[[name]] else default
}

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
    refreshed_at = as.character(Sys.time()),
    season,
    week = if ("week" %in% names(pbp2)) week else NA_integer_,
    game_id,
    game_date,
    posteam,
    defteam,
    qtr,
    time,
    drive,
    play_id,
    desc,

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

    play_type,
    pass,
    rush,
    qb_dropback,
    sack,
    interception,
    fumble_lost,
    return_team,

    passer_player_id,
    passer_player_name,
    receiver_player_id,
    receiver_player_name,
    rusher_player_id,
    rusher_player_name,
    kicker_player_id,
    kicker_player_name
  )

if (nrow(new_scoring) == 0) {
  cat("No scoring plays found. Exiting.\n")
  quit(status = 0)
}

# -------------------------
# Upsert (latest-wins) by (game_id, play_id)
# -------------------------
if (file.exists(out_path)) {
  old <- read.csv(out_path, stringsAsFactors = FALSE)
  combined <- bind_rows(old, new_scoring) %>%
    arrange(game_id, play_id, refreshed_at) %>%
    group_by(game_id, play_id) %>%
    slice_tail(n = 1) %>%
    ungroup()
} else {
  combined <- new_scoring
}

write.csv(combined, out_path, row.names = FALSE)
cat("Wrote cumulative scoring plays:", out_path, "rows:", nrow(combined), "\n")

latest_path <- "data/processed/scoring_plays_latest.csv"
write.csv(new_scoring, latest_path, row.names = FALSE)
cat(
  "Wrote latest refresh scoring plays:",
  latest_path,
  "rows:",
  nrow(new_scoring),
  "\n"
)
