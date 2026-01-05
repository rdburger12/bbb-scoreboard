#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(nflfastR)
})

# -------------------------
# Arg parsing (simple, robust)
# -------------------------
args <- commandArgs(trailingOnly = TRUE)

get_flag_value <- function(flag) {
  idx <- which(args == flag)
  if (length(idx) == 0) {
    return(NA)
  }
  if (idx[length(idx)] == length(args)) {
    return(NA)
  }
  return(args[idx[1] + 1])
}

has_flag <- function(flag) any(args == flag)

season_val <- get_flag_value("--season")
weeks_val <- get_flag_value("--weeks") # e.g. "19,20,21,22"
keep_raw <- FALSE
if (has_flag("--keep_raw")) {
  kv <- get_flag_value("--keep_raw")
  keep_raw <- !is.na(kv) && as.integer(kv) == 1
}

# Support explicit game ids: everything after --game_ids is treated as ids
game_ids <- character()
if (has_flag("--game_ids")) {
  idx <- which(args == "--game_ids")[1]
  if (idx < length(args)) {
    game_ids <- args[(idx + 1):length(args)]
  }
}

dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

# -------------------------
# Resolve game_ids
# -------------------------
if (length(game_ids) == 0) {
  if (is.na(season_val)) {
    stop(
      "Usage:\n  Rscript r/refresh_pbp.R --season <SEASON> [--weeks 19,20,21,22] [--keep_raw 0|1]\n  OR\n  Rscript r/refresh_pbp.R --game_ids <GAME_ID_1> <GAME_ID_2> ... [--keep_raw 0|1]"
    )
  }

  season <- as.integer(season_val)
  cat("Loading schedule for season:", season, "\n")

  sched <- fast_scraper_schedules(seasons = season)

  if (!is.na(weeks_val)) {
    weeks <- as.integer(strsplit(weeks_val, ",")[[1]])
    sched <- sched %>% filter(week %in% weeks)
    cat("Filtering to weeks:", paste(weeks, collapse = ","), "\n")
  }

  game_ids <- sched$game_id %>% unique() %>% as.character()
  cat("Resolved game_ids from schedule:", length(game_ids), "\n")
} else {
  cat("Using provided game_ids:", paste(game_ids, collapse = ", "), "\n")
}

if (length(game_ids) == 0) {
  stop("No game_ids resolved. Check season/weeks or passed game_ids.")
}

# -------------------------
# Build pbp
# -------------------------
cat("Building nflfastR pbp for", length(game_ids), "game(s)\n")

pbp <- build_nflfastR_pbp(game_ids = game_ids) %>%
  decode_player_ids()

# -------------------------
# Filter scoring plays and write CSV
# -------------------------
# -------------------------
# Derive scoring plays (version-stable)
# -------------------------
pbp2 <- pbp %>%
  mutate(
    is_td = !is.na(touchdown) & touchdown == 1,
    is_fg = !is.na(field_goal_result) & field_goal_result == "made",
    is_xp = !is.na(extra_point_result) &
      extra_point_result %in% c("good", "made"),
    is_2pt = !is.na(two_point_conv_result) &
      two_point_conv_result %in% c("success", "good"),
    is_safety = !is.na(safety) & safety == 1,
    is_scoring_play = is_td | is_fg | is_xp | is_2pt | is_safety
  )

scoring <- pbp2 %>%
  filter(is_scoring_play) %>%
  transmute(
    season,
    game_id,
    game_date,
    week,
    posteam,
    defteam,
    qtr,
    time,
    drive,
    play_id,
    desc,

    # scoring outcomes
    touchdown,
    field_goal_result,
    extra_point_result,
    two_point_conv_result,
    safety,

    # derived flags (handy for Python scoring rules)
    is_td,
    is_fg,
    is_xp,
    is_2pt,
    is_safety,

    # play classification / context (only if present)
    play_type,
    pass,
    rush,
    qb_dropback,
    sack,
    interception,
    fumble_lost,
    return_team,

    # key participants
    passer_player_id,
    passer_player_name,
    receiver_player_id,
    receiver_player_name,
    rusher_player_id,
    rusher_player_name,
    kicker_player_id,
    kicker_player_name
  )

out_scoring <- "data/processed/scoring_plays.csv"
write.csv(scoring, out_scoring, row.names = FALSE)
cat("Wrote:", out_scoring, "rows:", nrow(scoring), "\n")

if (keep_raw) {
  out_full <- "data/raw/pbp_latest.csv"
  write.csv(pbp, out_full, row.names = FALSE)
  cat("Wrote full pbp to:", out_full, "\n")
}
