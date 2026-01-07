#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(nflfastR)
  library(readr)
  library(dotenv)
})

dotenv::load_dot_env()

# -------------------------
# CLI args + season resolve
# -------------------------
args <- commandArgs(trailingOnly = TRUE)

get_flag_value <- function(flag) {
  idx <- which(args == flag)
  if (length(idx) == 0) {
    return(NA_character_)
  }
  if (idx[1] >= length(args)) {
    return(NA_character_)
  }
  args[idx[1] + 1]
}

season_arg <- get_flag_value("--season")

# Try CLI first, then BBB_SEASON env var
season <- suppressWarnings(as.integer(season_arg))
if (is.na(season)) {
  season_env <- Sys.getenv("BBB_SEASON", unset = NA_character_)
  season <- suppressWarnings(as.integer(season_env))
}

if (is.na(season)) {
  stop(
    "Season not specified.\n",
    "Provide --season <YEAR> or set BBB_SEASON in .env"
  )
}

cat("Building playoff_game_ids for season:", season, "\n")

out_path <- file.path(
  "data",
  "config",
  sprintf("playoff_game_ids_%s.csv", season)
)

dir.create(dirname(out_path), recursive = TRUE, showWarnings = FALSE)

sched <- nflfastR::fast_scraper_schedules(seasons = season)

# Prefer explicit postseason indicator if present; otherwise fall back to weeks 19-22
if ("game_type" %in% names(sched)) {
  # Common nflverse convention: REG vs POST (or similar)
  playoffs <- sched %>% filter(toupper(game_type) != "REG")
} else if ("season_type" %in% names(sched)) {
  playoffs <- sched %>%
    filter(toupper(season_type) %in% c("POST", "POSTSEASON"))
} else {
  playoffs <- sched %>% filter(week %in% 19:22)
}

game_ids <- playoffs %>%
  transmute(game_id = as.character(game_id)) %>%
  distinct() %>%
  arrange(game_id)

if (nrow(game_ids) == 0) {
  stop(
    "No playoff game_ids found for this season using available schedule columns."
  )
}

readr::write_csv(game_ids, out_path)
cat("Wrote", nrow(game_ids), "game_ids to", out_path, "\n")
