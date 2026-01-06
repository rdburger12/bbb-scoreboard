# r/lib/paths.R
get_paths <- function(out_path = NA_character_, season = NA_integer_) {
  dir.create("data/processed", recursive = TRUE, showWarnings = FALSE)

  if (is.na(out_path) || out_path == "") {
    out_path <- "data/processed/scoring_plays.csv"
  }

  processed_dir <- dirname(out_path)

  list(
    out_path = out_path,
    processed_dir = processed_dir,
    latest_path = file.path(processed_dir, "scoring_plays_latest.csv"),
    log_path = file.path(processed_dir, "refresh_log.csv"),
    status_path = file.path(processed_dir, "refresh_status.csv"),
    positions_path = if (!is.na(season)) {
      file.path(processed_dir, sprintf("player_positions_%s.csv", season))
    } else {
      NA_character_
    }
  )
}
