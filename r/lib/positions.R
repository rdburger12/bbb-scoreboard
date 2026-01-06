ensure_player_positions <- function(season, pos_path) {
  if (is.na(pos_path) || pos_path == "") {
    return(invisible(TRUE))
  }
  if (file.exists(pos_path)) {
    return(invisible(TRUE))
  }

  suppressPackageStartupMessages({
    library(dplyr)
    library(stringr)
    library(nflfastR)
  })

  ro_raw <- nflfastR::fast_scraper_roster(seasons = as.integer(season))

  # In your environment, this roster provides gsis_id (00-00xxxxxx), not player_id UUIDs.
  if (!("gsis_id" %in% names(ro_raw))) {
    stop(
      "fast_scraper_roster() did not return gsis_id. Columns found: ",
      paste(names(ro_raw), collapse = ", ")
    )
  }

  ro <- ro_raw %>%
    transmute(
      player_id = as.character(gsis_id), # IMPORTANT: write player_id = GSIS id
      position = toupper(str_trim(position))
    ) %>%
    filter(!is.na(player_id), player_id != "") %>%
    mutate(
      position = str_replace(position, "^FB$", "RB"),
      position_bucket = case_when(
        position %in% c("QB", "RB", "WR", "TE", "K") ~ position,
        TRUE ~ "OTH"
      )
    ) %>%
    distinct(player_id, position_bucket)

  write.csv(ro, pos_path, row.names = FALSE)
  invisible(TRUE)
}
