# r/lib/upsert.R
upsert_latest_wins <- function(old, new_scoring) {
  old_exists <- !is.null(old) && nrow(old) > 0

  if (!old_exists) {
    return(new_scoring)
  }

  # Align schemas
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

  combined_pre <- dplyr::bind_rows(old, new_scoring)

  combined_pre %>%
    dplyr::arrange(game_id, play_id, refreshed_at) %>%
    dplyr::group_by(game_id, play_id) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()
}
