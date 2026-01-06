# r/lib/schedule.R
resolve_game_ids_for_week <- function(season, week) {
  sched <- nflfastR::fast_scraper_schedules(seasons = season) %>%
    dplyr::filter(week == !!week)

  unique(as.character(sched$game_id))
}
