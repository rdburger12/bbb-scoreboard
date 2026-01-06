# r/lib/scoring_plays.R
# Derive scoring plays for BBB Scoreboard from nflfastR pbp.
# This is intentionally small and deterministic to match Calculate BBB Points.R.

suppressPackageStartupMessages({
  library(dplyr)
})

# Safe column access: returns df[[name]] if present else default vector (must be length nrow(df))
col_or <- function(df, name, default) {
  if (name %in% names(df)) df[[name]] else default
}

# Normalize string-ish vectors (safe for NA)
as_chr <- function(x) {
  x <- ifelse(is.na(x), NA_character_, as.character(x))
  x
}

# Normalize integer-ish vectors (safe for NA)
as_int <- function(x) {
  suppressWarnings(as.integer(x))
}

# Normalize logical-ish vectors (safe for NA; treats 1 as TRUE)
as_lgl <- function(x) {
  if (is.logical(x)) {
    return(ifelse(is.na(x), FALSE, x))
  }
  if (is.numeric(x) || is.integer(x)) {
    return(ifelse(is.na(x), FALSE, x == 1))
  }
  # fallback: strings
  x2 <- tolower(ifelse(is.na(x), "", as.character(x)))
  x2 %in% c("1", "true", "t", "yes")
}

#' derive_scoring_plays
#'
#' @param pbp nflfastR play-by-play data.frame/tibble (from fast_scraper / load_pbp etc.)
#' @param refreshed_at character timestamp to stamp output rows (optional)
#' @param season integer season (optional; will pull from pbp if present)
#' @param week_default integer fallback week if pbp lacks week column (optional)
#'
#' @return tibble of scoring plays with canonical BBB flags
derive_scoring_plays <- function(
  pbp,
  refreshed_at = as.character(Sys.time()),
  season = NA_integer_,
  week_default = NA_integer_
) {
  if (is.null(pbp) || nrow(pbp) == 0) {
    return(tibble::tibble())
  }

  n <- nrow(pbp)

  # Pull raw columns (or defaults) once
  touchdown <- as_int(col_or(pbp, "touchdown", rep(0L, n)))
  safety <- as_int(col_or(pbp, "safety", rep(0L, n)))

  field_goal_result <- as_chr(col_or(
    pbp,
    "field_goal_result",
    rep(NA_character_, n)
  ))
  extra_point_result <- as_chr(col_or(
    pbp,
    "extra_point_result",
    rep(NA_character_, n)
  ))
  two_point_conv_result <- as_chr(col_or(
    pbp,
    "two_point_conv_result",
    rep(NA_character_, n)
  ))

  pass_touchdown <- as_lgl(col_or(pbp, "pass_touchdown", rep(FALSE, n)))
  rush_touchdown <- as_lgl(col_or(pbp, "rush_touchdown", rep(FALSE, n)))

  # nflfastR canonical flag for “defensive two-point conversion”
  # This includes blocked XP returned for 2 and defensive returns on 2pt tries.
  defensive_two_point_conv <- as_int(col_or(
    pbp,
    "defensive_two_point_conv",
    rep(0L, n)
  ))

  # Canonical scoring booleans
  is_td <- touchdown == 1
  is_fg <- !is.na(field_goal_result) & tolower(field_goal_result) == "made"
  is_xp <- !is.na(extra_point_result) &
    tolower(extra_point_result) %in% c("good", "made")

  # Offensive 2pt success (your old R logic uses success)
  # We allow "good" as well, but it is rare and harmless.
  is_2pt_off <- !is.na(two_point_conv_result) &
    tolower(two_point_conv_result) %in% c("success", "good")

  # Defensive 2pt (authoritative)
  is_def_two_pt <- !is.na(defensive_two_point_conv) &
    defensive_two_point_conv == 1

  # Safety (authoritative)
  is_safety <- !is.na(safety) & safety == 1

  # TD attribution splits (authoritative)
  is_td_off <- is_td & (pass_touchdown | rush_touchdown)
  is_td_def <- is_td & !is_td_off

  # “Scoring play” inclusion for storage
  # - includes defensive two-point conversions even if not marked as “2pt success”
  is_scoring_play <- is_td |
    is_fg |
    is_xp |
    is_2pt_off |
    is_safety |
    is_def_two_pt

  # Build output
  out <- pbp %>%
    mutate(
      refreshed_at = refreshed_at,

      # Fill season/week if missing in pbp
      season = if ("season" %in% names(pbp)) {
        as_int(.data$season)
      } else {
        as_int(rep(season, n))
      },
      week = if ("week" %in% names(pbp)) {
        as_int(.data$week)
      } else {
        as_int(rep(week_default, n))
      },

      touchdown = touchdown,
      safety = safety,
      field_goal_result = field_goal_result,
      extra_point_result = extra_point_result,
      two_point_conv_result = two_point_conv_result,

      pass_touchdown = pass_touchdown,
      rush_touchdown = rush_touchdown,
      defensive_two_point_conv = defensive_two_point_conv,

      is_td = is_td,
      is_fg = is_fg,
      is_xp = is_xp,
      is_2pt = is_2pt_off, # keep name consistent with your CSV schema
      is_safety = is_safety,

      is_def_two_pt = is_def_two_pt,
      is_td_off = is_td_off,
      is_td_def = is_td_def,

      is_scoring_play = is_scoring_play
    ) %>%
    filter(.data$is_scoring_play) %>%
    transmute(
      refreshed_at,

      season,
      week,

      game_id = as_chr(col_or(., "game_id", rep(NA_character_, n()))),
      game_date = as_chr(col_or(., "game_date", rep(NA_character_, n()))),

      posteam = as_chr(col_or(., "posteam", rep(NA_character_, n()))),
      defteam = as_chr(col_or(., "defteam", rep(NA_character_, n()))),

      qtr = as_int(col_or(., "qtr", rep(NA_integer_, n()))),
      time = as_chr(col_or(., "time", rep(NA_character_, n()))),
      drive = as_int(col_or(., "drive", rep(NA_integer_, n()))),

      play_id = as_int(col_or(., "play_id", rep(NA_integer_, n()))),
      desc = as_chr(col_or(., "desc", rep(NA_character_, n()))),

      touchdown,
      field_goal_result,
      extra_point_result,
      two_point_conv_result,
      safety,

      # Base flags used by Python
      is_td,
      is_fg,
      is_xp,
      is_2pt,
      is_safety,

      # Authoritative attribution flags (used to avoid heuristic mistakes)
      pass_touchdown,
      rush_touchdown,
      is_td_off,
      is_td_def,

      defensive_two_point_conv,
      is_def_two_pt,

      # Context columns (kept because they are useful for UI/diagnostics)
      play_type = as_chr(col_or(., "play_type", rep(NA_character_, n()))),
      pass = as_int(col_or(., "pass", rep(NA_integer_, n()))),
      rush = as_int(col_or(., "rush", rep(NA_integer_, n()))),
      qb_dropback = as_int(col_or(., "qb_dropback", rep(NA_integer_, n()))),
      sack = as_int(col_or(., "sack", rep(NA_integer_, n()))),
      interception = as_int(col_or(., "interception", rep(NA_integer_, n()))),
      fumble_lost = as_int(col_or(., "fumble_lost", rep(NA_integer_, n()))),

      return_team = as_chr(col_or(., "return_team", rep(NA_character_, n()))),

      passer_player_id = as_chr(col_or(
        .,
        "passer_player_id",
        rep(NA_character_, n())
      )),
      passer_player_name = as_chr(col_or(
        .,
        "passer_player_name",
        rep(NA_character_, n())
      )),

      receiver_player_id = as_chr(col_or(
        .,
        "receiver_player_id",
        rep(NA_character_, n())
      )),
      receiver_player_name = as_chr(col_or(
        .,
        "receiver_player_name",
        rep(NA_character_, n())
      )),

      rusher_player_id = as_chr(col_or(
        .,
        "rusher_player_id",
        rep(NA_character_, n())
      )),
      rusher_player_name = as_chr(col_or(
        .,
        "rusher_player_name",
        rep(NA_character_, n())
      )),

      kicker_player_id = as_chr(col_or(
        .,
        "kicker_player_id",
        rep(NA_character_, n())
      )),
      kicker_player_name = as_chr(col_or(
        .,
        "kicker_player_name",
        rep(NA_character_, n())
      ))
    )

  out
}
