# r/lib/scoring_plays.R
col_or <- function(df, name, default) {
  if (name %in% names(df)) df[[name]] else default
}

derive_scoring_plays <- function(pbp, refreshed_at, season, week) {
  pbp2 <- pbp %>%
    dplyr::mutate(
      touchdown = col_or(., "touchdown", rep(0L, dplyr::n())),
      safety = col_or(., "safety", rep(0L, dplyr::n())),
      field_goal_result = col_or(
        .,
        "field_goal_result",
        rep(NA_character_, dplyr::n())
      ),
      extra_point_result = col_or(
        .,
        "extra_point_result",
        rep(NA_character_, dplyr::n())
      ),
      two_point_conv_result = col_or(
        .,
        "two_point_conv_result",
        rep(NA_character_, dplyr::n())
      ),

      is_td = !is.na(touchdown) & touchdown == 1,
      is_fg = !is.na(field_goal_result) & tolower(field_goal_result) == "made",
      is_xp = !is.na(extra_point_result) &
        tolower(extra_point_result) %in% c("good", "made"),

      # Offensive 2pt success
      is_2pt_off = !is.na(two_point_conv_result) &
        tolower(two_point_conv_result) %in% c("success", "good"),

      desc_l = tolower(col_or(., "desc", rep("", n()))),

      is_xp_good = !is.na(extra_point_result) &
        tolower(extra_point_result) %in% c("good", "made"),

      # Blocked XP returned by defense for 2 (rare). Often encoded as XP attempt, not two_point_conv_result.
      is_pat_def_2pt = stringr::str_detect(desc_l, "extra point") &
        stringr::str_detect(desc_l, "blocked") &
        stringr::str_detect(desc_l, "return") &
        stringr::str_detect(desc_l, "two"),

      is_xp = is_xp_good,

      # Defensive 2pt return (rare): not always labeled as success/good and may not set touchdown==1
      is_def_2pt_return = !is.na(two_point_conv_result) &
        (tolower(two_point_conv_result) %in%
          c("return", "returned") |
          stringr::str_detect(
            desc_l,
            "two[- ]point.*return|return.*two[- ]point|defensive two[- ]point"
          )),

      is_2pt = is_2pt_off | is_def_2pt_return,

      is_safety = !is.na(safety) & safety == 1,
      is_scoring_play = is_td |
        is_fg |
        is_xp |
        is_2pt |
        is_safety |
        is_pat_def_2pt
    )

  pbp2 %>%
    dplyr::filter(is_scoring_play) %>%
    dplyr::transmute(
      refreshed_at = refreshed_at,
      season = season,
      week = col_or(., "week", rep(week, dplyr::n())),
      game_id = game_id,
      game_date = col_or(., "game_date", rep(NA_character_, dplyr::n())),
      posteam = col_or(., "posteam", rep(NA_character_, dplyr::n())),
      defteam = col_or(., "defteam", rep(NA_character_, dplyr::n())),
      qtr = col_or(., "qtr", rep(NA_integer_, dplyr::n())),
      time = col_or(., "time", rep(NA_character_, dplyr::n())),
      drive = col_or(., "drive", rep(NA_integer_, dplyr::n())),
      play_id = play_id,
      desc = col_or(., "desc", rep(NA_character_, dplyr::n())),

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
      is_def_2pt_return = col_or(., "is_def_2pt_return", rep(FALSE, n())),
      is_pat_def_2pt = col_or(., "is_pat_def_2pt", rep(FALSE, n())),
      play_type = col_or(., "play_type", rep(NA_character_, dplyr::n())),
      pass = col_or(., "pass", rep(NA_integer_, dplyr::n())),
      rush = col_or(., "rush", rep(NA_integer_, dplyr::n())),
      qb_dropback = col_or(., "qb_dropback", rep(NA_integer_, dplyr::n())),
      sack = col_or(., "sack", rep(NA_integer_, dplyr::n())),
      interception = col_or(., "interception", rep(NA_integer_, dplyr::n())),
      fumble_lost = col_or(., "fumble_lost", rep(NA_integer_, dplyr::n())),
      return_team = col_or(., "return_team", rep(NA_character_, dplyr::n())),

      passer_player_id = col_or(
        .,
        "passer_player_id",
        rep(NA_character_, dplyr::n())
      ),
      passer_player_name = col_or(
        .,
        "passer_player_name",
        rep(NA_character_, dplyr::n())
      ),
      receiver_player_id = col_or(
        .,
        "receiver_player_id",
        rep(NA_character_, dplyr::n())
      ),
      receiver_player_name = col_or(
        .,
        "receiver_player_name",
        rep(NA_character_, dplyr::n())
      ),
      rusher_player_id = col_or(
        .,
        "rusher_player_id",
        rep(NA_character_, dplyr::n())
      ),
      rusher_player_name = col_or(
        .,
        "rusher_player_name",
        rep(NA_character_, dplyr::n())
      ),
      kicker_player_id = col_or(
        .,
        "kicker_player_id",
        rep(NA_character_, dplyr::n())
      ),
      kicker_player_name = col_or(
        .,
        "kicker_player_name",
        rep(NA_character_, dplyr::n())
      )
    )
}
