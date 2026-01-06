# r/lib/logging.R
make_log_row <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}

write_log_and_status <- function(row_df, log_path, status_path) {
  # keep your schema-rotation behavior or remove it; no behavior change for now
  if (file.exists(log_path)) {
    header <- names(read.csv(log_path, nrows = 0, stringsAsFactors = FALSE))
    if (!identical(header, names(row_df))) {
      rotated <- sub(
        "\\.csv$",
        paste0("_old_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"),
        log_path
      )
      file.rename(log_path, rotated)
      cat("Log schema changed. Rotated old log to:", rotated, "\n")
    }
  }

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

  write.csv(row_df, status_path, row.names = FALSE)
}
