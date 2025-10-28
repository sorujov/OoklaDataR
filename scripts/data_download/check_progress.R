# =============================================================================
# Simple Progress Checker
# =============================================================================
# Run this to see how many tasks are complete
# =============================================================================

library(here)

cat("\n")
cat("============================================================\n")
cat("         BATCH PROCESSING PROGRESS CHECK                   \n")
cat("============================================================\n\n")

# Count completed files
processed_dir <- here("data", "processed")
if (dir.exists(processed_dir)) {
  files <- list.files(processed_dir, pattern = "\\.rds$", full.names = FALSE)
  n_files <- length(files)
} else {
  n_files <- 0
}

# Total tasks
TOTAL_TASKS <- 572  # 11 countries × 26 quarters × 2 types

cat("COMPLETED TASKS:", n_files, "out of", TOTAL_TASKS, "\n")
cat("PROGRESS:", round(n_files / TOTAL_TASKS * 100, 1), "%\n\n")

# Estimate time remaining
if (n_files > 0) {
  avg_time_per_task <- 5  # minutes (rough estimate)
  remaining_tasks <- TOTAL_TASKS - n_files
  estimated_minutes <- remaining_tasks * avg_time_per_task / 3  # 3 cores
  estimated_hours <- estimated_minutes / 60
  
  cat("ESTIMATED TIME REMAINING:", round(estimated_hours, 1), "hours\n\n")
}

# Show completed countries
cat("============================================================\n")
cat("         COMPLETED COUNTRIES                                \n")
cat("============================================================\n\n")

aggregated_dir <- here("data", "aggregated")
if (dir.exists(aggregated_dir)) {
  country_files <- list.files(aggregated_dir, pattern = "_all_quarters\\.csv$", 
                              full.names = FALSE)
  
  if (length(country_files) > 0) {
    for (f in country_files) {
      country <- sub("_all_quarters.csv", "", f)
      cat("✓", country, "\n")
    }
  } else {
    cat("(No countries fully completed yet)\n")
  }
} else {
  cat("(No aggregated data yet)\n")
}

cat("\n")
cat("============================================================\n")
cat("         WHAT'S HAPPENING?                                  \n")
cat("============================================================\n\n")

cat("The script is downloading internet speed data for:\n")
cat("  • 11 CIS countries (Armenia, Azerbaijan, Belarus, etc.)\n")
cat("  • 26 quarters (2019 Q2 through 2025 Q3)\n")
cat("  • 2 network types (Fixed broadband and Mobile cellular)\n")
cat("  • Total: 572 separate data files\n\n")

cat("It processes one country at a time, with 3 tasks running\n")
cat("simultaneously per country (using 3 CPU cores).\n\n")

cat("Each task takes about 2-5 minutes depending on data size.\n\n")

cat("============================================================\n\n")

cat("TO CHECK PROGRESS AGAIN, run:\n")
cat("  Rscript scripts/data_download/check_progress.R\n\n")
