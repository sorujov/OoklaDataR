# =============================================================================
# Test Checkpoint System
# =============================================================================
# Quick test to verify resume logic works correctly
# =============================================================================

library(tidyverse)
library(here)

cat("Testing Checkpoint/Resume System\n")
cat("=================================\n\n")

# Load config
config <- readRDS(here("config.rds"))

# All quarters (2019 Q2 - 2025 Q3)
ALL_QUARTERS <- expand.grid(
  year = 2019:2025,
  quarter = 1:4,
  stringsAsFactors = FALSE
) %>%
  filter(!(year == 2019 & quarter == 1)) %>%
  filter(!(year == 2025 & quarter == 4)) %>%
  arrange(year, quarter)

NETWORK_TYPES <- c("fixed", "mobile")
OUTPUT_DIR <- here("data", "processed")

# Test with Armenia
country_code <- "AM"
country_name <- "Armenia"

cat("Testing for:", country_name, "\n")
cat("Expected tasks per country:", nrow(ALL_QUARTERS) * length(NETWORK_TYPES), "\n\n")

# Create task list - FIXED VERSION
tasks <- ALL_QUARTERS %>%
  crossing(network_type = NETWORK_TYPES)

cat("Tasks created:", nrow(tasks), "\n")

# Check for completed tasks
tasks$output_file <- file.path(
  OUTPUT_DIR,
  paste0(country_code, "_", tasks$year, "Q", tasks$quarter, "_", 
         tasks$network_type, ".rds")
)
tasks$completed <- file.exists(tasks$output_file)

tasks_remaining <- tasks %>% filter(!completed)

cat("Completed tasks:", sum(tasks$completed), "\n")
cat("Remaining tasks:", nrow(tasks_remaining), "\n\n")

if (sum(tasks$completed) > 0) {
  cat("✓ CHECKPOINT WORKING!\n")
  cat("  These files will be SKIPPED:\n\n")
  
  completed_tasks <- tasks %>% filter(completed)
  print(completed_tasks %>% 
          select(year, quarter, network_type) %>% 
          head(10))
  
  if (nrow(completed_tasks) > 10) {
    cat("\n  ... and", nrow(completed_tasks) - 10, "more\n")
  }
} else {
  cat("✗ No completed files found\n")
}

cat("\n")
if (nrow(tasks_remaining) > 0) {
  cat("Next tasks to process:\n\n")
  print(tasks_remaining %>% 
          select(year, quarter, network_type) %>% 
          head(5))
} else {
  cat("✓ All tasks complete for", country_name, "!\n")
}

cat("\n=================================\n")
cat("Test complete!\n")
