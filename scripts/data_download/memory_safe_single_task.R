# =============================================================================
# Memory-Safe Single Task Processor
# =============================================================================
# Use this to process individual tasks with maximum memory safety
# Useful for debugging OOM issues or processing specific quarters
# =============================================================================

library(tidyverse)
library(here)

# Source the modular function
source(here("scripts", "data_download", "process_ookla_data.R"))

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║         Memory-Safe Single Task Processor                            ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

# =============================================================================
# CONFIGURATION - MODIFY THESE
# =============================================================================

COUNTRY_CODE <- "AM"      # Change to your country code
YEAR <- 2024              # Change to your year
QUARTER <- 2              # Change to your quarter
NETWORK_TYPE <- "mobile"  # "fixed" or "mobile"

# =============================================================================
# PROCESS
# =============================================================================

OUTPUT_DIR <- here("data", "processed")
dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)

# Load config
config <- readRDS(here("config.rds"))
country_name <- config$country_names[COUNTRY_CODE]

cat("Task:", country_name, YEAR, "Q", QUARTER, toupper(NETWORK_TYPE), "\n")
cat("Output:", OUTPUT_DIR, "\n\n")

# Check if already done
output_file <- file.path(
  OUTPUT_DIR,
  paste0(COUNTRY_CODE, "_", YEAR, "Q", QUARTER, "_", NETWORK_TYPE, ".rds")
)

if (file.exists(output_file)) {
  cat("⚠️  This task is already completed!\n")
  cat("   File:", output_file, "\n\n")
  cat("Delete this file if you want to reprocess.\n")
  quit(save = "no")
}

# Load boundaries
boundaries_file <- here("data", "boundaries", "cis_countries.rds")
if (!file.exists(boundaries_file)) {
  stop("Boundaries not found. Run setup first.")
}
cis_countries <- readRDS(boundaries_file)

# Show memory before
cat("Memory before processing:\n")
mem_before <- gc(full = TRUE)
print(mem_before)
cat("\n")

# Process
cat("Starting processing...\n\n")
start_time <- Sys.time()

result <- tryCatch({
  process_ookla_data(
    country_code = COUNTRY_CODE,
    year = YEAR,
    quarter = QUARTER,
    network_type = NETWORK_TYPE,
    cis_countries = cis_countries,
    config = config,
    save_output = TRUE,
    output_dir = OUTPUT_DIR,
    verbose = TRUE
  )
}, error = function(e) {
  cat("\n❌ ERROR:", conditionMessage(e), "\n")
  cat("\nFull error details:\n")
  print(e)
  return(NULL)
})

elapsed <- difftime(Sys.time(), start_time, units = "mins")

# Show memory after
cat("\nMemory after processing:\n")
mem_after <- gc(full = TRUE)
print(mem_after)
cat("\n")

# Memory increase
mem_increase <- mem_after[2, 2] - mem_before[2, 2]
cat("Memory increase:", round(mem_increase, 1), "MB\n\n")

# Results
if (!is.null(result)) {
  cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
  cat("║                         ✅ SUCCESS!                                  ║\n")
  cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
  
  cat("Time:", round(elapsed, 2), "minutes\n")
  cat("Output:", output_file, "\n\n")
  
  cat("Results:\n")
  print(result)
} else {
  cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
  cat("║                         ❌ FAILED!                                   ║\n")
  cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
  
  cat("The task failed. Check the error message above.\n")
  cat("Common issues:\n")
  cat("  • Out of memory (OOM) - try closing other applications\n")
  cat("  • Network timeout - check internet connection\n")
  cat("  • Data not available for this quarter\n")
}

cat("\n")
