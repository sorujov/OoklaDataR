# =============================================================================
# OoklaDataR - Master Pipeline Script (Non-Interactive)
# =============================================================================
# Purpose: Run the complete data pipeline from download to aggregation
# Author: OoklaDataR Project
# Date: 2025-10-27
# Note: This version skips the confirmation prompt
# =============================================================================

library(here)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                     OoklaDataR Master Pipeline                       ║\n")
cat("║               Download and Process Ookla Data for CIS                ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

# Configuration
START_TIME <- Sys.time()

# Check if setup has been run
if (!file.exists(here("config.rds"))) {
  cat("✗ Configuration file not found!\n")
  cat("Please run scripts/00_setup.R first\n\n")
  quit(save = "no", status = 1)
}

config <- readRDS(here("config.rds"))

cat("Configuration loaded:\n")
cat("  Countries:", length(config$countries), "CIS countries\n")
cat("  Time period:", config$start_year, "Q", config$start_quarter, "to",
    config$end_year, "Q", config$end_quarter, "\n")
cat("  Data types:", paste(config$data_types, collapse = ", "), "\n\n")

# Info about what will happen
cat("ℹ️  PIPELINE INFO:\n")
cat("  - Processing 26 quarters (2019 Q2 - 2025 Q3)\n")
cat("  - Downloading ~5-13 GB total (temporary files)\n")
cat("  - Final storage: < 5 GB (raw files auto-deleted)\n")
cat("  - Estimated time: 2-6 hours\n\n")

cat("Starting automatically...\n\n")

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                     Starting Data Pipeline                           ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

# Step 1: Download Data
cat("\n")
cat("┌─────────────────────────────────────────────────────────────────────┐\n")
cat("│ STEP 1: Downloading Ookla Data from AWS S3                         │\n")
cat("└─────────────────────────────────────────────────────────────────────┘\n\n")

step1_start <- Sys.time()

tryCatch({
  source(here("scripts", "01_download_data.R"), local = TRUE)
  step1_time <- difftime(Sys.time(), step1_start, units = "mins")
  cat("\n✓ Step 1 completed in", round(step1_time, 1), "minutes\n")
}, error = function(e) {
  cat("\n✗ Error in Step 1:", e$message, "\n")
  quit(save = "no", status = 1)
})

# Step 2: Aggregate Data
cat("\n")
cat("┌─────────────────────────────────────────────────────────────────────┐\n")
cat("│ STEP 2: Aggregating Data to Time Series                            │\n")
cat("└─────────────────────────────────────────────────────────────────────┘\n\n")

step2_start <- Sys.time()

# Check if we have processed files
processed_files <- list.files(here("data", "processed"), pattern = "\\.rds$")

if (length(processed_files) == 0) {
  cat("⚠️  No processed files found. Skipping aggregation.\n")
} else {
  tryCatch({
    source(here("scripts", "03_aggregate_data.R"), local = TRUE)
    step2_time <- difftime(Sys.time(), step2_start, units = "mins")
    cat("\n✓ Step 2 completed in", round(step2_time, 1), "minutes\n")
  }, error = function(e) {
    cat("\n✗ Error in Step 2:", e$message, "\n")
  })
}

# Step 3: Extract Azerbaijan Tiles
cat("\n")
cat("┌─────────────────────────────────────────────────────────────────────┐\n")
cat("│ STEP 3: Extracting Azerbaijan Tiles for Mapping                    │\n")
cat("└─────────────────────────────────────────────────────────────────────┘\n\n")

step3_start <- Sys.time()

# Check if we have Azerbaijan data
az_files <- list.files(here("data", "processed"), pattern = "^AZ_.*\\.rds$")

if (length(az_files) == 0) {
  cat("⚠️  No Azerbaijan data found. Skipping tile extraction.\n")
} else {
  tryCatch({
    source(here("scripts", "04_azerbaijan_tiles.R"), local = TRUE)
    step3_time <- difftime(Sys.time(), step3_start, units = "mins")
    cat("\n✓ Step 3 completed in", round(step3_time, 1), "minutes\n")
  }, error = function(e) {
    cat("\n✗ Error in Step 3:", e$message, "\n")
  })
}

# Final Summary
TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")

cat("\n\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                        Pipeline Complete!                            ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("Total processing time:", round(TOTAL_TIME, 1), "minutes\n\n")

cat("Output files created:\n")
cat("  📊 data/aggregated/Fixed.csv - Fixed broadband time series\n")
cat("  📊 data/aggregated/Cellular.csv - Mobile network time series\n")
cat("  📊 data/aggregated/All_Statistics_Detailed.csv - Detailed statistics\n")
cat("  📊 data/aggregated/Azerbaijan_TimeSeries.csv - Azerbaijan summary\n")
cat("  🗺️  data/processed/azerbaijan_tiles/ - Azerbaijan mapping data\n\n")

# Check output files
if (file.exists(here("data", "aggregated", "Fixed.csv"))) {
  fixed <- read.csv(here("data", "aggregated", "Fixed.csv"))
  cat("✓ Fixed broadband data:", nrow(fixed), "records,",
      length(unique(fixed$Country)), "countries\n")
}

if (file.exists(here("data", "aggregated", "Cellular.csv"))) {
  cellular <- read.csv(here("data", "aggregated", "Cellular.csv"))
  cat("✓ Cellular data:", nrow(cellular), "records,",
      length(unique(cellular$Country)), "countries\n")
}

az_tiles <- list.files(here("data", "processed", "azerbaijan_tiles"), 
                       pattern = "\\.gpkg$")
if (length(az_tiles) > 0) {
  cat("✓ Azerbaijan tiles:", length(az_tiles), "GeoPackage files\n")
}

cat("\n\n🎉 Success! Your Ookla CIS data is ready for analysis.\n\n")

cat("Next steps:\n")
cat("  1. Explore data/aggregated/ for time series analysis\n")
cat("  2. Use azerbaijan_tiles/ for detailed mapping\n")
cat("  3. Create visualizations with R, Python, or QGIS\n")
cat("  4. Replicate Internet-Evolution analysis methodology\n\n")
