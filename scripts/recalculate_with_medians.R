# =============================================================================
# Recalculate Aggregations with Medians from Existing RDS Files
# =============================================================================
# This script reads the already-processed RDS files and recalculates
# aggregations to include both mean and median values
# =============================================================================

library(tidyverse)
library(sf)
library(here)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║     Recalculating Aggregations with Medians from Existing RDS        ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

# =============================================================================
# CONFIGURATION
# =============================================================================

PROCESSED_DIR <- here("data", "processed")
AGGREGATED_DIR <- here("data", "aggregated")

dir.create(AGGREGATED_DIR, recursive = TRUE, showWarnings = FALSE)

# Load config
config <- readRDS(here("config.rds"))

cat("Configuration:\n")
cat("  Processed files directory:", PROCESSED_DIR, "\n")
cat("  Output directory:", AGGREGATED_DIR, "\n")
cat("  Countries:", length(config$countries), "\n\n")

# =============================================================================
# FUNCTION: Aggregate Single RDS File
# =============================================================================

aggregate_rds_file <- function(file_path, config) {
  tryCatch({
    # Read the RDS file
    data_sf <- readRDS(file_path)
    
    # Check if it's already aggregated (single row) or raw tile data
    if (nrow(data_sf) == 1) {
      # This is already an aggregated file
      if (all(c("download_mbps", "upload_mbps", "latency_ms") %in% names(data_sf))) {
        # Check if it already has median values
        if (all(c("download_mbps_median", "upload_mbps_median", "latency_ms_median") %in% names(data_sf))) {
          return(data_sf)  # Already has medians, return as-is
        } else {
          # Has means but no medians - can't calculate medians from a single aggregated value
          cat("  ⚠️  Already aggregated (no tile data for median), adding NA medians\n")
          data_sf$download_mbps_median <- NA_real_
          data_sf$upload_mbps_median <- NA_real_
          data_sf$latency_ms_median <- NA_real_
          return(data_sf)
        }
      }
    }
    
    # Extract metadata from filename
    filename <- basename(file_path)
    parts <- str_match(filename, "^([A-Z]{2})_(\\d{4})Q(\\d)_(fixed|mobile)\\.rds$")
    
    if (is.na(parts[1])) {
      cat("  ✗ Invalid filename format\n")
      return(NULL)
    }
    
    country_code <- parts[2]
    year <- as.integer(parts[3])
    quarter <- as.integer(parts[4])
    network_type <- parts[5]
    
    # Check if this is sf object with geometry or already processed
    if (inherits(data_sf, "sf")) {
      # Raw tile data - aggregate it
      result_df <- data_sf %>%
        st_drop_geometry() %>%
        summarise(
          download_mbps = round(mean(avg_d_kbps / 1000, na.rm = TRUE), 2),
          upload_mbps = round(mean(avg_u_kbps / 1000, na.rm = TRUE), 2),
          latency_ms = round(mean(avg_lat_ms, na.rm = TRUE), 1),
          download_mbps_median = round(median(avg_d_kbps / 1000, na.rm = TRUE), 2),
          upload_mbps_median = round(median(avg_u_kbps / 1000, na.rm = TRUE), 2),
          latency_ms_median = round(median(avg_lat_ms, na.rm = TRUE), 1),
          tile_count = n(),
          total_tests = sum(tests, na.rm = TRUE),
          total_devices = sum(devices, na.rm = TRUE)
        ) %>%
        mutate(
          country_code = country_code,
          country_name = config$country_names[country_code],
          year = year,
          quarter = quarter,
          network_type = network_type,
          date = as.Date(paste0(year, "-", quarter * 3, "-01"))
        )
    } else {
      # Already processed data frame - just add metadata if missing
      result_df <- data_sf %>%
        mutate(
          country_code = country_code,
          country_name = config$country_names[country_code],
          year = year,
          quarter = quarter,
          network_type = network_type,
          date = as.Date(paste0(year, "-", quarter * 3, "-01"))
        )
    }
    
    return(result_df)
    
  }, error = function(e) {
    cat("  ✗ Error:", conditionMessage(e), "\n")
    return(NULL)
  })
}

# =============================================================================
# PROCESS ALL RDS FILES
# =============================================================================

# Get all RDS files
rds_files <- list.files(PROCESSED_DIR, pattern = "\\.rds$", full.names = TRUE)
rds_files <- rds_files[!grepl("^\\.gitkeep", basename(rds_files))]

cat(sprintf("Found %d RDS files to process\n\n", length(rds_files)))

# Process each file
all_results <- list()
processed_count <- 0
skipped_count <- 0
error_count <- 0

pb <- txtProgressBar(min = 0, max = length(rds_files), style = 3)

for (i in seq_along(rds_files)) {
  file_path <- rds_files[i]
  filename <- basename(file_path)
  
  setTxtProgressBar(pb, i)
  
  result <- aggregate_rds_file(file_path, config)
  
  if (!is.null(result)) {
    all_results[[i]] <- result
    processed_count <- processed_count + 1
  } else {
    error_count <- error_count + 1
  }
  
  # Periodic garbage collection
  if (i %% 50 == 0) {
    gc(verbose = FALSE)
  }
}

close(pb)

cat(sprintf("\n\n✓ Processed: %d files\n", processed_count))
cat(sprintf("✗ Errors: %d files\n", error_count))
cat(sprintf("⊙ Skipped: %d files\n\n", skipped_count))

# =============================================================================
# COMBINE AND SAVE RESULTS
# =============================================================================

if (length(all_results) > 0) {
  cat("Combining results...\n")
  
  final_results <- bind_rows(all_results[!sapply(all_results, is.null)])
  
  if (nrow(final_results) > 0) {
    # Save master file
    master_file <- file.path(AGGREGATED_DIR, "CIS_Internet_Speed_2019-2025.csv")
    write_csv(final_results, master_file)
    cat(sprintf("✓ Saved master file: %d records\n", nrow(final_results)))
    
    # Save country-specific files
    cat("\nSaving country-specific files...\n")
    for (country_code in config$countries) {
      country_data <- final_results %>%
        filter(country_code == !!country_code)
      
      if (nrow(country_data) > 0) {
        country_file <- file.path(AGGREGATED_DIR, 
                                 paste0(country_code, "_all_quarters.csv"))
        write_csv(country_data, country_file)
        cat(sprintf("  ✓ %s: %d records\n", country_code, nrow(country_data)))
      }
    }
    
    # Summary statistics
    cat("\nCalculating summary statistics...\n")
    summary_stats <- final_results %>%
      group_by(country_name, network_type) %>%
      summarise(
        quarters = n(),
        avg_download = round(mean(download_mbps, na.rm = TRUE), 1),
        avg_upload = round(mean(upload_mbps, na.rm = TRUE), 1),
        avg_latency = round(mean(latency_ms, na.rm = TRUE), 0),
        median_download = round(mean(download_mbps_median, na.rm = TRUE), 1),
        median_upload = round(mean(upload_mbps_median, na.rm = TRUE), 1),
        median_latency = round(mean(latency_ms_median, na.rm = TRUE), 0),
        total_tiles = sum(tile_count, na.rm = TRUE),
        .groups = "drop"
      ) %>%
      arrange(country_name, network_type)
    
    summary_file <- file.path(AGGREGATED_DIR, "summary_statistics.csv")
    write_csv(summary_stats, summary_file)
    
    cat("\n")
    cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
    cat("║                     Summary Statistics                               ║\n")
    cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
    
    print(summary_stats, n = Inf)
    
  } else {
    cat("⚠️  No results to save\n")
  }
} else {
  cat("⚠️  No data processed\n")
}

# =============================================================================
# FINAL SUMMARY
# =============================================================================

TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                  ✅ RECALCULATION COMPLETE!                          ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat(sprintf("⏱️  Total time: %.2f minutes\n", TOTAL_TIME))
cat(sprintf("📁 Output directory: %s\n", AGGREGATED_DIR))
cat(sprintf("📊 Total records: %d\n", nrow(final_results)))
cat(sprintf("🗂️  Country files: %d\n", length(config$countries)))
cat("\n")
