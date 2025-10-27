# =============================================================================
# OoklaDataR - Data Aggregation Script
# =============================================================================
# Purpose: Aggregate filtered tile data to country-level time series
# Author: OoklaDataR Project  
# Date: 2025-10-27
# =============================================================================

library(tidyverse)
library(sf)
library(here)
library(lubridate)

cat("=== OoklaDataR - Data Aggregation ===\n\n")

# Load configuration
config <- readRDS(here("config.rds"))

# Create output directory
dir.create(here("data", "aggregated"), recursive = TRUE, showWarnings = FALSE)

# Function to extract date and type from filename
parse_filename <- function(filename) {
  # Example: AZ_2024-07-01_performance_mobile_tiles.rds
  parts <- str_split(filename, "_")[[1]]
  
  country <- parts[1]
  date_str <- parts[2]
  type <- parts[4]
  
  list(
    country = country,
    date = ymd(date_str),
    type = type
  )
}

# Function to aggregate a single file
aggregate_file <- function(file_path) {
  
  info <- parse_filename(basename(file_path))
  
  tryCatch({
    # Read RDS file
    tiles_sf <- readRDS(file_path)
    
    if (nrow(tiles_sf) == 0) {
      return(NULL)
    }
    
    # Drop geometry for faster processing
    tiles_df <- st_drop_geometry(tiles_sf)
    
    # Calculate statistics
    stats <- tibble(
      country = info$country,
      country_name = config$country_names[info$country],
      date = info$date,
      year = year(info$date),
      quarter = quarter(info$date),
      month = month(info$date),
      type = info$type,
      
      # Download speed (convert from kbps to Mbps)
      download_median_mbps = median(tiles_df$avg_d_kbps, na.rm = TRUE) / 1000,
      download_mean_mbps = mean(tiles_df$avg_d_kbps, na.rm = TRUE) / 1000,
      download_min_mbps = min(tiles_df$avg_d_kbps, na.rm = TRUE) / 1000,
      download_max_mbps = max(tiles_df$avg_d_kbps, na.rm = TRUE) / 1000,
      
      # Upload speed (convert from kbps to Mbps)
      upload_median_mbps = median(tiles_df$avg_u_kbps, na.rm = TRUE) / 1000,
      upload_mean_mbps = mean(tiles_df$avg_u_kbps, na.rm = TRUE) / 1000,
      upload_min_mbps = min(tiles_df$avg_u_kbps, na.rm = TRUE) / 1000,
      upload_max_mbps = max(tiles_df$avg_u_kbps, na.rm = TRUE) / 1000,
      
      # Latency (ms)
      latency_median_ms = median(tiles_df$avg_lat_ms, na.rm = TRUE),
      latency_mean_ms = mean(tiles_df$avg_lat_ms, na.rm = TRUE),
      latency_min_ms = min(tiles_df$avg_lat_ms, na.rm = TRUE),
      latency_max_ms = max(tiles_df$avg_lat_ms, na.rm = TRUE),
      
      # Test counts
      total_tests = sum(tiles_df$tests, na.rm = TRUE),
      total_devices = sum(tiles_df$devices, na.rm = TRUE),
      tile_count = nrow(tiles_df)
    )
    
    return(stats)
    
  }, error = function(e) {
    warning("Error processing ", basename(file_path), ": ", e$message)
    return(NULL)
  })
}

# Process all RDS files
cat("Loading processed files...\n")

processed_files <- list.files(
  here("data", "processed"),
  pattern = "\\.rds$",
  full.names = TRUE
)

if (length(processed_files) == 0) {
  cat("✗ No processed files found in data/processed/\n")
  cat("Run scripts/02_filter_by_country.R first\n")
  quit(save = "no", status = 1)
}

cat("Found", length(processed_files), "files to aggregate\n\n")

cat("Aggregating data...\n")

# Aggregate all files
all_stats <- processed_files %>%
  map_dfr(aggregate_file) %>%
  arrange(country, type, date)

if (nrow(all_stats) == 0) {
  cat("✗ No data aggregated\n")
  quit(save = "no", status = 1)
}

cat("✓ Aggregated", nrow(all_stats), "records\n\n")

# Summary
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Aggregation Summary\n")
cat("=" , rep("=", 60), "=\n", sep = "")

cat("\nCountries:", n_distinct(all_stats$country), "\n")
print(table(all_stats$country))

cat("\nNetwork types:", n_distinct(all_stats$type), "\n")
print(table(all_stats$type))

cat("\nDate range:", min(all_stats$date), "to", max(all_stats$date), "\n")

# Split by network type and save
cat("\n\nSaving aggregated data...\n")

# Fixed broadband
fixed_data <- all_stats %>%
  filter(type == "fixed") %>%
  select(
    Country = country,
    Country_Name = country_name,
    Date = date,
    Year = year,
    Quarter = quarter,
    Month = month,
    Download_Median_Mbps = download_median_mbps,
    Download_Mean_Mbps = download_mean_mbps,
    Upload_Median_Mbps = upload_median_mbps,
    Upload_Mean_Mbps = upload_mean_mbps,
    Latency_Median_ms = latency_median_ms,
    Latency_Mean_ms = latency_mean_ms,
    Total_Tests = total_tests,
    Total_Devices = total_devices,
    Tile_Count = tile_count
  )

if (nrow(fixed_data) > 0) {
  write_csv(fixed_data, here("data", "aggregated", "Fixed.csv"))
  cat("✓ Fixed broadband:", nrow(fixed_data), "records →", 
      "data/aggregated/Fixed.csv\n")
}

# Mobile/Cellular
mobile_data <- all_stats %>%
  filter(type == "mobile") %>%
  select(
    Country = country,
    Country_Name = country_name,
    Date = date,
    Year = year,
    Quarter = quarter,
    Month = month,
    Download_Median_Mbps = download_median_mbps,
    Download_Mean_Mbps = download_mean_mbps,
    Upload_Median_Mbps = upload_median_mbps,
    Upload_Mean_Mbps = upload_mean_mbps,
    Latency_Median_ms = latency_median_ms,
    Latency_Mean_ms = latency_mean_ms,
    Total_Tests = total_tests,
    Total_Devices = total_devices,
    Tile_Count = tile_count
  )

if (nrow(mobile_data) > 0) {
  write_csv(mobile_data, here("data", "aggregated", "Cellular.csv"))
  cat("✓ Mobile/Cellular:", nrow(mobile_data), "records →", 
      "data/aggregated/Cellular.csv\n")
}

# Save detailed version with all statistics
write_csv(all_stats, here("data", "aggregated", "All_Statistics_Detailed.csv"))
cat("✓ Detailed statistics →", 
    "data/aggregated/All_Statistics_Detailed.csv\n")

# Create summary report
cat("\n\n")
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Quick Preview - Recent Data\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

recent_data <- all_stats %>%
  filter(date >= max(date) - months(3)) %>%
  select(country_name, date, type, download_median_mbps, 
         upload_median_mbps, latency_median_ms) %>%
  arrange(desc(date), country_name, type)

print(recent_data, n = 20)

cat("\n\n✓ Aggregation complete!\n")
cat("\nOutput files created:\n")
cat("  - data/aggregated/Fixed.csv\n")
cat("  - data/aggregated/Cellular.csv\n")
cat("  - data/aggregated/All_Statistics_Detailed.csv\n")
cat("\nNext step: Run scripts/04_azerbaijan_tiles.R for detailed Azerbaijan mapping data\n")
