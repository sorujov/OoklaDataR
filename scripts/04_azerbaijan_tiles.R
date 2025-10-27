# =============================================================================
# OoklaDataR - Azerbaijan Tile Extraction Script
# =============================================================================
# Purpose: Extract detailed Azerbaijan tiles for mapping and visualization
# Author: OoklaDataR Project
# Date: 2025-10-27
# =============================================================================

library(tidyverse)
library(sf)
library(here)
library(lubridate)

cat("=== OoklaDataR - Azerbaijan Tile Extraction ===\n\n")

# Load configuration
config <- readRDS(here("config.rds"))

# Create output directory for Azerbaijan tiles
az_dir <- here("data", "processed", "azerbaijan_tiles")
dir.create(az_dir, recursive = TRUE, showWarnings = FALSE)

# Function to extract date and type from filename
parse_filename <- function(filename) {
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

# Find all Azerbaijan files
cat("Searching for Azerbaijan data files...\n")

az_files <- list.files(
  here("data", "processed"),
  pattern = "^AZ_.*\\.rds$",
  full.names = TRUE
)

if (length(az_files) == 0) {
  cat("✗ No Azerbaijan files found\n")
  cat("Make sure data has been downloaded and filtered first\n")
  quit(save = "no", status = 1)
}

cat("Found", length(az_files), "Azerbaijan data files\n\n")

# Process each file
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Extracting Azerbaijan tiles\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

processed_count <- 0

for (file_path in az_files) {
  info <- parse_filename(basename(file_path))
  
  cat("Processing:", basename(file_path), "\n")
  
  tryCatch({
    # Read Azerbaijan tiles
    tiles_sf <- readRDS(file_path)
    
    if (nrow(tiles_sf) == 0) {
      cat("  ✗ Empty file, skipping\n\n")
      next
    }
    
    cat("  Loaded", nrow(tiles_sf), "tiles\n")
    
    # Add metadata columns
    tiles_sf <- tiles_sf %>%
      mutate(
        date = info$date,
        year = year(info$date),
        quarter = quarter(info$date),
        month = month(info$date),
        type = info$type,
        # Convert speeds to Mbps
        download_mbps = avg_d_kbps / 1000,
        upload_mbps = avg_u_kbps / 1000,
        latency_ms = avg_lat_ms
      )
    
    # Create output filename
    output_file <- file.path(
      az_dir,
      paste0(
        "AZ_", 
        format(info$date, "%Y%m%d"),
        "_", info$type,
        "_tiles.gpkg"
      )
    )
    
    # Save as GeoPackage (better than shapefile for complex data)
    st_write(
      tiles_sf, 
      output_file, 
      driver = "GPKG",
      delete_dsn = TRUE,
      quiet = TRUE
    )
    
    cat("  ✓ Saved", nrow(tiles_sf), "tiles to", basename(output_file), "\n")
    
    # Calculate and display summary statistics
    stats <- tiles_sf %>%
      st_drop_geometry() %>%
      summarise(
        median_download = median(download_mbps, na.rm = TRUE),
        median_upload = median(upload_mbps, na.rm = TRUE),
        median_latency = median(latency_ms, na.rm = TRUE),
        total_tests = sum(tests, na.rm = TRUE)
      )
    
    cat("  Stats: Download:", round(stats$median_download, 1), "Mbps,",
        "Upload:", round(stats$median_upload, 1), "Mbps,",
        "Latency:", round(stats$median_latency, 1), "ms\n")
    
    processed_count <- processed_count + 1
    cat("\n")
    
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n\n")
  })
}

# Create time series summary for Azerbaijan
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Creating Azerbaijan Time Series Summary\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

az_gpkg_files <- list.files(az_dir, pattern = "\\.gpkg$", full.names = TRUE)

if (length(az_gpkg_files) > 0) {
  
  # Extract summary from each GeoPackage
  az_summary <- az_gpkg_files %>%
    map_dfr(function(file) {
      tryCatch({
        tiles <- st_read(file, quiet = TRUE)
        tiles %>%
          st_drop_geometry() %>%
          summarise(
            date = first(date),
            year = first(year),
            quarter = first(quarter),
            month = first(month),
            type = first(type),
            download_median = median(download_mbps, na.rm = TRUE),
            download_mean = mean(download_mbps, na.rm = TRUE),
            upload_median = median(upload_mbps, na.rm = TRUE),
            upload_mean = mean(upload_mbps, na.rm = TRUE),
            latency_median = median(latency_ms, na.rm = TRUE),
            latency_mean = mean(latency_ms, na.rm = TRUE),
            tile_count = n(),
            total_tests = sum(tests, na.rm = TRUE),
            total_devices = sum(devices, na.rm = TRUE)
          )
      }, error = function(e) NULL)
    }) %>%
    arrange(date, type)
  
  # Save summary
  summary_file <- here("data", "aggregated", "Azerbaijan_TimeSeries.csv")
  write_csv(az_summary, summary_file)
  cat("✓ Azerbaijan time series saved to:", basename(summary_file), "\n\n")
  
  # Display summary
  cat("Azerbaijan Data Overview:\n")
  cat("  Date range:", min(az_summary$date), "to", max(az_summary$date), "\n")
  cat("  Network types:", paste(unique(az_summary$type), collapse = ", "), "\n")
  cat("  Total records:", nrow(az_summary), "\n\n")
  
  # Show recent speeds
  cat("Recent Azerbaijan Speeds:\n")
  recent <- az_summary %>%
    filter(date >= max(date) - months(6)) %>%
    select(date, type, download_median, upload_median, latency_median, tile_count) %>%
    arrange(desc(date), type)
  
  print(recent, n = 20)
}

cat("\n\n=" , rep("=", 60), "=\n", sep = "")
cat("Azerbaijan Extraction Complete\n")
cat("=" , rep("=", 60), "=\n", sep = "")

cat("\nProcessed", processed_count, "files\n")
cat("\nOutput location:", az_dir, "\n")
cat("\nFiles created:\n")

output_files <- list.files(az_dir, pattern = "\\.gpkg$")
for (f in output_files) {
  file_size <- file.info(file.path(az_dir, f))$size / (1024^2)
  cat("  -", f, "(", round(file_size, 1), "MB )\n")
}

cat("\n✓ Azerbaijan tiles ready for mapping!\n")
cat("\nThese GeoPackage files can be used with:\n")
cat("  - QGIS (open source GIS software)\n")
cat("  - R packages: sf, tmap, leaflet, mapview\n")
cat("  - Python: geopandas, folium\n")
cat("\nNext: Create visualizations with scripts/05_create_maps.R (coming soon)\n")
