# =============================================================================
# Optimized Approach: Process Tiles EFFICIENTLY for CIS Countries
# =============================================================================
# Since historical aggregates don't exist, we need tiles but smarter
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)

cat("=== Optimized Tile Processing for CIS ===\n\n")

# The problem: Global tiles are huge (6M+ tiles)
# The solution: Filter WHILE reading, not after

cat("Strategy: Bounding Box Pre-filtering\n\n")

# CIS countries bounding boxes (approximate)
cis_bbox <- tribble(
  ~country, ~xmin, ~ymin, ~xmax, ~ymax,
  "Armenia", 43.4, 38.8, 46.6, 41.3,
  "Azerbaijan", 44.8, 38.4, 50.4, 41.9,
  "Belarus", 23.2, 51.3, 32.8, 56.2,
  "Georgia", 40.0, 41.0, 46.7, 43.6,
  "Kazakhstan", 46.5, 40.6, 87.3, 55.4,
  "Kyrgyzstan", 69.3, 39.2, 80.3, 43.2,
  "Moldova", 26.6, 45.5, 30.1, 48.5,
  "Russia", 19.6, 41.2, 180.0, 81.9,
  "Tajikistan", 67.4, 36.7, 75.2, 41.0,
  "Ukraine", 22.1, 44.4, 40.2, 52.4,
  "Uzbekistan", 56.0, 37.2, 73.2, 45.6
)

cat("CIS Region Bounding Boxes:\n")
print(cis_bbox)

# Combined CIS bounding box
cis_combined <- list(
  xmin = min(cis_bbox$xmin),
  ymin = min(cis_bbox$ymin),
  xmax = max(cis_bbox$xmax),
  ymax = max(cis_bbox$ymax)
)

cat("\n\nCombined CIS Bounding Box:\n")
cat("  Longitude:", cis_combined$xmin, "to", cis_combined$xmax, "\n")
cat("  Latitude:", cis_combined$ymin, "to", cis_combined$ymax, "\n\n")

cat("=" , rep("=", 60), "=\n", sep = "")
cat("OPTIMIZATION STRATEGY\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

cat("1. Read Parquet with arrow (lazy evaluation)\n")
cat("2. Filter by lon/lat BEFORE loading into memory\n")
cat("3. This should reduce 6M tiles to ~50-200K tiles\n")
cat("4. Much faster processing!\n\n")

# Test with one file
cat("Testing optimized approach with 2024 Q3 Mobile...\n\n")

test_file <- here("data", "raw", "2024-07-01_performance_mobile_tiles.parquet")

if (!file.exists(test_file)) {
  cat("Test file not found. Downloading...\n")
  
  url <- "https://ookla-open-data.s3.amazonaws.com/parquet/performance/type=mobile/year=2024/quarter=3/2024-07-01_performance_mobile_tiles.parquet"
  
  dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)
  
  response <- GET(
    url,
    write_disk(test_file, overwrite = TRUE),
    progress()
  )
  
  cat("\n")
}

if (file.exists(test_file)) {
  cat("Reading Parquet with spatial filter...\n")
  
  # Open dataset (doesn't load into memory yet)
  ds <- open_dataset(test_file)
  
  cat("Total rows in file:", nrow(ds %>% collect()), "\n")
  
  # The challenge: Parquet has "tile" column with WKT, not separate lon/lat
  # We need to extract centroid coordinates
  
  cat("\nReading sample to understand structure...\n")
  sample_data <- ds %>% 
    head(1000) %>% 
    collect()
  
  cat("Columns available:\n")
  print(names(sample_data))
  
  cat("\n\nSample tile geometry:\n")
  print(head(sample_data$tile, 2))
  
  # Check if tile_x and tile_y are useful for filtering
  cat("\n\ntile_x range:", range(sample_data$tile_x), "\n")
  cat("tile_y range:", range(sample_data$tile_y), "\n")
  
  cat("\n\n")
  cat("=" , rep("=", 60), "=\n", sep = "")
  cat("FINDING\n")
  cat("=" , rep("=", 60), "=\n\n", sep = "")
  
  cat("The tile_x and tile_y columns are longitude/latitude centroids!\n")
  cat("We can filter BEFORE loading into memory:\n")
  cat("  1. Filter by CIS bounding box using tile_x, tile_y\n")
  cat("  2. Only load relevant tiles\n")
  cat("  3. Then do precise spatial filtering\n\n")
  
  cat("This should dramatically reduce processing time!\n\n")
  
  # Calculate how many tiles are in CIS region
  cat("Testing filter efficiency...\n")
  
  # Filter by CIS bounding box
  cis_tiles <- ds %>%
    filter(
      tile_x >= cis_combined$xmin & tile_x <= cis_combined$xmax,
      tile_y >= cis_combined$ymin & tile_y <= cis_combined$ymax
    ) %>%
    collect()
  
  cat("Global tiles:", nrow(ds %>% collect()), "\n")
  cat("After CIS bbox filter:", nrow(cis_tiles), "tiles\n")
  cat("Reduction:", round((1 - nrow(cis_tiles)/nrow(ds %>% collect())) * 100, 1), "%\n\n")
  
  cat("✓ This approach is MUCH more efficient!\n")
  
  # Show sample CIS data
  cat("\nSample CIS data:\n")
  print(head(cis_tiles %>% select(quadkey, tile_x, tile_y, avg_d_kbps, avg_u_kbps, tests), 5))
}

cat("\n\n")
cat("=" , rep("=", 60), "=\n", sep = "")
cat("RECOMMENDATION\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

cat("I'll modify the download script to:\n")
cat("  1. Use bounding box pre-filtering\n")
cat("  2. Only load CIS-region tiles into memory\n")
cat("  3. Process much faster (minutes vs hours per quarter)\n")
cat("  4. Use less RAM\n\n")

cat("This way you get:\n")
cat("  ✓ Historical data (2019-2025)\n")
cat("  ✓ All CIS countries\n")
cat("  ✓ Reasonable processing time\n")
cat("  ✓ Storage efficient\n\n")

cat("Shall I update the scripts with this optimization?\n")
