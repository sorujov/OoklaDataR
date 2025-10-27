# =============================================================================
# Test Azerbaijan Data - Single Quarter  
# =============================================================================
# Purpose: Quick test with just Azerbaijan to verify pipeline works
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)
library(rnaturalearth)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║           Azerbaijan Test: 2024 Q3 (Fixed + Mobile)                  ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

# Configuration
config <- readRDS(here("config.rds"))
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

# Azerbaijan bounding box (much smaller than all CIS!)
AZ_BBOX <- list(xmin = 44.8, ymin = 38.4, xmax = 50.4, ymax = 41.9)

TEST_YEAR <- 2024
TEST_QUARTER <- 3

cat("Testing with: Azerbaijan only,", TEST_YEAR, "Q", TEST_QUARTER, "\n")
cat("Bounding box: Lon", AZ_BBOX$xmin, "-", AZ_BBOX$xmax, 
    ", Lat", AZ_BBOX$ymin, "-", AZ_BBOX$ymax, "\n\n")

# Create directories
dir.create(here("data", "temp_test"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "boundaries"), recursive = TRUE, showWarnings = FALSE)

# Get Azerbaijan boundary
cat("Loading Azerbaijan boundary...\n")
boundaries_file <- here("data", "boundaries", "cis_countries.rds")

if (!file.exists(boundaries_file)) {
  world <- ne_countries(scale = "medium", returnclass = "sf")
  cis_countries <- world %>%
    filter(iso_a2 %in% config$countries) %>%
    select(country = name, iso_a2, geometry) %>%
    st_make_valid()
  saveRDS(cis_countries, boundaries_file)
  azerbaijan <- cis_countries %>% filter(iso_a2 == "AZ")
} else {
  cis_countries <- readRDS(boundaries_file)
  azerbaijan <- cis_countries %>% filter(iso_a2 == "AZ")
}

cat("✓ Azerbaijan boundary loaded\n\n")

# Process both network types
all_results <- list()

for (network_type in c("mobile", "fixed")) {
  
  cat("┌─────────────────────────────────────────────────────────────────────┐\n")
  cat("│", toupper(network_type), "Network                                              │\n")
  cat("└─────────────────────────────────────────────────────────────────────┘\n\n")
  
  # Build S3 path
  prefix <- paste0(
    config$s3_prefix,
    "type=", network_type,
    "/year=", TEST_YEAR,
    "/quarter=", TEST_QUARTER,
    "/"
  )
  
  # List and download
  cat("1. Listing files in S3...\n")
  list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", URLencode(prefix, reserved = TRUE))
  response <- GET(list_url, timeout(60))
  
  if (status_code(response) != 200) {
    cat("✗ Failed to list files\n\n")
    next
  }
  
  content_text <- content(response, "text", encoding = "UTF-8")
  keys <- str_extract_all(content_text, "<Key>([^<]+\\.parquet)</Key>")[[1]]
  keys <- str_replace_all(keys, "</?Key>", "")
  
  if (length(keys) == 0) {
    cat("✗ No files found\n\n")
    next
  }
  
  cat("✓ Found", length(keys), "file(s)\n\n")
  
  # Download
  parquet_file <- here("data", "raw", basename(keys[1]))
  
  # Check if file exists and has reasonable size (>50 MB for Ookla data)
  needs_download <- TRUE
  if (file.exists(parquet_file)) {
    file_size <- file.info(parquet_file)$size / (1024^2)
    if (file_size > 50) {
      cat("2. File already exists\n")
      cat("   Size:", round(file_size, 1), "MB\n\n")
      needs_download <- FALSE
    } else {
      cat("2. File exists but too small (", round(file_size, 1), "MB) - re-downloading...\n")
      file.remove(parquet_file)
    }
  }
  
  if (needs_download) {
    cat("2. Downloading...\n")
    dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)
    url <- paste0(S3_BASE, "/", keys[1])
    
    # Try download with retries
    max_retries <- 3
    for (attempt in 1:max_retries) {
      dl_resp <- GET(url, write_disk(parquet_file, overwrite = TRUE), progress(), timeout(300))
      
      if (status_code(dl_resp) == 200) {
        file_size <- file.info(parquet_file)$size / (1024^2)
        cat("\n✓ Downloaded:", round(file_size, 1), "MB\n\n")
        
        # Verify file size is reasonable
        if (file_size < 50) {
          cat("⚠ Warning: File size suspiciously small, retrying...\n")
          if (attempt < max_retries) {
            Sys.sleep(2)
            next
          }
        }
        break
      } else {
        cat("✗ Download failed (attempt", attempt, "of", max_retries, ")\n")
        if (attempt < max_retries) Sys.sleep(2)
      }
    }
  }
  
  # Process with AZERBAIJAN bbox filter
  cat("3. Reading with Azerbaijan bbox filter...\n")
  
  tryCatch({
    ds <- open_dataset(parquet_file)
    
    # Filter by Azerbaijan bounding box ONLY (much faster!)
    az_tiles <- ds %>%
      filter(
        tile_x >= AZ_BBOX$xmin & tile_x <= AZ_BBOX$xmax,
        tile_y >= AZ_BBOX$ymin & tile_y <= AZ_BBOX$ymax
      ) %>%
      collect()
    
    cat("✓ Filtered to", nrow(az_tiles), "Azerbaijan tiles\n")
    cat("  (Reduced from millions to thousands!)\n\n")
    
    if (nrow(az_tiles) == 0) {
      cat("✗ No Azerbaijan tiles found\n\n")
      next
    }
    
    # Convert to spatial
    cat("4. Converting to spatial features...\n")
    tiles_sf <- st_as_sf(az_tiles, wkt = "tile", crs = 4326)
    tiles_sf <- st_make_valid(tiles_sf)
    cat("✓ Created spatial features\n\n")
    
    # Precise intersection with Azerbaijan
    cat("5. Matching with Azerbaijan boundary...\n")
    az_final <- st_intersection(tiles_sf, azerbaijan)
    cat("✓ Final Azerbaijan tiles:", nrow(az_final), "\n\n")
    
    # Calculate statistics
    cat("6. Computing statistics...\n")
    stats <- az_final %>%
      st_drop_geometry() %>%
      summarise(
        download_mbps = round(median(avg_d_kbps / 1000, na.rm = TRUE), 1),
        upload_mbps = round(median(avg_u_kbps / 1000, na.rm = TRUE), 1),
        latency_ms = round(median(avg_lat_ms, na.rm = TRUE), 0),
        tile_count = n(),
        total_tests = sum(tests, na.rm = TRUE)
      )
    
    cat("✓ Statistics calculated\n\n")
    
    # Save
    output_file <- here("data", "temp_test", 
                       paste0("AZ_", TEST_YEAR, "Q", TEST_QUARTER, "_", network_type, ".rds"))
    saveRDS(az_final, output_file)
    
    # Store results
    all_results[[network_type]] <- stats %>%
      mutate(type = network_type, .before = 1)
    
    cat("=" , rep("=", 70), "=\n", sep = "")
    cat("RESULTS -", toupper(network_type), "\n")
    cat("=" , rep("=", 70), "=\n", sep = "")
    cat("  Download:", stats$download_mbps, "Mbps\n")
    cat("  Upload:", stats$upload_mbps, "Mbps\n")
    cat("  Latency:", stats$latency_ms, "ms\n")
    cat("  Tests:", format(stats$total_tests, big.mark = ","), "\n")
    cat("  Tiles:", format(stats$tile_count, big.mark = ","), "\n")
    cat("=" , rep("=", 70), "=\n\n", sep = "")
    
  }, error = function(e) {
    cat("✗ Error:", e$message, "\n\n")
  })
  
  # Clean up
  cat("7. Cleaning up raw file...\n")
  if (file.exists(parquet_file)) {
    file.remove(parquet_file)
    cat("✓ Removed\n\n")
  }
}

# Final summary
TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                     Azerbaijan Test Complete!                        ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("Processing time:", round(TOTAL_TIME, 1), "minutes\n\n")

if (length(all_results) > 0) {
  results_df <- bind_rows(all_results)
  write_csv(results_df, here("data", "aggregated", "Azerbaijan_2024Q3_Test.csv"))
  
  cat("Results Summary:\n")
  print(results_df)
  
  cat("\n\n✓ Test successful!\n")
  cat("✓ Saved to: data/aggregated/Azerbaijan_2024Q3_Test.csv\n\n")
  
  cat("This confirms the pipeline works!\n")
  cat("Ready to process all 26 quarters for all CIS countries.\n\n")
} else {
  cat("✗ No results generated\n")
}
