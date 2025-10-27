# =============================================================================
# Quick Test: One Quarter (2024 Q3) - Complete Pipeline
# =============================================================================
# Download, filter, aggregate, and see results in minutes
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)
library(rnaturalearth)
library(lubridate)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║           Quick Test: 2024 Q3 (Fixed + Mobile)                       ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

# Configuration
config <- readRDS(here("config.rds"))
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"
CIS_BBOX <- list(xmin = 19.6, ymin = 36.7, xmax = 180, ymax = 81.9)

# Test quarter
TEST_YEAR <- 2024
TEST_QUARTER <- 3

cat("Testing with:", TEST_YEAR, "Q", TEST_QUARTER, "\n")
cat("This will process both Fixed and Mobile networks\n\n")

# Create directories
dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "processed"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "boundaries"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "aggregated"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "temp_test"), recursive = TRUE, showWarnings = FALSE)

# =============================================================================
# STEP 1: Get Country Boundaries
# =============================================================================
cat("\n┌─────────────────────────────────────────────────────────────────────┐\n")
cat("│ STEP 1: Loading Country Boundaries                                 │\n")
cat("└─────────────────────────────────────────────────────────────────────┘\n\n")

boundaries_file <- here("data", "boundaries", "cis_countries.rds")

if (!file.exists(boundaries_file)) {
  cat("Downloading CIS country boundaries...\n")
  world <- ne_countries(scale = "medium", returnclass = "sf")
  cis_countries <- world %>%
    filter(iso_a2 %in% config$countries) %>%
    select(country = name, iso_a2, iso_a3, geometry) %>%
    st_make_valid()
  saveRDS(cis_countries, boundaries_file)
  cat("✓ Saved", nrow(cis_countries), "country boundaries\n")
} else {
  cis_countries <- readRDS(boundaries_file)
  cat("✓ Loaded", nrow(cis_countries), "country boundaries\n")
}

# =============================================================================
# STEP 2: Download and Process Data
# =============================================================================

for (data_type in c("mobile", "fixed")) {
  
  cat("\n┌─────────────────────────────────────────────────────────────────────┐\n")
  cat("│ STEP 2:", toupper(data_type), "Network                                   │\n")
  cat("└─────────────────────────────────────────────────────────────────────┘\n\n")
  
  # Construct S3 path
  prefix <- paste0(
    config$s3_prefix,
    "type=", data_type,
    "/year=", TEST_YEAR,
    "/quarter=", TEST_QUARTER,
    "/"
  )
  
  # List files
  cat("Listing files in S3...\n")
  list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", URLencode(prefix, reserved = TRUE))
  response <- GET(list_url, timeout(60))
  
  if (status_code(response) != 200) {
    cat("✗ Failed to list files\n")
    next
  }
  
  content_text <- content(response, "text", encoding = "UTF-8")
  keys <- str_extract_all(content_text, "<Key>([^<]+\\.parquet)</Key>")[[1]]
  keys <- str_replace_all(keys, "</?Key>", "")
  
  if (length(keys) == 0) {
    cat("✗ No files found\n")
    next
  }
  
  cat("✓ Found", length(keys), "file(s)\n\n")
  
  # Download
  for (key in keys) {
    parquet_file <- here("data", "raw", basename(key))
    
    if (file.exists(parquet_file)) {
      cat("File already downloaded, skipping download\n")
    } else {
      cat("Downloading:", basename(key), "\n")
      url <- paste0(S3_BASE, "/", key)
      dl_response <- GET(url, write_disk(parquet_file, overwrite = TRUE), progress(), timeout(300))
      
      if (status_code(dl_response) == 200) {
        file_size <- file.info(parquet_file)$size / (1024^2)
        cat("✓ Downloaded:", round(file_size, 1), "MB\n\n")
      }
    }
    
    # Process immediately
    cat("Processing with CIS bbox filter...\n")
    
    tryCatch({
      # Read with bbox filter
      ds <- open_dataset(parquet_file)
      
      cis_tiles <- ds %>%
        filter(
          tile_x >= CIS_BBOX$xmin & tile_x <= CIS_BBOX$xmax,
          tile_y >= CIS_BBOX$ymin & tile_y <= CIS_BBOX$ymax
        ) %>%
        collect()
      
      cat("✓ Filtered to", nrow(cis_tiles), "CIS tiles\n")
      
      if (nrow(cis_tiles) == 0) {
        cat("✗ No CIS tiles found\n")
        next
      }
      
      # Convert to spatial
      cat("Converting to spatial features...\n")
      tiles_sf <- st_as_sf(cis_tiles, wkt = "tile", crs = 4326)
      tiles_sf <- st_make_valid(tiles_sf)
      
      # Spatial intersection with countries
      cat("Matching tiles to countries...\n")
      tiles_with_country <- st_intersection(tiles_sf, cis_countries)
      
      cat("✓ Matched", nrow(tiles_with_country), "tiles to CIS countries\n")
      
      # Save by country
      cat("Saving by country:\n")
      for (country_code in unique(tiles_with_country$iso_a2)) {
        country_tiles <- tiles_with_country %>% filter(iso_a2 == country_code)
        
        output_file <- here(
          "data", "temp_test",
          paste0(country_code, "_", TEST_YEAR, TEST_QUARTER, "_", data_type, ".rds")
        )
        
        saveRDS(country_tiles, output_file)
        cat("  ", country_code, ":", nrow(country_tiles), "tiles\n")
      }
      
    }, error = function(e) {
      cat("✗ Error:", e$message, "\n")
    })
    
    # Clean up raw file
    cat("\nCleaning up raw file...\n")
    if (file.exists(parquet_file)) {
      file.remove(parquet_file)
      cat("✓ Removed raw Parquet\n")
    }
  }
}

# =============================================================================
# STEP 3: Aggregate Results
# =============================================================================
cat("\n┌─────────────────────────────────────────────────────────────────────┐\n")
cat("│ STEP 3: Aggregating Results                                        │\n")
cat("└─────────────────────────────────────────────────────────────────────┘\n\n")

test_files <- list.files(here("data", "temp_test"), pattern = "\\.rds$", full.names = TRUE)

if (length(test_files) == 0) {
  cat("✗ No processed files found\n")
} else {
  
  results <- test_files %>%
    map_dfr(function(file) {
      parts <- str_split(basename(file), "_")[[1]]
      country <- parts[1]
      year_quarter <- paste0(parts[2], parts[3])
      type <- str_remove(parts[4], "\\.rds$")
      
      tiles <- readRDS(file)
      tiles_df <- st_drop_geometry(tiles)
      
      tibble(
        Country = country,
        Country_Name = config$country_names[country],
        Year = TEST_YEAR,
        Quarter = TEST_QUARTER,
        Type = type,
        Download_Mbps = round(median(tiles_df$avg_d_kbps / 1000, na.rm = TRUE), 1),
        Upload_Mbps = round(median(tiles_df$avg_u_kbps / 1000, na.rm = TRUE), 1),
        Latency_ms = round(median(tiles_df$avg_lat_ms, na.rm = TRUE), 0),
        Tile_Count = nrow(tiles_df),
        Total_Tests = sum(tiles_df$tests, na.rm = TRUE)
      )
    })
  
  cat("✓ Aggregated", nrow(results), "records\n\n")
  
  # Display results
  cat("=" , rep("=", 70), "=\n", sep = "")
  cat("RESULTS: 2024 Q3 - CIS Internet Speeds\n")
  cat("=" , rep("=", 70), "=\n\n", sep = "")
  
  results_display <- results %>%
    arrange(Type, desc(Download_Mbps)) %>%
    select(Country_Name, Type, Download_Mbps, Upload_Mbps, Latency_ms, Total_Tests)
  
  print(results_display, n = 30)
  
  # Save to CSV
  cat("\n\nSaving test results...\n")
  write_csv(results, here("data", "aggregated", "Test_2024Q3_Results.csv"))
  cat("✓ Saved to: data/aggregated/Test_2024Q3_Results.csv\n")
  
  # Quick comparison
  cat("\n")
  cat("=" , rep("=", 70), "=\n", sep = "")
  cat("Top 5 Fixed Broadband Speeds (2024 Q3)\n")
  cat("=" , rep("=", 70), "=\n", sep = "")
  
  top_fixed <- results %>%
    filter(Type == "fixed") %>%
    arrange(desc(Download_Mbps)) %>%
    head(5) %>%
    select(Rank = Country_Name, Download = Download_Mbps, Upload = Upload_Mbps)
  
  top_fixed$Rank <- paste0(1:nrow(top_fixed), ". ", top_fixed$Rank)
  print(top_fixed, row.names = FALSE)
  
  cat("\n")
  cat("=" , rep("=", 70), "=\n", sep = "")
  cat("Top 5 Mobile Speeds (2024 Q3)\n")
  cat("=" , rep("=", 70), "=\n", sep = "")
  
  top_mobile <- results %>%
    filter(Type == "mobile") %>%
    arrange(desc(Download_Mbps)) %>%
    head(5) %>%
    select(Rank = Country_Name, Download = Download_Mbps, Upload = Upload_Mbps)
  
  top_mobile$Rank <- paste0(1:nrow(top_mobile), ". ", top_mobile$Rank)
  print(top_mobile, row.names = FALSE)
}

# =============================================================================
# Summary
# =============================================================================
TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")

cat("\n\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                        Test Complete!                                ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("Processing time:", round(TOTAL_TIME, 1), "minutes\n")
cat("Output: data/aggregated/Test_2024Q3_Results.csv\n\n")

cat("✓ Test successful! Ready to process all 26 quarters (2019-2025)?\n\n")
