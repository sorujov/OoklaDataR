# =============================================================================
# Universal Azerbaijan Test - Works for 2019-2025
# =============================================================================
# Purpose: Efficient processing for both old and new schemas
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)
library(rnaturalearth)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║      Universal Azerbaijan Test: Any Quarter (2019-2025)             ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

# Configuration
config <- readRDS(here("config.rds"))
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

# Azerbaijan bounding box
AZ_BBOX <- list(xmin = 44.8, ymin = 38.4, xmax = 50.4, ymax = 41.9)

# TEST CONFIGURATION - Change these!
TEST_YEAR <- 2021  # Try 2019, 2020, 2024, etc.
TEST_QUARTER <- 2

cat("Testing:", TEST_YEAR, "Q", TEST_QUARTER, "\n")
cat("Azerbaijan bbox: Lon [", AZ_BBOX$xmin, ",", AZ_BBOX$xmax, 
    "] Lat [", AZ_BBOX$ymin, ",", AZ_BBOX$ymax, "]\n\n")

# Create directories
dir.create(here("data", "temp_test"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "boundaries"), recursive = TRUE, showWarnings = FALSE)

# Load Azerbaijan boundary
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

# =============================================================================
# Helper Function: Generate Quadkeys for Azerbaijan
# =============================================================================
generate_az_quadkeys <- function(bbox, zoom = 5) {
  latlon_to_quadkey <- function(lat, lon, zoom) {
    lat_rad <- lat * pi / 180
    n <- 2^zoom
    x <- floor((lon + 180) / 360 * n)
    y <- floor((1 - log(tan(lat_rad) + 1/cos(lat_rad)) / pi) / 2 * n)
    
    quadkey <- ""
    for (i in zoom:1) {
      digit <- 0
      mask <- bitwShiftL(1, i - 1)
      if (bitwAnd(as.integer(x), as.integer(mask)) != 0) digit <- digit + 1
      if (bitwAnd(as.integer(y), as.integer(mask)) != 0) digit <- digit + 2
      quadkey <- paste0(quadkey, digit)
    }
    return(quadkey)
  }
  
  # Generate grid points
  lat_seq <- seq(bbox$ymin, bbox$ymax, by = 0.5)
  lon_seq <- seq(bbox$xmin, bbox$xmax, by = 0.5)
  
  grid <- expand.grid(lat = lat_seq, lon = lon_seq)
  quadkeys <- mapply(latlon_to_quadkey, grid$lat, grid$lon, 
                     MoreArgs = list(zoom = zoom))
  
  # Return unique prefixes at zoom 4
  unique(substr(quadkeys, 1, 4))
}

# Generate Azerbaijan quadkey prefixes once
AZ_QUADKEYS <- generate_az_quadkeys(AZ_BBOX)
cat("Generated", length(AZ_QUADKEYS), "quadkey prefixes for Azerbaijan:", 
    paste(AZ_QUADKEYS, collapse = ", "), "\n\n")

# =============================================================================
# Process both network types
# =============================================================================
all_results <- list()

for (network_type in c("fixed", "mobile")) {
  
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
  
  # List files
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
  
  # Build S3 URI for direct access
  quarter_start <- sprintf("%s-%02d-01", TEST_YEAR, ((TEST_QUARTER - 1) * 3) + 1)
  s3_uri <- paste0(
    "s3://ookla-open-data/parquet/performance/type=", network_type,
    "/year=", TEST_YEAR, "/quarter=", TEST_QUARTER, "/",
    quarter_start, "_performance_", network_type, "_tiles.parquet"
  )
  
  # Try filtering on S3
  cat("2. Attempting S3 filtering for Azerbaijan...\n")
  
  tryCatch({
    ds <- open_dataset(s3_uri, format = "parquet")
    
    # Check schema
    schema_cols <- names(ds$schema)
    has_tile_coords <- all(c("tile_x", "tile_y") %in% schema_cols)
    
    if (has_tile_coords) {
      # NEW SCHEMA: Use tile_x/tile_y bbox filter
      cat("   Using tile_x/tile_y bbox (new schema)...\n")
      az_tiles <- ds %>%
        filter(
          tile_x >= AZ_BBOX$xmin & tile_x <= AZ_BBOX$xmax,
          tile_y >= AZ_BBOX$ymin & tile_y <= AZ_BBOX$ymax
        ) %>%
        collect()
      
      cat("✓ S3 bbox filtering successful!\n")
      
    } else {
      # OLD SCHEMA: Use quadkey prefix filter
      cat("   Old schema detected. Using quadkey filtering...\n")
      
      prefix_length <- nchar(AZ_QUADKEYS[1])
      
      az_tiles <- ds %>%
        mutate(qk_prefix = substr(quadkey, 1, prefix_length)) %>%
        filter(qk_prefix %in% AZ_QUADKEYS) %>%
        select(-qk_prefix) %>%
        collect()
      
      cat("✓ S3 quadkey filtering successful!\n")
    }
    
    cat("   Downloaded", format(nrow(az_tiles), big.mark = ","), "tiles\n")
    cat("   Size:", round(object.size(az_tiles) / 1024^2, 1), "MB\n\n")
    
    if (nrow(az_tiles) == 0) {
      cat("✗ No Azerbaijan tiles found\n\n")
      next
    }
    
    # Convert to spatial
    cat("3. Converting to spatial features...\n")
    tiles_sf <- st_as_sf(az_tiles, wkt = "tile", crs = 4326)
    tiles_sf <- st_make_valid(tiles_sf)
    cat("✓ Created spatial features\n\n")
    
    # Precise intersection with Azerbaijan
    cat("4. Refining with precise Azerbaijan boundary...\n")
    az_final <- st_intersection(tiles_sf, azerbaijan)
    cat("✓ Final Azerbaijan tiles:", format(nrow(az_final), big.mark = ","), "\n\n")
    
    # Calculate statistics
    cat("5. Computing statistics...\n")
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
    
    cat("=", rep("=", 70), "=\n", sep = "")
    cat("RESULTS -", toupper(network_type), "\n")
    cat("=", rep("=", 70), "=\n", sep = "")
    cat("  Download:", stats$download_mbps, "Mbps\n")
    cat("  Upload:", stats$upload_mbps, "Mbps\n")
    cat("  Latency:", stats$latency_ms, "ms\n")
    cat("  Tests:", format(stats$total_tests, big.mark = ","), "\n")
    cat("  Tiles:", format(stats$tile_count, big.mark = ","), "\n")
    cat("=", rep("=", 70), "=\n\n", sep = "")
    
  }, error = function(e) {
    cat("✗ Error:", conditionMessage(e), "\n\n")
  })
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
  
  dir.create(here("data", "aggregated"), recursive = TRUE, showWarnings = FALSE)
  write_csv(results_df, here("data", "aggregated", 
                             paste0("Azerbaijan_", TEST_YEAR, "Q", TEST_QUARTER, "_Test.csv")))
  
  cat("Results Summary:\n")
  print(results_df)
  
  cat("\n\n✅ Test successful for", TEST_YEAR, "Q", TEST_QUARTER, "!\n")
  cat("✓ Saved to: data/aggregated/Azerbaijan_", TEST_YEAR, "Q", TEST_QUARTER, "_Test.csv\n\n")
  
  cat("✨ This works for ANY year (2019-2025)!\n")
  cat("   Old schema (2019-2022): Uses quadkey filtering\n")
  cat("   New schema (2023-2025): Uses tile_x/tile_y filtering\n\n")
} else {
  cat("✗ No results generated\n")
}
