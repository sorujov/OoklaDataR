# =============================================================================
# OoklaDataR - Country Filtering Script
# =============================================================================
# Purpose: Filter Ookla tiles by CIS country boundaries
# Author: OoklaDataR Project
# Date: 2025-10-27
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(rnaturalearth)
library(lwgeom)

cat("=== OoklaDataR - Country Filtering ===\n\n")

# Load configuration
config <- readRDS(here("config.rds"))

# Create output directories
dir.create(here("data", "processed"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "boundaries"), recursive = TRUE, showWarnings = FALSE)

# Download and prepare country boundaries (one-time)
boundaries_file <- here("data", "boundaries", "cis_countries.rds")

if (!file.exists(boundaries_file)) {
  cat("Downloading CIS country boundaries...\n")
  
  # Get world map
  world <- ne_countries(scale = "medium", returnclass = "sf")
  
  # Filter for CIS countries
  cis_countries <- world %>%
    filter(iso_a2 %in% config$countries) %>%
    select(
      country = name,
      iso_a2,
      iso_a3,
      geometry
    )
  
  # Ensure valid geometries
  cis_countries <- st_make_valid(cis_countries)
  
  cat("Downloaded boundaries for", nrow(cis_countries), "countries:\n")
  print(cis_countries %>% st_drop_geometry() %>% select(country, iso_a2))
  
  # Save boundaries
  saveRDS(cis_countries, boundaries_file)
  cat("\n✓ Boundaries saved to:", boundaries_file, "\n\n")
} else {
  cat("Loading existing country boundaries...\n")
  cis_countries <- readRDS(boundaries_file)
  cat("✓ Loaded boundaries for", nrow(cis_countries), "countries\n\n")
}

# CIS bounding box for pre-filtering
CIS_BBOX <- list(xmin = 19.6, ymin = 36.7, xmax = 180, ymax = 81.9)

# Function to filter Parquet file by country
filter_parquet_by_countries <- function(parquet_file, countries_sf, output_dir) {
  
  cat("Processing:", basename(parquet_file), "\n")
  
  tryCatch({
    # Read Parquet file with CIS bounding box filter (OPTIMIZED!)
    cat("  Reading Parquet file with CIS bbox filter...\n")
    ds <- open_dataset(parquet_file)
    
    # Pre-filter by CIS bounding box BEFORE loading into memory
    df <- ds %>%
      filter(
        tile_x >= CIS_BBOX$xmin & tile_x <= CIS_BBOX$xmax,
        tile_y >= CIS_BBOX$ymin & tile_y <= CIS_BBOX$ymax
      ) %>%
      collect()
    
    if (nrow(df) == 0) {
      cat("  ✗ No tiles in CIS region\n")
      return(NULL)
    }
    
    cat("  Loaded", nrow(df), "CIS tiles (bbox filtered)\n")
    
    # Check for geometry column (might be named 'tile' or 'geometry')
    geom_col <- NULL
    if ("tile" %in% names(df)) {
      geom_col <- "tile"
    } else if ("geometry" %in% names(df)) {
      geom_col <- "geometry"
    } else {
      cat("  ✗ No geometry column found\n")
      return(NULL)
    }
    
    # Convert to sf object
    cat("  Converting to spatial features...\n")
    
    # Handle WKT geometries
    if (is.character(df[[geom_col]])) {
      tiles_sf <- st_as_sf(df, wkt = geom_col, crs = 4326)
    } else {
      cat("  ✗ Unexpected geometry format\n")
      return(NULL)
    }
    
    # Ensure valid geometries
    tiles_sf <- st_make_valid(tiles_sf)
    
    # Spatial intersection with CIS countries
    cat("  Filtering by country boundaries...\n")
    
    tiles_filtered <- st_intersection(tiles_sf, countries_sf)
    
    if (nrow(tiles_filtered) == 0) {
      cat("  ✗ No tiles found in CIS countries\n")
      return(NULL)
    }
    
    cat("  ✓ Retained", nrow(tiles_filtered), "tiles in CIS region\n")
    
    # Save filtered data by country
    for (country_code in unique(tiles_filtered$iso_a2)) {
      country_tiles <- tiles_filtered %>%
        filter(iso_a2 == country_code)
      
      if (nrow(country_tiles) > 0) {
        # Create output filename
        base_name <- tools::file_path_sans_ext(basename(parquet_file))
        output_file <- file.path(
          output_dir,
          paste0(country_code, "_", base_name, ".rds")
        )
        
        # Save as RDS for efficient storage
        saveRDS(country_tiles, output_file)
        
        cat("    Saved", nrow(country_tiles), "tiles for", 
            country_code, "to", basename(output_file), "\n")
      }
    }
    
    return(tiles_filtered)
    
  }, error = function(e) {
    cat("  ✗ Error:", e$message, "\n")
    return(NULL)
  })
}

# Process all Parquet files in raw directory
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Processing Parquet files...\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

parquet_files <- list.files(
  here("data", "raw"),
  pattern = "\\.parquet$",
  full.names = TRUE
)

if (length(parquet_files) == 0) {
  cat("No Parquet files found in data/raw/\n")
  cat("Run scripts/01_download_data.R first\n")
} else {
  cat("Found", length(parquet_files), "Parquet files to process\n\n")
  
  for (file in parquet_files) {
    result <- filter_parquet_by_countries(
      file,
      cis_countries,
      here("data", "processed")
    )
    cat("\n")
  }
  
  cat("=" , rep("=", 60), "=\n", sep = "")
  cat("Filtering Complete\n")
  cat("=" , rep("=", 60), "=\n", sep = "")
  
  # Summary of processed files
  processed_files <- list.files(here("data", "processed"), pattern = "\\.rds$")
  cat("\nProcessed files saved:", length(processed_files), "\n")
  
  # Count by country
  countries_in_data <- str_extract(processed_files, "^[A-Z]{2}")
  country_counts <- table(countries_in_data)
  cat("\nFiles by country:\n")
  print(sort(country_counts, decreasing = TRUE))
}

cat("\n✓ Country filtering complete\n")
cat("\nNext step: Run scripts/03_aggregate_data.R to create time series\n")
