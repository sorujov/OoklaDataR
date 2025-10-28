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

# CIS bounding box for generating quadkey prefixes
CIS_BBOX <- list(xmin = 19.6, ymin = 36.7, xmax = 180, ymax = 81.9)

# Helper: Generate quadkey prefixes for a bounding box
generate_quadkey_prefixes <- function(bbox, zoom = 4) {
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
  
  # Generate grid points across the bbox
  lat_seq <- seq(bbox$ymin, bbox$ymax, by = 2)  # Every 2 degrees
  lon_seq <- seq(bbox$xmin, bbox$xmax, by = 2)
  
  grid <- expand.grid(lat = lat_seq, lon = lon_seq)
  quadkeys <- mapply(latlon_to_quadkey, grid$lat, grid$lon, 
                     MoreArgs = list(zoom = zoom))
  
  # Return unique prefixes
  unique(quadkeys)
}

# Generate CIS quadkey prefixes once at module load
CIS_QUADKEYS <- generate_quadkey_prefixes(CIS_BBOX, zoom = 4)

# Function to filter Parquet file by country
filter_parquet_by_countries <- function(parquet_file, countries_sf, output_dir) {
  
  cat("Processing:", basename(parquet_file), "\n")
  
  tryCatch({
    cat("  Opening dataset...\n")
    ds <- open_dataset(parquet_file)
    
    # Check schema to determine filtering strategy
    schema_cols <- names(ds$schema)
    has_tile_coords <- all(c("tile_x", "tile_y") %in% schema_cols)
    
    if (has_tile_coords) {
      # NEW SCHEMA (2023+): Use tile_x/tile_y bbox filter
      cat("  New schema detected. Using tile_x/tile_y bbox filter...\n")
      df <- ds %>%
        filter(
          tile_x >= CIS_BBOX$xmin & tile_x <= CIS_BBOX$xmax,
          tile_y >= CIS_BBOX$ymin & tile_y <= CIS_BBOX$ymax
        ) %>%
        collect()
      cat("  Loaded", format(nrow(df), big.mark = ","), "CIS tiles (bbox filtered)\n")
      
    } else {
      # OLD SCHEMA (pre-2023): Use quadkey prefix filter
      cat("  Old schema detected. Using quadkey prefix filter...\n")
      
      prefix_length <- nchar(CIS_QUADKEYS[1])
      
      df <- ds %>%
        mutate(qk_prefix = substr(quadkey, 1, prefix_length)) %>%
        filter(qk_prefix %in% CIS_QUADKEYS) %>%
        select(-qk_prefix) %>%
        collect()
      
      cat("  Loaded", format(nrow(df), big.mark = ","), "CIS tiles (quadkey filtered)\n")
    }
    
    if (nrow(df) == 0) {
      cat("  ✗ No tiles in CIS region\n")
      return(NULL)
    }
    
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
