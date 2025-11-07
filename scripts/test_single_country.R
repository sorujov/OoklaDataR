#!/share/home/orujov/.conda/envs/.ookladatar/bin/Rscript
# =============================================================================
# Test Script: Single Country Download with Parallel Quarters
# =============================================================================
# Purpose: Download and aggregate Ookla data for ONE country across all quarters
#          Test proper median and mean calculation before scaling to 11 countries
# Author: OoklaDataR Project
# Date: 2025-11-06
# =============================================================================

suppressPackageStartupMessages({
  library(tidyverse)
  library(arrow)
  library(sf)
  library(httr)
  library(parallel)
  library(here)
})

cat("\n")
cat(strrep("=", 80), "\n")
cat("OOKLA DATA DOWNLOAD - SINGLE COUNTRY TEST\n")
cat(strrep("=", 80), "\n\n")

# =============================================================================
# CONFIGURATION
# =============================================================================

# S3 bucket configuration
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

# Get country from environment variable, command line, or use default
if (Sys.getenv("TEST_COUNTRY") != "") {
  TEST_COUNTRY <- Sys.getenv("TEST_COUNTRY")
  cat("Processing country from environment:", TEST_COUNTRY, "\n")
} else {
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) >= 1) {
    TEST_COUNTRY <- args[1]
    cat("Processing country from command line:", TEST_COUNTRY, "\n")
  } else {
    TEST_COUNTRY <- "AM"  # Default: Armenia (changed from Azerbaijan)
  }
}

# Test parameters
START_YEAR <- 2019
END_YEAR <- 2025
QUARTERS <- c(1, 2, 3, 4)
NETWORK_TYPES <- c("mobile", "fixed")

# Parallel processing for quarters - Can be enabled if needed
USE_PARALLEL <- TRUE  # Changed to TRUE to enable parallel

# Detect available cores (from SLURM or system)
if (Sys.getenv("SLURM_CPUS_PER_TASK") != "") {
  # On SLURM, use all available cores
  available_cores <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK"))
  N_CORES <- available_cores  # Use all available cores
  cat("Running on SLURM with", N_CORES, "cores\n")
} else {
  N_CORES <- min(4, detectCores() - 1)
  cat("Running locally with", N_CORES, "cores\n")
}

cat("\nConfiguration:\n")
cat("  Country:", TEST_COUNTRY, "\n")
cat("  Period:", START_YEAR, "-", END_YEAR, "\n")
cat("  Network types:", paste(NETWORK_TYPES, collapse = ", "), "\n")
cat("  Parallel processing:", USE_PARALLEL, "(", N_CORES, "cores )\n\n")

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================

#' Calculate weighted median correctly
weighted_median <- function(values, weights) {
  if (length(values) == 0 || all(is.na(values))) return(NA_real_)
  
  # Remove NA and invalid weights
  valid_idx <- !is.na(values) & !is.na(weights) & weights > 0
  if (sum(valid_idx) == 0) return(NA_real_)
  
  values <- values[valid_idx]
  weights <- weights[valid_idx]
  
  # Sort by values
  ord <- order(values)
  values <- values[ord]
  weights <- weights[ord]
  
  # Find weighted median position
  cum_weights <- cumsum(weights)
  total_weight <- sum(weights)
  median_pos <- total_weight / 2
  
  idx <- which(cum_weights >= median_pos)[1]
  return(values[idx])
}

#' List S3 files for a quarter
list_s3_files <- function(data_type, year, quarter) {
  prefix <- paste0("parquet/performance/type=", data_type,
                   "/year=", year, "/quarter=", quarter, "/")
  list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", 
                    URLencode(prefix, reserved = TRUE))
  
  tryCatch({
    response <- GET(list_url, timeout(30))
    if (status_code(response) != 200) return(NULL)
    
    content_text <- content(response, "text", encoding = "UTF-8")
    # Extract all Key tags
    all_keys <- str_match_all(content_text, "<Key>([^<]+)</Key>")[[1]]
    if (nrow(all_keys) > 0) {
      keys <- all_keys[, 2]  # Get captured group
      # Filter for parquet files only
      keys <- keys[str_detect(keys, "\\.parquet$")]
    } else {
      keys <- character(0)
    }
    
    return(if(length(keys) > 0) keys else NULL)
  }, error = function(e) {
    message("Error listing S3: ", e$message)
    return(NULL)
  })
}

#' Generate quadkey prefixes for a bounding box
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
  lat_seq <- seq(bbox$ymin, bbox$ymax, by = 0.5)  # Every 0.5 degrees for better coverage
  lon_seq <- seq(bbox$xmin, bbox$xmax, by = 0.5)
  
  grid <- expand.grid(lat = lat_seq, lon = lon_seq)
  quadkeys <- mapply(latlon_to_quadkey, grid$lat, grid$lon, 
                     MoreArgs = list(zoom = zoom))
  
  # Return unique prefixes
  unique(quadkeys)
}

#' Download and filter one parquet file (in-memory)
process_parquet_file <- function(file_key, country_boundary, bbox, quadkeys) {
  
  tryCatch({
    # Build S3 URI for direct access (faster than HTTP download)
    s3_uri <- paste0("s3://ookla-open-data/", file_key)
    
    # Open dataset directly from S3
    ds <- open_dataset(s3_uri, format = "parquet")
    
    # Check schema
    schema_cols <- names(ds$schema)
    has_tile_coords <- all(c("tile_x", "tile_y") %in% schema_cols)
    
    # Apply appropriate filter based on schema
    if (has_tile_coords) {
      # NEW SCHEMA (2023+): Use tile_x/tile_y bbox filter
      df <- ds %>%
        filter(
          tile_x >= bbox$xmin & tile_x <= bbox$xmax,
          tile_y >= bbox$ymin & tile_y <= bbox$ymax
        ) %>%
        collect()
    } else {
      # OLD SCHEMA (pre-2023): Use quadkey prefix filter
      prefix_length <- nchar(quadkeys[1])
      
      df <- ds %>%
        mutate(qk_prefix = substr(quadkey, 1, prefix_length)) %>%
        filter(qk_prefix %in% quadkeys) %>%
        select(-qk_prefix) %>%
        collect()
    }
    
    if (nrow(df) == 0) return(NULL)
    
    # Convert to spatial and filter by country boundary
    tiles_sf <- st_as_sf(df, wkt = "tile", crs = 4326)
    tiles_sf <- st_make_valid(tiles_sf)
    
    # Precise intersection with country boundary
    tiles_filtered <- st_intersection(tiles_sf, country_boundary)
    
    if (nrow(tiles_filtered) > 0) {
      return(st_drop_geometry(tiles_filtered))
    }
    return(NULL)
    
  }, error = function(e) {
    message("Error processing file: ", e$message)
    return(NULL)
  })
}

#' Calculate statistics from tiles
calculate_statistics <- function(tiles_df) {
  if (is.null(tiles_df) || nrow(tiles_df) == 0) return(NULL)
  
  # Convert to Mbps
  tiles_df <- tiles_df %>%
    mutate(
      download_mbps = avg_d_kbps / 1000,
      upload_mbps = avg_u_kbps / 1000,
      latency_ms = avg_lat_ms
    )
  
  # Counts
  total_tests <- sum(tiles_df$tests, na.rm = TRUE)
  total_devices <- sum(tiles_df$devices, na.rm = TRUE)
  
  # WEIGHTED statistics - Using DEVICES (user-centric approach, matches Ookla methodology)
  download_weighted_mean <- weighted.mean(tiles_df$download_mbps, tiles_df$devices, na.rm = TRUE)
  upload_weighted_mean <- weighted.mean(tiles_df$upload_mbps, tiles_df$devices, na.rm = TRUE)
  latency_weighted_mean <- weighted.mean(tiles_df$latency_ms, tiles_df$devices, na.rm = TRUE)
  
  download_weighted_median <- weighted_median(tiles_df$download_mbps, tiles_df$devices)
  upload_weighted_median <- weighted_median(tiles_df$upload_mbps, tiles_df$devices)
  latency_weighted_median <- weighted_median(tiles_df$latency_ms, tiles_df$devices)
  
  # UNWEIGHTED statistics (simple average/median of tiles)
  download_mean <- mean(tiles_df$download_mbps, na.rm = TRUE)
  upload_mean <- mean(tiles_df$upload_mbps, na.rm = TRUE)
  latency_mean <- mean(tiles_df$latency_ms, na.rm = TRUE)
  
  download_median <- median(tiles_df$download_mbps, na.rm = TRUE)
  upload_median <- median(tiles_df$upload_mbps, na.rm = TRUE)
  latency_median <- median(tiles_df$latency_ms, na.rm = TRUE)
  
  list(
    # Weighted stats (by devices - user-centric)
    download_weighted_mean = download_weighted_mean,
    upload_weighted_mean = upload_weighted_mean,
    latency_weighted_mean = latency_weighted_mean,
    download_weighted_median = download_weighted_median,
    upload_weighted_median = upload_weighted_median,
    latency_weighted_median = latency_weighted_median,
    # Unweighted stats
    download_mean = download_mean,
    upload_mean = upload_mean,
    latency_mean = latency_mean,
    download_median = download_median,
    upload_median = upload_median,
    latency_median = latency_median,
    # Counts
    tile_count = nrow(tiles_df),
    total_tests = total_tests,
    total_devices = total_devices
  )
}

# =============================================================================
# MAIN PROCESSING FUNCTION
# =============================================================================

#' Process one quarter for the country
process_quarter <- function(year, quarter, network_type, 
                           country_code, country_name, country_boundary,
                           bbox, quadkeys, log_file = NULL) {
  
  id <- paste0(country_code, "_", year, "Q", quarter, "_", network_type)
  
  # Logging function
  log_msg <- function(msg) {
    timestamp <- format(Sys.time(), "%H:%M:%S")
    full_msg <- paste(timestamp, "-", id, msg)
    cat(full_msg, "\n")
    if (!is.null(log_file)) {
      cat(full_msg, "\n", file = log_file, append = TRUE)
    }
  }
  
  log_msg("")
  
  tryCatch({
    # List S3 files
    file_keys <- list_s3_files(network_type, year, quarter)
    if (is.null(file_keys) || length(file_keys) == 0) {
      log_msg("  ⚠ No files found in S3")
      return(NULL)
    }
    
    log_msg(paste("  Found", length(file_keys), "files, filtering..."))
    
    # Process all files and collect tiles
    all_tiles <- list()
    for (i in seq_along(file_keys)) {
      tiles <- process_parquet_file(file_keys[i], country_boundary, bbox, quadkeys)
      if (!is.null(tiles)) {
        all_tiles[[length(all_tiles) + 1]] <- tiles
      }
    }
    
    if (length(all_tiles) == 0) {
      log_msg("  ⚠ No tiles in country boundary")
      return(NULL)
    }
    
    # Combine and calculate
    combined <- bind_rows(all_tiles)
    stats <- calculate_statistics(combined)
    
    if (is.null(stats)) return(NULL)
    
    # Create result
    result <- tibble(
      country_code = country_code,
      country_name = country_name,
      year = year,
      quarter = quarter,
      network_type = network_type,
      date = as.Date(paste0(year, "-", quarter * 3 - 2, "-01")),
      # Weighted statistics
      download_mbps_weighted_mean = stats$download_weighted_mean,
      upload_mbps_weighted_mean = stats$upload_weighted_mean,
      latency_ms_weighted_mean = stats$latency_weighted_mean,
      download_mbps_weighted_median = stats$download_weighted_median,
      upload_mbps_weighted_median = stats$upload_weighted_median,
      latency_ms_weighted_median = stats$latency_weighted_median,
      # Unweighted statistics  
      download_mbps_mean = stats$download_mean,
      upload_mbps_mean = stats$upload_mean,
      latency_ms_mean = stats$latency_mean,
      download_mbps_median = stats$download_median,
      upload_mbps_median = stats$upload_median,
      latency_ms_median = stats$latency_median,
      # Counts
      tile_count = stats$tile_count,
      total_tests = stats$total_tests,
      total_devices = stats$total_devices
    )
    
    log_msg(paste("  ✓", format(stats$tile_count, big.mark = ","), "tiles,",
                  format(stats$total_tests, big.mark = ","), "tests"))
    
    return(result)
    
  }, error = function(e) {
    log_msg(paste("  ✗ ERROR:", e$message))
    return(NULL)
  })
}

# =============================================================================
# LOAD COUNTRY BOUNDARY
# =============================================================================

cat("Loading country boundary...\n")

boundaries_file <- here("data", "boundaries", "cis_countries.rds")

if (!file.exists(boundaries_file)) {
  cat("\n⚠ Boundaries file not found. Creating it...\n\n")
  
  # Create boundaries
  library(rnaturalearth)
  
  CIS_COUNTRIES <- c("AM", "AZ", "BY", "GE", "KZ", "KG", "MD", "RU", "TJ", "TM", "UZ")
  
  world <- ne_countries(scale = "medium", returnclass = "sf")
  cis_boundaries <- world %>%
    filter(iso_a2 %in% CIS_COUNTRIES) %>%
    select(country = name, iso_a2, iso_a3, geometry) %>%
    st_make_valid()
  
  dir.create(dirname(boundaries_file), recursive = TRUE, showWarnings = FALSE)
  saveRDS(cis_boundaries, boundaries_file)
  
  cat("✓ Boundaries created\n\n")
} else {
  cis_boundaries <- readRDS(boundaries_file)
}

# Get test country
country_info <- cis_boundaries %>% filter(iso_a2 == TEST_COUNTRY)
if (nrow(country_info) == 0) {
  stop("Country not found: ", TEST_COUNTRY)
}

country_name <- country_info$country[1]
country_boundary <- country_info

# Calculate country-specific bounding box (with buffer for edge tiles)
country_bbox_raw <- st_bbox(country_boundary)
COUNTRY_BBOX <- list(
  xmin = country_bbox_raw["xmin"] - 0.5,  # Add 0.5 degree buffer
  ymin = country_bbox_raw["ymin"] - 0.5,
  xmax = country_bbox_raw["xmax"] + 0.5,
  ymax = country_bbox_raw["ymax"] + 0.5
)

# Generate country-specific quadkey prefixes for old schema
COUNTRY_QUADKEYS <- generate_quadkey_prefixes(COUNTRY_BBOX, zoom = 4)

cat("✓ Country:", country_name, "(", TEST_COUNTRY, ")\n")
cat("  Bounding box: [", sprintf("%.2f", COUNTRY_BBOX$xmin), ",", 
    sprintf("%.2f", COUNTRY_BBOX$ymin), "] to [",
    sprintf("%.2f", COUNTRY_BBOX$xmax), ",", 
    sprintf("%.2f", COUNTRY_BBOX$ymax), "]\n\n")

# =============================================================================
# CREATE TASK LIST
# =============================================================================

tasks <- expand.grid(
  year = START_YEAR:END_YEAR,
  quarter = QUARTERS,
  network_type = NETWORK_TYPES,
  stringsAsFactors = FALSE
) %>%
  arrange(year, quarter, network_type)

# Filter out 2025 Q4 (only include Q1-Q3 for 2025)
tasks <- tasks %>%
  filter(!(year == 2025 & quarter == 4))

cat("Total tasks to process:", nrow(tasks), "\n\n")

# =============================================================================
# PROCESS TASKS
# =============================================================================

cat(strrep("-", 80), "\n")
cat("STARTING DOWNLOAD...\n")
cat(strrep("-", 80), "\n\n")

start_time <- Sys.time()

if (USE_PARALLEL && N_CORES > 1) {
  cat("Using parallel processing with", N_CORES, "cores\n\n")
  
  # Create log file for parallel processing
  log_dir <- here("logs")
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  log_file <- file.path(log_dir, paste0("parallel_", TEST_COUNTRY, "_", 
                                        format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))
  
  # CRITICAL FIX: Convert SF boundary to WKT string for serialization
  # SF objects with geometries don't serialize well to parallel workers
  country_boundary_wkt <- st_as_text(st_geometry(country_boundary)[[1]])
  
  cl <- makeCluster(N_CORES, type = "PSOCK")  # PSOCK is more reliable than FORK on clusters
  on.exit(stopCluster(cl), add = TRUE)
  
  # Export to workers
  clusterExport(cl, c(
    "process_quarter", "process_parquet_file", "list_s3_files",
    "calculate_statistics", "weighted_median", "generate_quadkey_prefixes",
    "TEST_COUNTRY", "country_name", "country_boundary_wkt", "S3_BASE",
    "tasks", "COUNTRY_BBOX", "COUNTRY_QUADKEYS", "log_file"
  ), envir = environment())
  
  # Load packages on workers
  clusterEvalQ(cl, {
    library(tidyverse)
    library(arrow)
    library(sf)
    library(httr)
    library(here)
  })
  
  # Process in parallel
  results <- parLapply(cl, 1:nrow(tasks), function(i) {
    tryCatch({
      task <- tasks[i, ]
      
      # Reconstruct SF boundary from WKT on worker
      country_boundary_worker <- st_sf(
        geometry = st_as_sfc(country_boundary_wkt, crs = 4326)
      )
      
      process_quarter(
        year = task$year,
        quarter = task$quarter,
        network_type = task$network_type,
        country_code = TEST_COUNTRY,
        country_name = country_name,
        country_boundary = country_boundary_worker,
        bbox = COUNTRY_BBOX,
        quadkeys = COUNTRY_QUADKEYS,
        log_file = log_file
      )
    }, error = function(e) {
      msg <- paste("ERROR in task", i, "(", task$year, "Q", task$quarter, 
                   task$network_type, "):", conditionMessage(e))
      cat(msg, "\n", file = log_file, append = TRUE)
      return(NULL)
    })
  })
  
} else {
  cat("Processing sequentially\n\n")
  
  results <- lapply(1:nrow(tasks), function(i) {
    task <- tasks[i, ]
    process_quarter(
      year = task$year,
      quarter = task$quarter,
      network_type = task$network_type,
      country_code = TEST_COUNTRY,
      country_name = country_name,
      country_boundary = country_boundary,
      bbox = COUNTRY_BBOX,
      quadkeys = COUNTRY_QUADKEYS
    )
  })
}

end_time <- Sys.time()

# =============================================================================
# COMBINE AND SAVE RESULTS
# =============================================================================

cat("\n", strrep("-", 80), "\n")
cat("PROCESSING RESULTS...\n")
cat(strrep("-", 80), "\n\n")

# Remove NULL results
results_df <- bind_rows(results[!sapply(results, is.null)])

if (nrow(results_df) == 0) {
  cat("⚠ No data collected!\n\n")
  quit(status = 1)
}

# Create output directory
output_dir <- here("data", "processed", TEST_COUNTRY)
dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)

# Save CSV
output_file <- file.path(output_dir, paste0(TEST_COUNTRY, "_all_quarters.csv"))
write_csv(results_df, output_file)

# Create summary by network type
summary_stats <- results_df %>%
  group_by(network_type) %>%
  summarise(
    quarters = n(),
    avg_download_mean = mean(download_mbps_mean, na.rm = TRUE),
    avg_download_median = mean(download_mbps_median, na.rm = TRUE),
    avg_upload_mean = mean(upload_mbps_mean, na.rm = TRUE),
    avg_upload_median = mean(upload_mbps_median, na.rm = TRUE),
    total_tiles = sum(tile_count, na.rm = TRUE),
    total_tests = sum(total_tests, na.rm = TRUE),
    .groups = "drop"
  )

# =============================================================================
# DISPLAY RESULTS
# =============================================================================

cat("\n", strrep("=", 80), "\n")
cat("✓ DOWNLOAD COMPLETE!\n")
cat(strrep("=", 80), "\n\n")

cat("Country:", country_name, "(", TEST_COUNTRY, ")\n")
cat("Processing time:", round(difftime(end_time, start_time, units = "mins"), 2), "minutes\n")
cat("Records collected:", nrow(results_df), "out of", nrow(tasks), "tasks\n\n")

cat("Summary by Network Type:\n")
print(summary_stats, n = Inf)

cat("\n\nOutput saved to:", output_file, "\n")

# Display sample of results
cat("\nSample of results (first 10 rows):\n")
print(results_df %>% 
        select(year, quarter, network_type, download_mbps_mean, 
               download_mbps_median, tile_count, total_tests) %>%
        head(10), 
      n = 10)

cat("\n", strrep("=", 80), "\n")
cat("✓ SUCCESS! Review results before processing all 11 countries.\n")
cat(strrep("=", 80), "\n\n")

cat("Next steps:\n")
cat("  1. Review the CSV file:", output_file, "\n")
cat("  2. Check that median and mean values look reasonable\n")
cat("  3. If satisfied, run the multi-country script\n\n")
