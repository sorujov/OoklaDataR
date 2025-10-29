# =============================================================================
# Modular Function: Process Ookla Data for Any Country/Quarter
# =============================================================================
# Extracted from test_azerbaijan.R and made reusable
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)
library(rnaturalearth)
library(lwgeom)
if (!require("remotes")) install.packages("remotes")
remotes::install_github("teamookla/ooklaOpenDataR")
library(ooklaOpenDataR)
# =============================================================================
# HELPER: Generate Quadkey Prefixes
# =============================================================================
generate_quadkey_prefixes <- function(bbox, zoom = 5, prefix_length = 4) {
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
  
  lat_seq <- seq(bbox$ymin, bbox$ymax, by = 0.5)
  lon_seq <- seq(bbox$xmin, bbox$xmax, by = 0.5)
  
  grid <- expand.grid(lat = lat_seq, lon = lon_seq)
  quadkeys <- mapply(latlon_to_quadkey, grid$lat, grid$lon, 
                     MoreArgs = list(zoom = zoom))
  
  unique(substr(quadkeys, 1, prefix_length))
}

# =============================================================================
# MAIN FUNCTION
# =============================================================================
# =============================================================================
# process_ookla_data.R - Optimized with ooklaOpenDataR
# =============================================================================

process_ookla_data <- function(country_code, year, quarter, network_type,
                                cis_countries, config, 
                                save_output = TRUE, output_dir = NULL,
                                verbose = TRUE) {

  # Define output file path
  output_file <- NULL
  if (save_output && !is.null(output_dir)) {
    output_file <- file.path(
      output_dir,
      paste0(country_code, "_", year, "Q", quarter, "_", network_type, ".rds")
    )
  }
  
  # Create a template for empty or errored results
  empty_result <- data.frame(
      country_code = country_code,
      country_name = config$country_names[country_code],
      year = year,
      quarter = quarter,
      network_type = network_type,
      date = as.Date(paste0(year, "-", quarter * 3, "-01")),
      download_mbps = NA_real_,
      upload_mbps = NA_real_,
      latency_ms = NA_real_,
      download_mbps_median = NA_real_,
      upload_mbps_median = NA_real_,
      latency_ms_median = NA_real_,
      tile_count = 0L,
      total_tests = 0L,
      total_devices = 0L
    )

  tryCatch({
    
    # Download data for the specified country using the package
    data_sf <- get_performance_tiles(
      service = network_type,
      year = year,
      quarter = quarter,
      country_iso_a2 = country_code
    )
    
    # Check if data was returned
    if (is.null(data_sf) || nrow(data_sf) == 0) {
      if (verbose) cat("No data returned from ooklaOpenDataR\n")
      if (!is.null(output_file)) saveRDS(empty_result, output_file)
      return(empty_result)
    }

    # The package returns an sf object, so we can directly summarize
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

    # Save the aggregated result
    if (!is.null(output_file)) {
      saveRDS(result_df, output_file)
    }
    
    return(result_df)

  }, error = function(e) {
    if (verbose) cat("Error:", conditionMessage(e), "\n")
    if (!is.null(output_file)) {
      error_result <- empty_result
      error_result$tile_count <- -1 # Mark as an error
      saveRDS(error_result, output_file)
    }
    return(NULL)
  })
}
