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
process_ookla_data <- function(
    country_code,
    year,
    quarter,
    network_type,
    country_bbox = NULL,
    cis_countries = NULL,
    config = NULL,
    save_output = FALSE,
    output_dir = NULL,
    verbose = TRUE
) {
  
  start_time <- Sys.time()
  
  if (verbose) {
    cat("\n┌─────────────────────────────────────────────────────────────────────┐\n")
    cat("│", country_code, year, "Q", quarter, toupper(network_type), 
        "                                            │\n")
    cat("└─────────────────────────────────────────────────────────────────────┘\n\n")
  }
  
  # Load config if not provided
  if (is.null(config)) {
    config <- readRDS(here("config.rds"))
  }
  
  S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"
  
  # Load boundaries if not provided
  if (is.null(cis_countries)) {
    boundaries_file <- here("data", "boundaries", "cis_countries.rds")
    if (file.exists(boundaries_file)) {
      cis_countries <- readRDS(boundaries_file)
    } else {
      stop("Boundaries not found. Run setup first.")
    }
  }
  
  # Get country boundary
  country_boundary <- cis_countries %>% 
    filter(iso_a2 == country_code) %>%
    st_make_valid()  # Fix any geometry issues
  
  if (nrow(country_boundary) == 0) {
    stop("Country ", country_code, " not found")
  }
  
  # Get bbox if not provided
  if (is.null(country_bbox)) {
    bbox_sf <- st_bbox(country_boundary)
    country_bbox <- list(
      xmin = as.numeric(bbox_sf["xmin"]),
      ymin = as.numeric(bbox_sf["ymin"]),
      xmax = as.numeric(bbox_sf["xmax"]),
      ymax = as.numeric(bbox_sf["ymax"])
    )
  }
  
  # Generate quadkey prefixes
  quadkey_prefixes <- generate_quadkey_prefixes(country_bbox)
  
  # Build S3 URI
  quarter_start <- sprintf("%s-%02d-01", year, ((quarter - 1) * 3) + 1)
  s3_uri <- paste0(
    "s3://ookla-open-data/", config$s3_prefix,
    "type=", network_type,
    "/year=", year,
    "/quarter=", quarter, "/",
    quarter_start, "_performance_", network_type, "_tiles.parquet"
  )
  
  if (verbose) cat("1. Downloading from S3 (streaming mode)...\n")
  
  # Download and filter with CHUNKING to prevent OOM
  tryCatch({
    ds <- open_dataset(s3_uri, format = "parquet")
    
    # Check schema
    schema_cols <- names(ds$schema)
    has_tile_coords <- all(c("tile_x", "tile_y") %in% schema_cols)
    
    # STREAMING APPROACH: Process in chunks
    if (has_tile_coords) {
      # NEW SCHEMA
      if (verbose) cat("   New schema: tile_x/tile_y bbox filter (streaming)\n")
      
      # Apply filter but DON'T collect yet
      ds_filtered <- ds %>%
        filter(
          tile_x >= country_bbox$xmin & tile_x <= country_bbox$xmax,
          tile_y >= country_bbox$ymin & tile_y <= country_bbox$ymax
        )
      
      # Check row count BEFORE collecting
      row_count <- ds_filtered %>% count() %>% pull(n)
      
      if (row_count == 0) {
        warning("No tiles found for ", country_code, " ", year, " Q", quarter)
        return(NULL)
      }
      
      # If dataset is large, process in chunks
      if (row_count > 100000) {
        if (verbose) cat("   Large dataset (", format(row_count, big.mark = ","), 
                        " rows) - processing in chunks\n")
        
        chunk_size <- 50000
        tiles_list <- list()
        
        for (i in seq(0, row_count, by = chunk_size)) {
          chunk <- ds_filtered %>%
            slice(i:(i + chunk_size - 1)) %>%
            collect()
          
          tiles_list[[length(tiles_list) + 1]] <- chunk
          
          if (verbose && length(tiles_list) %% 5 == 0) {
            cat("   Processed", format(i + nrow(chunk), big.mark = ","), 
                "of", format(row_count, big.mark = ","), "rows\n")
          }
          
          # Force garbage collection after each chunk
          gc(verbose = FALSE)
        }
        
        tiles <- bind_rows(tiles_list)
        rm(tiles_list)
        gc(verbose = FALSE)
      } else {
        # Small dataset, collect all at once
        tiles <- ds_filtered %>% collect()
      }
      
    } else {
      # OLD SCHEMA
      if (verbose) cat("   Old schema: quadkey prefix filter (streaming)\n")
      prefix_length <- nchar(quadkey_prefixes[1])
      
      ds_filtered <- ds %>%
        mutate(qk_prefix = substr(quadkey, 1, prefix_length)) %>%
        filter(qk_prefix %in% quadkey_prefixes) %>%
        select(-qk_prefix)
      
      # Check row count BEFORE collecting
      row_count <- ds_filtered %>% count() %>% pull(n)
      
      if (row_count == 0) {
        warning("No tiles found for ", country_code, " ", year, " Q", quarter)
        return(NULL)
      }
      
      # If dataset is large, process in chunks
      if (row_count > 100000) {
        if (verbose) cat("   Large dataset (", format(row_count, big.mark = ","), 
                        " rows) - processing in chunks\n")
        
        chunk_size <- 50000
        tiles_list <- list()
        
        for (i in seq(0, row_count, by = chunk_size)) {
          chunk <- ds_filtered %>%
            slice(i:(i + chunk_size - 1)) %>%
            collect()
          
          tiles_list[[length(tiles_list) + 1]] <- chunk
          
          if (verbose && length(tiles_list) %% 5 == 0) {
            cat("   Processed", format(i + nrow(chunk), big.mark = ","), 
                "of", format(row_count, big.mark = ","), "rows\n")
          }
          
          # Force garbage collection after each chunk
          gc(verbose = FALSE)
        }
        
        tiles <- bind_rows(tiles_list)
        rm(tiles_list)
        gc(verbose = FALSE)
      } else {
        # Small dataset, collect all at once
        tiles <- ds_filtered %>% collect()
      }
    }
    
    if (verbose) {
      cat("✓ Downloaded", format(nrow(tiles), big.mark = ","), "tiles\n\n")
    }
    
    if (nrow(tiles) == 0) {
      warning("No tiles found for ", country_code, " ", year, " Q", quarter)
      return(NULL)
    }
    
    # Convert to spatial IN CHUNKS to prevent OOM
    if (verbose) cat("2. Converting to spatial features (chunked)...\n")
    
    # Process spatial conversion in chunks
    if (nrow(tiles) > 50000) {
      chunk_size <- 25000
      tiles_sf_list <- list()
      
      for (i in seq(1, nrow(tiles), by = chunk_size)) {
        end_idx <- min(i + chunk_size - 1, nrow(tiles))
        chunk <- tiles[i:end_idx, ]
        
        chunk_sf <- st_as_sf(chunk, wkt = "tile", crs = 4326)
        chunk_sf <- st_make_valid(chunk_sf)
        
        tiles_sf_list[[length(tiles_sf_list) + 1]] <- chunk_sf
        
        if (verbose && length(tiles_sf_list) %% 5 == 0) {
          cat("   Converted", format(end_idx, big.mark = ","), 
              "of", format(nrow(tiles), big.mark = ","), "tiles\n")
        }
        
        rm(chunk, chunk_sf)
        gc(verbose = FALSE)
      }
      
      tiles_sf <- do.call(rbind, tiles_sf_list)
      rm(tiles_sf_list, tiles)
      gc(verbose = FALSE)
    } else {
      tiles_sf <- st_as_sf(tiles, wkt = "tile", crs = 4326)
      tiles_sf <- st_make_valid(tiles_sf)
      rm(tiles)
      gc(verbose = FALSE)
    }
    
    # Precise intersection IN CHUNKS to prevent OOM
    if (verbose) cat("3. Refining with country boundary (chunked)...\n")
    
    if (nrow(tiles_sf) > 50000) {
      chunk_size <- 25000
      tiles_final_list <- list()
      
      for (i in seq(1, nrow(tiles_sf), by = chunk_size)) {
        end_idx <- min(i + chunk_size - 1, nrow(tiles_sf))
        chunk <- tiles_sf[i:end_idx, ]
        
        chunk_final <- st_intersection(chunk, country_boundary)
        tiles_final_list[[length(tiles_final_list) + 1]] <- chunk_final
        
        if (verbose && length(tiles_final_list) %% 5 == 0) {
          cat("   Processed", format(end_idx, big.mark = ","), 
              "of", format(nrow(tiles_sf), big.mark = ","), "tiles\n")
        }
        
        rm(chunk, chunk_final)
        gc(verbose = FALSE)
      }
      
      tiles_final <- do.call(rbind, tiles_final_list)
      rm(tiles_final_list, tiles_sf)
      gc(verbose = FALSE)
    } else {
      tiles_final <- st_intersection(tiles_sf, country_boundary)
      rm(tiles_sf)
      gc(verbose = FALSE)
    }
    
    if (verbose) {
      cat("✓ Final:", format(nrow(tiles_final), big.mark = ","), "tiles\n\n")
    }
    
    # Calculate statistics
    if (verbose) cat("4. Computing statistics...\n")
    stats <- tiles_final %>%
      st_drop_geometry() %>%
      summarise(
        country_code = country_code,
        country_name = country_boundary$country[1],
        year = year,
        quarter = quarter,
        network_type = network_type,
        download_mbps = round(median(avg_d_kbps / 1000, na.rm = TRUE), 1),
        upload_mbps = round(median(avg_u_kbps / 1000, na.rm = TRUE), 1),
        latency_ms = round(median(avg_lat_ms, na.rm = TRUE), 0),
        tile_count = n(),
        total_tests = sum(tests, na.rm = TRUE)
      )
    
    # Save if requested
    if (save_output && !is.null(output_dir)) {
      dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
      output_file <- file.path(
        output_dir,
        paste0(country_code, "_", year, "Q", quarter, "_", network_type, ".rds")
      )
      saveRDS(tiles_final, output_file)
      if (verbose) cat("✓ Saved to:", output_file, "\n")
    }
    
    if (verbose) {
      elapsed <- difftime(Sys.time(), start_time, units = "secs")
      cat("\n✓ Completed in", round(elapsed, 1), "seconds\n")
    }
    
    return(stats)
    
  }, error = function(e) {
    warning("Error: ", country_code, " ", year, " Q", quarter, " ", 
            network_type, ": ", conditionMessage(e))
    return(NULL)
  })
}
