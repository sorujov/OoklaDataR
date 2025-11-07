#!/share/home/orujov/.conda/envs/.ookladatar/bin/Rscript
# =============================================================================
# Russia Download with Batched Parallel Processing (Memory-Efficient)
# =============================================================================
# Process Russia in small batches to avoid OOM with lower memory allocation
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
cat("RUSSIA OOKLA DOWNLOAD - BATCHED PROCESSING\n")
cat(strrep("=", 80), "\n\n")

S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"
TEST_COUNTRY <- "RU"
START_YEAR <- 2019
END_YEAR <- 2025
QUARTERS <- c(1,2,3,4)
NETWORK_TYPES <- c("mobile","fixed")

# BATCH SIZE: Process 4 tasks at a time to control memory
BATCH_SIZE <- 4

if (Sys.getenv("SLURM_CPUS_PER_TASK") != "") {
  N_CORES_TOTAL <- as.integer(Sys.getenv("SLURM_CPUS_PER_TASK"))
  N_CORES <- min(BATCH_SIZE, N_CORES_TOTAL)
  cat("Running on SLURM with", N_CORES_TOTAL, "cores (", N_CORES, "per batch)\n")
} else {
  N_CORES <- 4
  cat("Running locally with", N_CORES, "cores\n")
}

cat("\nConfiguration:\n")
cat("  Country: Russia (RU)\n")
cat("  Period:", START_YEAR, "-", END_YEAR, "\n")
cat("  Batch size:", BATCH_SIZE, "tasks at a time\n")
cat("  Memory-efficient mode: YES\n\n")

# -----------------------------------------------------------------------------
# Utility functions
# -----------------------------------------------------------------------------
weighted_median <- function(values, weights) {
  if (length(values) == 0 || all(is.na(values))) return(NA_real_)
  valid_idx <- !is.na(values) & !is.na(weights) & weights > 0
  if (sum(valid_idx) == 0) return(NA_real_)
  values <- values[valid_idx]; weights <- weights[valid_idx]
  ord <- order(values); values <- values[ord]; weights <- weights[ord]
  cum_weights <- cumsum(weights); total_weight <- sum(weights)
  median_pos <- total_weight / 2
  idx <- which(cum_weights >= median_pos)[1]
  values[idx]
}

list_s3_files <- function(data_type, year, quarter) {
  prefix <- paste0("parquet/performance/type=", data_type,
                   "/year=", year, "/quarter=", quarter, "/")
  list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", URLencode(prefix, reserved=TRUE))
  tryCatch({
    response <- GET(list_url, timeout(30))
    if (status_code(response) != 200) return(NULL)
    content_text <- content(response, "text", encoding="UTF-8")
    all_keys <- str_match_all(content_text, "<Key>([^<]+)</Key>")[[1]]
    keys <- if (nrow(all_keys) > 0) all_keys[,2] else character(0)
    keys <- keys[str_detect(keys, "\\.parquet$")]
    if (length(keys) > 0) keys else NULL
  }, error=function(e) { message("Error listing S3: ", e$message); NULL })
}

generate_quadkey_prefixes <- function(bbox, zoom=4) {
  latlon_to_quadkey <- function(lat, lon, zoom) {
    lat_rad <- lat * pi / 180; n <- 2^zoom
    x <- floor((lon + 180) / 360 * n)
    y <- floor((1 - log(tan(lat_rad) + 1/cos(lat_rad)) / pi) / 2 * n)
    quadkey <- ""; for (i in zoom:1) { digit <- 0; mask <- bitwShiftL(1, i-1)
      if (bitwAnd(as.integer(x), as.integer(mask)) != 0) digit <- digit + 1
      if (bitwAnd(as.integer(y), as.integer(mask)) != 0) digit <- digit + 2
      quadkey <- paste0(quadkey, digit) }
    quadkey }
  lat_seq <- seq(bbox$ymin, bbox$ymax, by=0.5)
  lon_seq <- seq(bbox$xmin, bbox$xmax, by=0.5)
  grid <- expand.grid(lat=lat_seq, lon=lon_seq)
  quadkeys <- mapply(latlon_to_quadkey, grid$lat, grid$lon, MoreArgs=list(zoom=zoom))
  unique(quadkeys)
}

process_parquet_file <- function(file_key, country_boundary, bbox, quadkeys) {
  tryCatch({
    s3_uri <- paste0("s3://ookla-open-data/", file_key)
    ds <- open_dataset(s3_uri, format="parquet")
    schema_cols <- names(ds$schema)
    has_tile_coords <- all(c("tile_x","tile_y") %in% schema_cols)
    if (has_tile_coords) {
      df <- ds %>% filter(tile_x >= bbox$xmin & tile_x <= bbox$xmax,
                          tile_y >= bbox$ymin & tile_y <= bbox$ymax) %>% collect()
    } else {
      prefix_length <- nchar(quadkeys[1])
      df <- ds %>% mutate(qk_prefix = substr(quadkey, 1, prefix_length)) %>%
        filter(qk_prefix %in% quadkeys) %>% select(-qk_prefix) %>% collect()
    }
    if (nrow(df) == 0) return(NULL)
    tiles_sf <- st_as_sf(df, wkt="tile", crs=4326)
    tiles_sf <- st_make_valid(tiles_sf)
    tiles_filtered <- suppressWarnings(st_filter(tiles_sf, country_boundary, .predicate=st_intersects))
    if (nrow(tiles_filtered) > 0) st_drop_geometry(tiles_filtered) else NULL
  }, error=function(e) { message("Error processing file: ", e$message); NULL })
}

calculate_statistics <- function(tiles_df) {
  if (is.null(tiles_df) || nrow(tiles_df) == 0) return(NULL)
  tiles_df <- tiles_df %>% mutate(download_mbps = avg_d_kbps / 1000,
                                  upload_mbps = avg_u_kbps / 1000,
                                  latency_ms = avg_lat_ms)
  total_tests <- sum(tiles_df$tests, na.rm=TRUE)
  total_devices <- sum(tiles_df$devices, na.rm=TRUE)
  list(
    download_weighted_mean = weighted.mean(tiles_df$download_mbps, tiles_df$devices, na.rm=TRUE),
    upload_weighted_mean = weighted.mean(tiles_df$upload_mbps, tiles_df$devices, na.rm=TRUE),
    latency_weighted_mean = weighted.mean(tiles_df$latency_ms, tiles_df$devices, na.rm=TRUE),
    download_weighted_median = weighted_median(tiles_df$download_mbps, tiles_df$devices),
    upload_weighted_median = weighted_median(tiles_df$upload_mbps, tiles_df$devices),
    latency_weighted_median = weighted_median(tiles_df$latency_ms, tiles_df$devices),
    download_mean = mean(tiles_df$download_mbps, na.rm=TRUE),
    upload_mean = mean(tiles_df$upload_mbps, na.rm=TRUE),
    latency_mean = mean(tiles_df$latency_ms, na.rm=TRUE),
    download_median = median(tiles_df$download_mbps, na.rm=TRUE),
    upload_median = median(tiles_df$upload_mbps, na.rm=TRUE),
    latency_median = median(tiles_df$latency_ms, na.rm=TRUE),
    tile_count = nrow(tiles_df),
    total_tests = total_tests,
    total_devices = total_devices
  )
}

# -----------------------------------------------------------------------------
# Load Russia boundary
# -----------------------------------------------------------------------------
cat("Loading Russia boundary...\n")
boundaries_file <- here("data","boundaries","cis_countries.rds")
if (!file.exists(boundaries_file)) {
  cat("Creating boundaries file...\n")
  library(rnaturalearth)
  CIS_COUNTRIES <- c("AM","AZ","BY","GE","KZ","KG","MD","RU","TJ","TM","UZ")
  world <- ne_countries(scale="medium", returnclass="sf")
  cis_boundaries <- world %>% filter(iso_a2 %in% CIS_COUNTRIES) %>%
    select(country=name, iso_a2, iso_a3, geometry) %>% st_make_valid()
  dir.create(dirname(boundaries_file), recursive=TRUE, showWarnings=FALSE)
  saveRDS(cis_boundaries, boundaries_file)
}
cis_boundaries <- readRDS(boundaries_file)
country_boundary <- cis_boundaries %>% filter(iso_a2 == "RU")
country_name <- "Russia"

country_bbox_raw <- st_bbox(country_boundary)
COUNTRY_BBOX <- list(
  xmin = country_bbox_raw["xmin"] - 0.5,
  ymin = country_bbox_raw["ymin"] - 0.5,
  xmax = country_bbox_raw["xmax"] + 0.5,
  ymax = country_bbox_raw["ymax"] + 0.5
)
COUNTRY_QUADKEYS <- generate_quadkey_prefixes(COUNTRY_BBOX, zoom=4)

cat("✓ Russia boundary loaded\n")
cat("  Bbox: [", sprintf("%.2f", COUNTRY_BBOX$xmin), ",",
    sprintf("%.2f", COUNTRY_BBOX$ymin), "] to [",
    sprintf("%.2f", COUNTRY_BBOX$xmax), ",",
    sprintf("%.2f", COUNTRY_BBOX$ymax), "]\n\n")

# Output setup
OUTPUT_DIR <- here("data","processed","RU")
if (!dir.exists(OUTPUT_DIR)) dir.create(OUTPUT_DIR, recursive=TRUE, showWarnings=FALSE)
INCREMENTAL_FILE <- file.path(OUTPUT_DIR, "RU_incremental.csv")

# -----------------------------------------------------------------------------
# Task list
# -----------------------------------------------------------------------------
tasks <- expand.grid(year=START_YEAR:END_YEAR, quarter=QUARTERS, network_type=NETWORK_TYPES,
                     stringsAsFactors=FALSE) %>% arrange(year, quarter, network_type) %>%
  filter(!(year==2025 & quarter==4))

cat("Total tasks:", nrow(tasks), "\n")
cat("Will process in", ceiling(nrow(tasks)/BATCH_SIZE), "batches\n\n")

# -----------------------------------------------------------------------------
# Quarter processor
# -----------------------------------------------------------------------------
process_quarter <- function(year, quarter, network_type, country_boundary, bbox, quadkeys, log_file=NULL) {
  id <- paste0("RU_", year, "Q", quarter, "_", network_type)
  log_msg <- function(msg) {
    timestamp <- format(Sys.time(), "%H:%M:%S")
    full_msg <- paste(timestamp, "-", id, msg)
    cat(full_msg, "\n")
    if (!is.null(log_file)) cat(full_msg, "\n", file=log_file, append=TRUE)
  }
  log_msg("")
  tryCatch({
    file_keys <- list_s3_files(network_type, year, quarter)
    if (is.null(file_keys) || length(file_keys) == 0) {
      log_msg("  ⚠ No files in S3")
      return(NULL)
    }
    log_msg(paste("  Found", length(file_keys), "files, filtering..."))
    all_tiles <- list()
    for (fk in file_keys) {
      tiles <- process_parquet_file(fk, country_boundary, bbox, quadkeys)
      if (!is.null(tiles)) all_tiles[[length(all_tiles)+1]] <- tiles
    }
    if (length(all_tiles) == 0) {
      log_msg("  ⚠ No tiles in Russia")
      return(NULL)
    }
    combined <- bind_rows(all_tiles)
    stats <- calculate_statistics(combined)
    if (is.null(stats)) return(NULL)
    result <- tibble(country_code="RU", country_name="Russia",
                     year=year, quarter=quarter, network_type=network_type,
                     date=as.Date(paste0(year,"-",quarter*3-2,"-01")),
                     download_mbps_weighted_mean = stats$download_weighted_mean,
                     upload_mbps_weighted_mean = stats$upload_weighted_mean,
                     latency_ms_weighted_mean = stats$latency_weighted_mean,
                     download_mbps_weighted_median = stats$download_weighted_median,
                     upload_mbps_weighted_median = stats$upload_weighted_median,
                     latency_ms_weighted_median = stats$latency_weighted_median,
                     download_mbps_mean = stats$download_mean,
                     upload_mbps_mean = stats$upload_mean,
                     latency_ms_mean = stats$latency_mean,
                     download_mbps_median = stats$download_median,
                     upload_mbps_median = stats$upload_median,
                     latency_ms_median = stats$latency_median,
                     tile_count = stats$tile_count,
                     total_tests = stats$total_tests,
                     total_devices = stats$total_devices)
    log_msg(paste("  ✓", format(stats$tile_count, big.mark=","), "tiles,",
                  format(stats$total_tests, big.mark=","), "tests"))
    if (!file.exists(INCREMENTAL_FILE)) {
      write.table(result, INCREMENTAL_FILE, sep=",", row.names=FALSE, col.names=TRUE, append=FALSE)
    } else {
      write.table(result, INCREMENTAL_FILE, sep=",", row.names=FALSE, col.names=FALSE, append=TRUE)
    }
    log_msg("  ↻ Saved incrementally")
    result
  }, error=function(e) {
    log_msg(paste("  ✗ ERROR:", e$message))
    NULL
  })
}

# -----------------------------------------------------------------------------
# Batched processing
# -----------------------------------------------------------------------------
cat(strrep("-",80), "\nSTARTING BATCHED DOWNLOAD...\n", strrep("-",80), "\n\n")
start_time <- Sys.time()

log_dir <- here("logs"); dir.create(log_dir, recursive=TRUE, showWarnings=FALSE)
log_file <- file.path(log_dir, paste0("parallel_RU_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))

country_boundary_wkt <- st_as_text(st_geometry(country_boundary)[[1]])

all_results <- list()
num_batches <- ceiling(nrow(tasks) / BATCH_SIZE)

for (batch_idx in 1:num_batches) {
  batch_start <- (batch_idx - 1) * BATCH_SIZE + 1
  batch_end <- min(batch_idx * BATCH_SIZE, nrow(tasks))
  batch_tasks <- tasks[batch_start:batch_end, ]
  
  cat("\n>>> BATCH", batch_idx, "of", num_batches, 
      "(tasks", batch_start, "to", batch_end, ")\n")
  
  # Create cluster for this batch only
  cl <- makeCluster(min(N_CORES, nrow(batch_tasks)), type="PSOCK")
  
  clusterExport(cl, c("process_quarter","process_parquet_file","list_s3_files",
                      "calculate_statistics","weighted_median","generate_quadkey_prefixes",
                      "country_boundary_wkt","S3_BASE","COUNTRY_BBOX","COUNTRY_QUADKEYS",
                      "log_file","INCREMENTAL_FILE","batch_tasks"), envir=environment())
  
  clusterEvalQ(cl, {
    library(tidyverse); library(arrow); library(sf); library(httr); library(here)
  })
  
  batch_results <- parLapply(cl, 1:nrow(batch_tasks), function(i) {
    task <- batch_tasks[i,]
    country_boundary_worker <- st_sf(geometry=st_as_sfc(country_boundary_wkt, crs=4326))
    process_quarter(task$year, task$quarter, task$network_type,
                   country_boundary_worker, COUNTRY_BBOX, COUNTRY_QUADKEYS, log_file)
  })
  
  stopCluster(cl)
  
  # Clean up after batch
  gc(verbose=FALSE)
  
  all_results <- c(all_results, batch_results)
  
  cat(">>> Batch", batch_idx, "complete. Memory cleaned.\n")
}

end_time <- Sys.time()

# -----------------------------------------------------------------------------
# Final save
# -----------------------------------------------------------------------------
cat("\n", strrep("-",80), "\nPROCESSING RESULTS...\n", strrep("-",80), "\n\n")
results_df <- bind_rows(all_results[!sapply(all_results, is.null)])

if (nrow(results_df) == 0) {
  cat("⚠ No data collected!\n\n")
  quit(status=1)
}

final_file <- file.path(OUTPUT_DIR, "RU_all_quarters.csv")
write_csv(results_df, final_file)

summary_stats <- results_df %>% group_by(network_type) %>%
  summarise(quarters=n(), avg_download_mean=mean(download_mbps_mean, na.rm=TRUE),
            total_tiles=sum(tile_count, na.rm=TRUE), total_tests=sum(total_tests, na.rm=TRUE),
            .groups="drop")

cat("\n", strrep("=",80), "\n✓ RUSSIA DOWNLOAD COMPLETE!\n", strrep("=",80), "\n\n")
cat("Processing time:", round(difftime(end_time, start_time, units="mins"),2), "minutes\n")
cat("Records:", nrow(results_df), "/", nrow(tasks), "tasks\n\n")
cat("Summary:\n"); print(summary_stats, n=Inf)
cat("\nFinal output:", final_file, "\n")
cat("Incremental:", INCREMENTAL_FILE, "\n")
cat("\n", strrep("=",80), "\n")
