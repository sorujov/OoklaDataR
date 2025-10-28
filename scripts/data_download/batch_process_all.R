# =============================================================================
# Parallel Batch Processing: All CIS Countries (2019-2025)
# =============================================================================
# Strategy: One country at a time, all quarters in parallel
# Memory-safe approach for large-scale processing
# =============================================================================

library(tidyverse)
library(sf)
library(here)
library(rnaturalearth)
library(furrr)
library(future)

# Source the modular function
source(here("scripts", "data_download", "process_ookla_data.R"))

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║     Parallel Batch: All CIS Countries & Quarters (2019-2025)        ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

BATCH_START <- Sys.time()

# =============================================================================
# CONFIGURATION
# =============================================================================

OUTPUT_DIR <- here("data", "processed")
AGGREGATED_DIR <- here("data", "aggregated")
LOG_DIR <- here("logs")

dir.create(OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(AGGREGATED_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(LOG_DIR, recursive = TRUE, showWarnings = FALSE)

# Load config
config <- readRDS(here("config.rds"))

# All quarters (2019 Q2 - 2025 Q3)
ALL_QUARTERS <- expand.grid(
  year = 2019:2025,
  quarter = 1:4,
  stringsAsFactors = FALSE
) %>%
  filter(!(year == 2019 & quarter == 1)) %>%
  filter(!(year == 2025 & quarter == 4)) %>%
  arrange(year, quarter)

NETWORK_TYPES <- c("fixed", "mobile")

cat("Configuration:\n")
cat("  Countries:", length(config$countries), "\n")
cat("  Quarters:", nrow(ALL_QUARTERS), "\n")
cat("  Network types:", length(NETWORK_TYPES), "\n")
cat("  Total tasks:", length(config$countries) * nrow(ALL_QUARTERS) * length(NETWORK_TYPES), "\n\n")

# =============================================================================
# SETUP
# =============================================================================

cat("Loading country boundaries...\n")
boundaries_file <- here("data", "boundaries", "cis_countries.rds")

if (!file.exists(boundaries_file)) {
  world <- ne_countries(scale = "medium", returnclass = "sf")
  cis_countries <- world %>%
    filter(iso_a2 %in% config$countries) %>%
    select(country = name, iso_a2, geometry) %>%
    st_make_valid()
  saveRDS(cis_countries, boundaries_file)
} else {
  cis_countries <- readRDS(boundaries_file)
}

cat("✓ Loaded", nrow(cis_countries), "countries\n\n")

# Setup parallel cores
n_cores <- max(1, parallel::detectCores() - 1)
cat("Using", n_cores, "CPU cores for parallel processing\n\n")

# =============================================================================
# MAIN LOOP: Process One Country at a Time
# =============================================================================

all_results <- list()

for (i in seq_along(config$countries)) {
  
  country_code <- config$countries[i]
  country_name <- config$country_names[country_code]
  country_start <- Sys.time()
  
  cat("\n")
  cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
  cat("│ Country", i, "of", length(config$countries), ":", 
      country_name, paste(rep(" ", 50 - nchar(country_name)), collapse = ""), "│\n")
  cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
  
  # Create task list
  tasks <- expand.grid(
    year = ALL_QUARTERS$year,
    quarter = ALL_QUARTERS$quarter,
    network_type = NETWORK_TYPES,
    stringsAsFactors = FALSE
  )
  
  # Check for completed tasks
  tasks$output_file <- file.path(
    OUTPUT_DIR,
    paste0(country_code, "_", tasks$year, "Q", tasks$quarter, "_", 
           tasks$network_type, ".rds")
  )
  tasks$completed <- file.exists(tasks$output_file)
  
  tasks_remaining <- tasks %>% filter(!completed)
  
  if (sum(tasks$completed) > 0) {
    cat("Skipping", sum(tasks$completed), "already completed tasks\n")
  }
  
  if (nrow(tasks_remaining) == 0) {
    cat("All tasks complete for", country_name, "!\n")
    next
  }
  
  cat("Processing", nrow(tasks_remaining), "tasks in parallel...\n\n")
  
  # Setup parallel backend
  plan(multisession, workers = n_cores)
  
  # Process all quarters in parallel
  country_results <- future_pmap(
    tasks_remaining %>% select(year, quarter, network_type),
    function(year, quarter, network_type) {
      process_ookla_data(
        country_code = country_code,
        year = year,
        quarter = quarter,
        network_type = network_type,
        cis_countries = cis_countries,
        config = config,
        save_output = TRUE,
        output_dir = OUTPUT_DIR,
        verbose = FALSE
      )
    },
    .options = furrr_options(seed = TRUE),
    .progress = TRUE
  )
  
  # Close parallel backend
  plan(sequential)
  
  # Collect results
  country_df <- bind_rows(country_results[!sapply(country_results, is.null)])
  
  if (nrow(country_df) > 0) {
    all_results[[i]] <- country_df
    
    # Save country-specific file
    write_csv(country_df, 
              file.path(AGGREGATED_DIR, paste0(country_code, "_all_quarters.csv")))
  }
  
  # Clean memory
  rm(country_results, country_df, tasks, tasks_remaining)
  gc()
  
  elapsed <- difftime(Sys.time(), country_start, units = "mins")
  cat("\n✅", country_name, "complete in", round(elapsed, 1), "minutes\n")
  
  # Progress estimate
  total_elapsed <- difftime(Sys.time(), BATCH_START, units = "mins")
  estimated_total <- total_elapsed * length(config$countries) / i
  remaining <- estimated_total - total_elapsed
  
  cat("Progress:", i, "/", length(config$countries), "countries |",
      "Elapsed:", round(total_elapsed, 1), "min |",
      "Est. remaining:", round(remaining, 1), "min\n")
}

# =============================================================================
# FINAL AGGREGATION
# =============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                     Creating Final Dataset                           ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

final_results <- bind_rows(all_results)

if (nrow(final_results) > 0) {
  # Save master file
  write_csv(final_results, file.path(AGGREGATED_DIR, "CIS_Internet_Speed_2019-2025.csv"))
  cat("✓ Saved master file: CIS_Internet_Speed_2019-2025.csv\n\n")
  
  # Summary statistics
  summary_stats <- final_results %>%
    group_by(country_name, network_type) %>%
    summarise(
      quarters = n(),
      avg_download = round(mean(download_mbps, na.rm = TRUE), 1),
      avg_upload = round(mean(upload_mbps, na.rm = TRUE), 1),
      avg_latency = round(mean(latency_ms, na.rm = TRUE), 0),
      total_tiles = sum(tile_count, na.rm = TRUE),
      .groups = "drop"
    )
  
  write_csv(summary_stats, file.path(AGGREGATED_DIR, "summary_statistics.csv"))
  
  cat("Summary Statistics:\n")
  print(summary_stats, n = Inf)
}

TOTAL_TIME <- difftime(Sys.time(), BATCH_START, units = "hours")

cat("\n\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                  ✅ BATCH COMPLETE!                                  ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("📊 Total time:", round(TOTAL_TIME, 2), "hours\n")
cat("📁 Output:", AGGREGATED_DIR, "\n\n")
