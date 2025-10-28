# =============================================================================
# Ultra Memory-Safe Batch Processing
# =============================================================================
# This version includes:
# - Sequential processing only (no parallelization)
# - Aggressive garbage collection
# - Memory monitoring
# - Automatic pausing if memory gets too high
# - Resume capability
# =============================================================================

library(tidyverse)
library(sf)
library(here)
library(rnaturalearth)

# Source the modular function
source(here("scripts", "data_download", "process_ookla_data.R"))

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║     Ultra Memory-Safe Batch Processing (Sequential Only)            ║\n")
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

# Memory limit threshold (in MB) - adjust based on your system
MEMORY_THRESHOLD_MB <- 4000  # Pause if memory usage exceeds this

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
cat("  Total tasks:", length(config$countries) * nrow(ALL_QUARTERS) * length(NETWORK_TYPES), "\n")
cat("  Memory threshold:", MEMORY_THRESHOLD_MB, "MB\n\n")

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

cat("⚠️  SEQUENTIAL MODE: Processing one task at a time\n")
cat("⚠️  This is the slowest but MOST reliable approach\n\n")

# =============================================================================
# HELPER: Check Memory Usage
# =============================================================================
check_memory <- function(threshold_mb = MEMORY_THRESHOLD_MB, wait_if_high = TRUE) {
  mem_info <- gc(full = FALSE, verbose = FALSE)
  mem_used_mb <- mem_info[2, 2]  # Ncells (MB)
  
  if (mem_used_mb > threshold_mb && wait_if_high) {
    cat("\n⚠️  High memory usage:", round(mem_used_mb, 1), "MB\n")
    cat("   Forcing garbage collection and waiting...\n")
    
    gc(full = TRUE, verbose = FALSE)
    Sys.sleep(5)
    
    mem_info <- gc(full = FALSE, verbose = FALSE)
    mem_used_mb <- mem_info[2, 2]
    cat("   After cleanup:", round(mem_used_mb, 1), "MB\n\n")
  }
  
  return(mem_used_mb)
}

# =============================================================================
# MAIN LOOP: Process One Country at a Time, One Task at a Time
# =============================================================================

all_results <- list()
error_log <- list()

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
  tasks <- ALL_QUARTERS %>%
    crossing(network_type = NETWORK_TYPES)
  
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
  
  cat("Processing", nrow(tasks_remaining), "tasks SEQUENTIALLY...\n\n")
  
  # Process each task one at a time
  country_results <- list()
  
  for (j in seq_len(nrow(tasks_remaining))) {
    task <- tasks_remaining[j, ]
    
    # Check memory before task
    mem_before <- check_memory()
    
    cat(sprintf("[%d/%d] %s %dQ%d %s (mem: %.0fMB)...", 
                j, nrow(tasks_remaining), 
                country_code, task$year, task$quarter, task$network_type,
                mem_before))
    
    task_start <- Sys.time()
    
    result <- tryCatch({
      res <- process_ookla_data(
        country_code = country_code,
        year = task$year,
        quarter = task$quarter,
        network_type = task$network_type,
        cis_countries = cis_countries,
        config = config,
        save_output = TRUE,
        output_dir = OUTPUT_DIR,
        verbose = FALSE
      )
      
      task_time <- difftime(Sys.time(), task_start, units = "secs")
      cat(sprintf(" ✓ (%.1fs)\n", task_time))
      res
      
    }, error = function(e) {
      cat(" ✗ ERROR\n")
      cat("   Error message:", conditionMessage(e), "\n")
      
      # Log error
      error_log[[length(error_log) + 1]] <<- list(
        country = country_code,
        year = task$year,
        quarter = task$quarter,
        network_type = task$network_type,
        error = conditionMessage(e),
        timestamp = Sys.time()
      )
      
      return(NULL)
    })
    
    country_results[[j]] <- result
    
    # Aggressive memory cleanup after EACH task
    gc(full = TRUE, verbose = FALSE)
    
    # Brief pause to let system stabilize
    Sys.sleep(1)
    
    # Check memory after task
    mem_after <- check_memory(wait_if_high = TRUE)
  }
  
  # Collect results for this country
  country_df <- bind_rows(country_results[!sapply(country_results, is.null)])
  
  if (nrow(country_df) > 0) {
    all_results[[i]] <- country_df
    
    # Save country-specific file
    write_csv(country_df, 
              file.path(AGGREGATED_DIR, paste0(country_code, "_all_quarters.csv")))
    
    cat("\n✅ Saved results for", country_name, "\n")
  } else {
    cat("\n⚠️  No new results for", country_name, "(may have been errors or all skipped)\n")
  }
  
  # Aggressive memory cleanup between countries
  rm(country_results, country_df, tasks, tasks_remaining)
  gc(full = TRUE, verbose = FALSE)
  
  # Longer pause between countries
  Sys.sleep(3)
  
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

# Save error log
if (length(error_log) > 0) {
  error_df <- bind_rows(error_log)
  write_csv(error_df, file.path(LOG_DIR, paste0("errors_", 
                                                 format(Sys.time(), "%Y%m%d_%H%M%S"), 
                                                 ".csv")))
  cat("\n⚠️  Errors occurred:", nrow(error_df), "tasks failed\n")
  cat("   Error log saved to:", LOG_DIR, "\n")
}

TOTAL_TIME <- difftime(Sys.time(), BATCH_START, units = "hours")

cat("\n\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                  ✅ BATCH COMPLETE!                                  ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("📊 Total time:", round(TOTAL_TIME, 2), "hours\n")
cat("📁 Output:", AGGREGATED_DIR, "\n")
cat("📝 Processed files:", list.files(OUTPUT_DIR, pattern = "\\.rds$") %>% length(), "\n\n")

cat("💡 To resume if interrupted, just run this script again.\n")
cat("   It will automatically skip completed tasks.\n\n")
