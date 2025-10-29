# =============================================================================
# Ultra Memory-Safe Batch Processing - FINAL OPTIMIZED VERSION
# =============================================================================
# Memory optimizations:
# - Arrow streaming with pre-filtering
# - Aggressive garbage collection
# - Memory monitoring and auto-pause
# - Sequential processing only
# - Resume capability
# =============================================================================

library(tidyverse)
library(sf)
library(here)
library(rnaturalearth)

# Source the modular function
source(here("scripts", "data_download", "process_ookla_data.R"))

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║     Ultra Memory-Safe Batch Processing (Final Optimized)            ║\n")
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

# AGGRESSIVE MEMORY SETTINGS
MEMORY_THRESHOLD_MB <- 3000  # Lower threshold for safety
GC_INTERVAL <- 3             # Force full GC every N tasks

# Disable s2 spherical geometry (reduces memory overhead)
sf_use_s2(FALSE)

# Arrow memory optimizations
options(arrow.unsafe_memory = FALSE)
arrow::set_cpu_count(2)      # Limit CPU threads
arrow::set_io_thread_count(2) # Limit I/O threads

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

# Configuration summary
cat("Configuration:\n")
cat("  Project root:", here(), "\n")
cat("  Output directory:", OUTPUT_DIR, "\n")
cat("  Countries:", length(config$countries), "\n")
cat("  Quarters:", nrow(ALL_QUARTERS), "\n")
cat("  Network types:", length(NETWORK_TYPES), "\n")
cat("  Total tasks:", length(config$countries) * nrow(ALL_QUARTERS) * length(NETWORK_TYPES), "\n")
cat("  Memory threshold:", MEMORY_THRESHOLD_MB, "MB\n")
cat("  GC interval:", GC_INTERVAL, "tasks\n\n")

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
cat("⚠️  Arrow streaming enabled for memory efficiency\n\n")

# =============================================================================
# HELPER: Enhanced Memory Monitor
# =============================================================================

check_memory <- function(threshold_mb = MEMORY_THRESHOLD_MB, 
                        force_cleanup = FALSE) {
  
  # Get detailed memory info
  mem_info <- gc(full = FALSE, verbose = FALSE)
  mem_used_mb <- sum(mem_info[, 2])  # Total MB used
  mem_max_mb <- sum(mem_info[, 6])   # Max used since last reset
  
  mem_percent <- ifelse(mem_max_mb > 0, (mem_used_mb / mem_max_mb) * 100, 0)
  
  if (mem_used_mb > threshold_mb || force_cleanup) {
    cat(sprintf("\n⚠️  Memory: %.0f MB (%.0f%% of session max)\n", 
                mem_used_mb, mem_percent))
    cat("   Forcing full garbage collection...\n")
    
    # Triple GC with pauses
    for (i in 1:3) {
      gc(full = TRUE, verbose = FALSE, reset = TRUE)
      Sys.sleep(1)
    }
    
    mem_info <- gc(full = FALSE, verbose = FALSE)
    mem_used_mb <- sum(mem_info[, 2])
    cat(sprintf("   After cleanup: %.0f MB\n\n", mem_used_mb))
  }
  
  return(mem_used_mb)
}

# =============================================================================
# MAIN LOOP: Process One Country at a Time, One Task at a Time
# =============================================================================

all_results <- list()
error_log <- list()
task_counter <- 0  # Global task counter for GC interval

for (i in seq_along(config$countries)) {
  
  country_code <- config$countries[i]
  country_name <- config$country_names[country_code]
  country_start <- Sys.time()
  
  cat("\n")
  cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
  cat(sprintf("│ Country %d of %d: %-54s │\n", 
              i, length(config$countries), country_name))
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
    cat(sprintf("✓ Skipping %d already completed tasks\n", sum(tasks$completed)))
  }
  
  if (nrow(tasks_remaining) == 0) {
    cat("✅ All tasks complete for", country_name, "!\n")
    next
  }
  
  cat(sprintf("Processing %d tasks sequentially...\n\n", nrow(tasks_remaining)))
  
  # Process each task one at a time
  country_results <- list()
  
  for (j in seq_len(nrow(tasks_remaining))) {
    task <- tasks_remaining[j, ]
    task_counter <- task_counter + 1
    
    # Force cleanup every GC_INTERVAL tasks
    if (task_counter %% GC_INTERVAL == 0) {
      check_memory(force_cleanup = TRUE)
    } else {
      mem_before <- check_memory()
    }
    
    cat(sprintf("[%d/%d] %s %dQ%d %s ", 
                j, nrow(tasks_remaining), 
                country_code, task$year, task$quarter, task$network_type))
    
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
      
      # Verify file was actually created
      output_file <- file.path(OUTPUT_DIR, 
                              paste0(country_code, "_", task$year, "Q", 
                                    task$quarter, "_", task$network_type, ".rds"))
      
      if (file.exists(output_file)) {
        file_size <- file.info(output_file)$size / 1024  # KB
        cat(sprintf("✓ (%.1fs, %.0fKB)\n", task_time, file_size))
      } else {
        cat(sprintf("⚠️  completed but file not saved (%.1fs)\n", task_time))
      }
      
      res
      
    }, error = function(e) {
      cat("✗ ERROR\n")
      cat(sprintf("   %s\n", conditionMessage(e)))
      
      # Force aggressive cleanup after error
      gc(full = TRUE, verbose = FALSE, reset = TRUE)
      Sys.sleep(3)
      
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
    
    # Aggressive cleanup after each task
    gc(full = TRUE, verbose = FALSE, reset = TRUE)
    Sys.sleep(1)
  }
  
  # Collect results for this country
  country_df <- bind_rows(country_results[!sapply(country_results, is.null)])
  
  if (nrow(country_df) > 0) {
    all_results[[i]] <- country_df
    
    # Save country-specific file
    country_file <- file.path(AGGREGATED_DIR, 
                             paste0(country_code, "_all_quarters.csv"))
    write_csv(country_df, country_file)
    
    cat(sprintf("\n✅ Saved %d records for %s\n", nrow(country_df), country_name))
  } else {
    cat(sprintf("\n⚠️  No new results for %s\n", country_name))
  }
  
  # Aggressive memory cleanup between countries
  rm(country_results, country_df, tasks, tasks_remaining)
  gc(full = TRUE, verbose = FALSE, reset = TRUE)
  Sys.sleep(3)
  
  # Progress reporting
  elapsed <- difftime(Sys.time(), country_start, units = "mins")
  total_elapsed <- difftime(Sys.time(), BATCH_START, units = "mins")
  estimated_total <- total_elapsed * length(config$countries) / i
  remaining <- estimated_total - total_elapsed
  
  cat(sprintf("\n✅ %s complete in %.1f minutes\n", country_name, elapsed))
  cat(sprintf("📊 Progress: %d/%d countries | Elapsed: %.1f min | Est. remaining: %.1f min\n",
              i, length(config$countries), total_elapsed, remaining))
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
  master_file <- file.path(AGGREGATED_DIR, "CIS_Internet_Speed_2019-2025.csv")
  write_csv(final_results, master_file)
  cat(sprintf("✓ Saved master file: %d records\n", nrow(final_results)))
  
  # Summary statistics
  summary_stats <- final_results %>%
    group_by(country_name, network_type) %>%
    summarise(
      quarters = n(),
      avg_download = round(mean(download_mbps, na.rm = TRUE), 1),
      avg_upload = round(mean(upload_mbps, na.rm = TRUE), 1),
      avg_latency = round(mean(latency_ms, na.rm = TRUE), 0),
      median_download = round(mean(download_mbps_median, na.rm = TRUE), 1),
      median_upload = round(mean(upload_mbps_median, na.rm = TRUE), 1),
      median_latency = round(mean(latency_ms_median, na.rm = TRUE), 0),
      total_tiles = sum(tile_count, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(country_name, network_type)
  
  summary_file <- file.path(AGGREGATED_DIR, "summary_statistics.csv")
  write_csv(summary_stats, summary_file)
  
  cat("\nSummary Statistics:\n")
  print(summary_stats, n = Inf)
} else {
  cat("⚠️  No results to aggregate\n")
}

# Save error log if any errors occurred
if (length(error_log) > 0) {
  error_df <- bind_rows(error_log)
  error_file <- file.path(LOG_DIR, 
                         paste0("errors_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".csv"))
  write_csv(error_df, error_file)
  
  cat(sprintf("\n⚠️  %d tasks failed - error log saved\n", nrow(error_df)))
  cat("   Most common errors:\n")
  
  error_summary <- error_df %>%
    count(error, sort = TRUE) %>%
    head(3)
  
  for (k in seq_len(nrow(error_summary))) {
    cat(sprintf("   - %s (%d times)\n", 
                error_summary$error[k], error_summary$n[k]))
  }
}

TOTAL_TIME <- difftime(Sys.time(), BATCH_START, units = "hours")

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                  ✅ BATCH COMPLETE!                                  ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat(sprintf("📊 Total time: %.2f hours\n", TOTAL_TIME))
cat(sprintf("📁 Output directory: %s\n", AGGREGATED_DIR))
cat(sprintf("📝 Processed files: %d\n", 
            length(list.files(OUTPUT_DIR, pattern = "\\.rds$"))))

# Final memory report
final_mem <- gc(full = TRUE, verbose = FALSE)
cat(sprintf("💾 Final memory usage: %.0f MB\n\n", sum(final_mem[, 2])))

cat("💡 To resume if interrupted, just run this script again.\n")
cat("   It will automatically skip completed tasks.\n\n")
