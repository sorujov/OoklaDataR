# =============================================================================
# Core Configuration Helper
# =============================================================================
# Run this to see your system specs and get recommendations
# =============================================================================

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║              System Configuration for Parallel Processing            ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

# Detect system info
available_cores <- parallel::detectCores()
cat("🖥️  CPU Cores Available:", available_cores, "\n")

# Try to get RAM info (works on Windows with systeminfo)
if (.Platform$OS.type == "windows") {
  tryCatch({
    # Get total RAM in GB (approximate)
    system_info <- system("wmic ComputerSystem get TotalPhysicalMemory", intern = TRUE)
    ram_bytes <- as.numeric(gsub("[^0-9]", "", system_info[2]))
    ram_gb <- round(ram_bytes / (1024^3), 1)
    cat("💾 Total RAM:", ram_gb, "GB\n\n")
    
    # Provide recommendations
    cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
    cat("║                          RECOMMENDATIONS                              ║\n")
    cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")
    
    if (ram_gb < 12) {
      cat("⚠️  LOW RAM SYSTEM (< 12 GB)\n")
      cat("   Recommended: n_cores = 1 (sequential mode)\n")
      cat("   Expected time: 8-10 hours for all countries\n\n")
      recommended <- 1
    } else if (ram_gb < 20) {
      cat("⚖️  MEDIUM RAM SYSTEM (12-20 GB)\n")
      cat("   Recommended: n_cores = 2 (balanced mode)\n")
      cat("   Expected time: 4-6 hours for all countries\n\n")
      recommended <- 2
    } else if (ram_gb < 32) {
      cat("💪 HIGH RAM SYSTEM (20-32 GB)\n")
      cat("   Recommended: n_cores = 3-4 (fast mode)\n")
      cat("   Expected time: 2-4 hours for all countries\n\n")
      recommended <- 3
    } else {
      cat("🚀 VERY HIGH RAM SYSTEM (32+ GB)\n")
      cat("   Recommended: n_cores = 4-6 (maximum speed)\n")
      cat("   Expected time: 1-3 hours for all countries\n\n")
      recommended <- min(4, available_cores - 2)
    }
    
  }, error = function(e) {
    cat("💾 RAM: Unable to detect automatically\n\n")
    cat("Recommended: Start with n_cores = 1 and increase if no errors\n\n")
    recommended <- 1
  })
} else {
  cat("💾 RAM: Detection not available on this platform\n\n")
  cat("Recommended: Start with n_cores = 1 and increase if no errors\n\n")
  recommended <- 1
}

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                      HOW TO CONFIGURE                                 ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("1. Open: scripts/data_download/batch_process_all.R\n\n")
cat("2. Find this line (around line 81):\n")
cat("   n_cores <- 1\n\n")
cat("3. Change it to:\n")
if (exists("recommended")) {
  cat("   n_cores <-", recommended, " # Recommended for your system\n\n")
} else {
  cat("   n_cores <- 2  # Or higher if you have more RAM\n\n")
}

cat("4. Save and run:\n")
cat("   Rscript scripts/data_download/batch_process_all.R\n\n")

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                         PARALLELIZATION GUIDE                         ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("┌───────────┬──────────────┬─────────────┬─────────────────────────────┐\n")
cat("│  n_cores  │  RAM Needed  │    Speed    │         Description         │\n")
cat("├───────────┼──────────────┼─────────────┼─────────────────────────────┤\n")
cat("│     1     │    8-16 GB   │   Slowest   │ Most reliable, sequential   │\n")
cat("│     2     │   16-24 GB   │   Balanced  │ Good speed/safety tradeoff  │\n")
cat("│     3     │   24-32 GB   │    Fast     │ Parallel, needs more RAM    │\n")
cat("│     4     │   32-40 GB   │    Faster   │ High parallelization        │\n")
cat("│    5-6    │    40+ GB    │   Fastest   │ Maximum speed, lots of RAM  │\n")
cat("└───────────┴──────────────┴─────────────┴─────────────────────────────┘\n\n")

cat("💡 TIPS:\n")
cat("   • Start conservative (lower n_cores) and increase if no OOM errors\n")
cat("   • Close other applications to free up RAM\n")
cat("   • Checkpoints work automatically - you can always resume\n")
cat("   • Monitor progress: Rscript scripts/data_download/check_progress.R\n\n")

cat("⚠️  IF YOU GET OOM ERRORS:\n")
cat("   • Reduce n_cores by 1\n")
cat("   • Or use: scripts/data_download/ultra_memory_safe_batch.R\n\n")
