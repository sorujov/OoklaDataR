#!/share/home/orujov/.conda/envs/.ookladatar/bin/Rscript
# =============================================================================
# Main Script: Run All CIS Countries Sequentially (with parallel quarters)
# =============================================================================

cat("\n")
cat(strrep("=", 80), "\n")
cat("OOKLA DATA DOWNLOAD - ALL CIS COUNTRIES (SEQUENTIAL)\n")
cat("Each country processes quarters in PARALLEL\n")
cat(strrep("=", 80), "\n\n")

# Test with Azerbaijan only first, then expand to all 11 CIS countries
CIS_COUNTRIES <- c("AZ", "AM", "BY", "GE", "KZ", "KG", "MD", "TJ", "UZ", "UA")  # Only Azerbaijan for testing device-weighted methodology

cat("Countries to process:", paste(CIS_COUNTRIES, collapse = ", "), "\n")
cat("Total countries:", length(CIS_COUNTRIES), "\n\n")

overall_start <- Sys.time()

# Process each country sequentially
for (i in seq_along(CIS_COUNTRIES)) {
  country <- CIS_COUNTRIES[i]
  
  cat("\n")
  cat(strrep("=", 80), "\n")
  cat("COUNTRY", i, "/", length(CIS_COUNTRIES), ":", country, "\n")
  cat(strrep("=", 80), "\n\n")
  
  country_start <- Sys.time()
  
  # Set the country as an environment variable
  Sys.setenv(TEST_COUNTRY = country)
  
  # Source and run the test script for this country
  tryCatch({
    source("scripts/test_single_country.R")
    
    country_end <- Sys.time()
    duration <- difftime(country_end, country_start, units = "mins")
    
    cat("\n")
    cat(strrep("-", 80), "\n")
    cat("✓ COUNTRY COMPLETE:", country, "\n")
    cat("  Duration:", round(duration, 2), "minutes\n")
    cat(strrep("-", 80), "\n")
    
  }, error = function(e) {
    cat("\n")
    cat(strrep("-", 80), "\n")
    cat("✗ ERROR processing", country, ":", conditionMessage(e), "\n")
    cat(strrep("-", 80), "\n")
  })
}

overall_end <- Sys.time()
total_duration <- difftime(overall_end, overall_start, units = "mins")

cat("\n")
cat(strrep("=", 80), "\n")
cat("✓✓✓ ALL COUNTRIES COMPLETE! ✓✓✓\n")
cat(strrep("=", 80), "\n\n")

cat("Total processing time:", round(total_duration, 2), "minutes\n")
cat("Average time per country:", round(total_duration / length(CIS_COUNTRIES), 2), "minutes\n\n")

cat("Results saved in: data/processed/[COUNTRY_CODE]/\n\n")
