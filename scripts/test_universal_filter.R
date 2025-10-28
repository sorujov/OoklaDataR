# =============================================================================
# Test Universal CIS Filtering - Single Quarter
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(httr)
library(rnaturalearth)
library(lwgeom)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║     Test Universal CIS Filtering: 2019 Q2 (All Countries)           ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

config <- readRDS(here("config.rds"))
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

TEST_YEAR <- 2019
TEST_QUARTER <- 2
TEST_TYPE <- "fixed"  # Test with just fixed for speed

cat("Testing:", TEST_YEAR, "Q", TEST_QUARTER, "-", TEST_TYPE, "\n\n")

# Create directories
dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "processed"), recursive = TRUE, showWarnings = FALSE)

# Build S3 path
prefix <- paste0(config$s3_prefix, "type=", TEST_TYPE, "/year=", TEST_YEAR, "/quarter=", TEST_QUARTER, "/")

# List files
cat("1. Listing files in S3...\n")
list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", URLencode(prefix, reserved = TRUE))
response <- GET(list_url, timeout(60))

content_text <- content(response, "text", encoding = "UTF-8")
keys <- str_extract_all(content_text, "<Key>([^<]+\\.parquet)</Key>")[[1]]
keys <- str_replace_all(keys, "</?Key>", "")

cat("✓ Found", length(keys), "file(s)\n\n")

# Download to raw
parquet_file <- here("data", "raw", basename(keys[1]))

if (!file.exists(parquet_file) || file.info(parquet_file)$size < 50*1024^2) {
  cat("2. Downloading...\n")
  url <- paste0(S3_BASE, "/", keys[1])
  dl_resp <- GET(url, write_disk(parquet_file, overwrite = TRUE), progress(), timeout(300))
  
  if (status_code(dl_resp) == 200) {
    file_size <- file.info(parquet_file)$size / (1024^2)
    cat("✓ Downloaded:", round(file_size, 1), "MB\n\n")
  }
} else {
  cat("2. File already exists\n\n")
}

# Run filtering script
cat("3. Running universal filtering script...\n\n")
source(here("scripts", "02_filter_by_country.R"), local = TRUE)

# Check results
processed_files <- list.files(here("data", "processed"), pattern = paste0("_", TEST_YEAR, "_", TEST_QUARTER, "_"), full.names = TRUE)

cat("\n\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                         Test Complete!                                ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

if (length(processed_files) > 0) {
  cat("✅ SUCCESS! Created", length(processed_files), "country files:\n\n")
  
  for (file in processed_files) {
    data <- readRDS(file)
    cat("  ", basename(file), ":", format(nrow(data), big.mark = ","), "tiles\n")
  }
  
  cat("\n✨ Universal filtering works for", TEST_YEAR, "Q", TEST_QUARTER, "!\n")
  cat("   Ready to process all 26 quarters (2019-2025)\n\n")
} else {
  cat("❌ FAILED. No processed files created.\n\n")
}

TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")
cat("Processing time:", round(TOTAL_TIME, 1), "minutes\n\n")
