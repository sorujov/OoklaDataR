# =============================================================================
# OoklaDataR - Test Download Script
# =============================================================================
# Purpose: Test download with one quarter (2024 Q3)
# =============================================================================

library(tidyverse)
library(arrow)
library(here)
library(httr)

cat("=== Testing Download for 2024 Q3 ===\n\n")

# Load configuration
config <- readRDS(here("config.rds"))

# S3 bucket URL
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

# Test with 2024 Q3
test_year <- 2024
test_quarter <- 3
test_type <- "mobile"  # Start with mobile, it's usually smaller

cat("Testing with:", test_year, "Q", test_quarter, "-", toupper(test_type), "\n\n")

# Construct S3 prefix
prefix <- paste0(
  config$s3_prefix,
  "type=", test_type,
  "/year=", test_year,
  "/quarter=", test_quarter,
  "/"
)

cat("S3 path:", prefix, "\n\n")

# List files
cat("Step 1: Listing files in S3...\n")
list_url <- paste0(S3_BASE, "/?list-type=2&prefix=", URLencode(prefix, reserved = TRUE))

cat("Request URL:", list_url, "\n")

response <- GET(list_url)
cat("Response status:", status_code(response), "\n")

if (status_code(response) == 200) {
  content_text <- content(response, "text", encoding = "UTF-8")
  
  # Extract file keys
  keys <- str_extract_all(content_text, "<Key>([^<]+\\.parquet)</Key>")[[1]]
  keys <- str_replace_all(keys, "</?Key>", "")
  
  cat("\n✓ Found", length(keys), "Parquet file(s)\n\n")
  
  if (length(keys) > 0) {
    cat("Files found:\n")
    for (k in keys) {
      cat("  -", basename(k), "\n")
    }
    
    # Test download of first file
    cat("\n\nStep 2: Testing download of first file...\n")
    test_key <- keys[1]
    dest_file <- here("data", "raw", basename(test_key))
    
    dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)
    
    url <- paste0(S3_BASE, "/", test_key)
    cat("Downloading from:", url, "\n")
    cat("Saving to:", dest_file, "\n\n")
    
    download_response <- GET(
      url,
      write_disk(dest_file, overwrite = TRUE),
      progress()
    )
    
    if (status_code(download_response) == 200 && file.exists(dest_file)) {
      file_size <- file.info(dest_file)$size / (1024^2)
      cat("\n✓ Download successful!\n")
      cat("File size:", round(file_size, 2), "MB\n\n")
      
      # Test reading the Parquet file
      cat("Step 3: Testing Parquet file reading...\n")
      tryCatch({
        df <- read_parquet(dest_file)
        cat("✓ Successfully read Parquet file\n")
        cat("Rows:", nrow(df), "\n")
        cat("Columns:", ncol(df), "\n")
        cat("\nColumn names:\n")
        print(names(df))
        
        cat("\n\nFirst few rows:\n")
        print(head(df, 3))
        
        cat("\n\n✓✓✓ ALL TESTS PASSED! ✓✓✓\n")
        cat("\nYou can now run the full download:\n")
        cat("  source('scripts/01_download_data.R')\n")
        
      }, error = function(e) {
        cat("✗ Error reading Parquet file:", e$message, "\n")
      })
      
    } else {
      cat("✗ Download failed\n")
      cat("Status:", status_code(download_response), "\n")
    }
    
  } else {
    cat("✗ No Parquet files found\n")
  }
  
} else {
  cat("✗ Failed to list files\n")
  cat("Status code:", status_code(response), "\n")
  cat("\nResponse content:\n")
  print(content(response, "text"))
}
