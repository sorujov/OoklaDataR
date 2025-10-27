# =============================================================================
# OoklaDataR - Data Download Script
# =============================================================================
# Purpose: Download Ookla speed test data from AWS S3 (quarter by quarter)
# Author: OoklaDataR Project
# Date: 2025-10-27
# Note: Uses R packages only - no AWS CLI needed!
# =============================================================================

library(tidyverse)
library(arrow)
library(here)
library(httr)

cat("=== OoklaDataR - Data Download ===\n\n")

# Load configuration
config <- readRDS(here("config.rds"))

# S3 bucket URL (public, no credentials needed)
S3_BASE <- "https://ookla-open-data.s3.amazonaws.com"

# Create directories
dir.create(here("data", "raw"), recursive = TRUE, showWarnings = FALSE)

# Function to generate all quarters between start and end
generate_quarters <- function(start_year, start_q, end_year, end_q) {
  quarters <- list()
  
  for (year in start_year:end_year) {
    start_quarter <- ifelse(year == start_year, start_q, 1)
    end_quarter <- ifelse(year == end_year, end_q, 4)
    
    for (quarter in start_quarter:end_quarter) {
      quarters[[length(quarters) + 1]] <- list(year = year, quarter = quarter)
    }
  }
  
  return(quarters)
}

# Generate list of all quarters to download
quarters <- generate_quarters(
  config$start_year, 
  config$start_quarter,
  config$end_year, 
  config$end_quarter
)

cat("Will process", length(quarters), "quarters from", 
    paste0(config$start_year, "-Q", config$start_quarter), "to",
    paste0(config$end_year, "-Q", config$end_quarter), "\n\n")

# Function to list files in S3 bucket
list_s3_files <- function(bucket_url, prefix) {
  cat("Listing files in:", prefix, "\n")
  
  # Construct the S3 list URL
  list_url <- paste0(bucket_url, "/?list-type=2&prefix=", URLencode(prefix, reserved = TRUE))
  
  tryCatch({
    response <- GET(list_url)
    
    if (status_code(response) != 200) {
      warning("Failed to list files. Status:", status_code(response))
      return(NULL)
    }
    
    # Parse XML response
    content_text <- content(response, "text", encoding = "UTF-8")
    
    # Extract file keys using regex (simple XML parsing)
    keys <- str_extract_all(content_text, "<Key>([^<]+\\.parquet)</Key>")[[1]]
    keys <- str_replace_all(keys, "</?Key>", "")
    
    if (length(keys) == 0) {
      cat("  No files found\n")
      return(NULL)
    }
    
    cat("  Found", length(keys), "files\n")
    return(keys)
    
  }, error = function(e) {
    warning("Error listing files:", e$message)
    return(NULL)
  })
}

# Function to download a single file
download_s3_file <- function(bucket_url, key, dest_file) {
  url <- paste0(bucket_url, "/", key)
  
  tryCatch({
    # Check if file already exists
    if (file.exists(dest_file)) {
      cat("    Already downloaded, skipping\n")
      return(TRUE)
    }
    
    # Download with progress
    cat("    Downloading:", basename(dest_file), "\n")
    response <- GET(
      url,
      write_disk(dest_file, overwrite = TRUE),
      progress()
    )
    
    if (status_code(response) == 200) {
      file_size <- file.info(dest_file)$size / (1024^2)  # MB
      cat("    ✓ Downloaded:", round(file_size, 1), "MB\n")
      return(TRUE)
    } else {
      warning("Failed to download. Status:", status_code(response))
      return(FALSE)
    }
    
  }, error = function(e) {
    warning("Error downloading file:", e$message)
    return(FALSE)
  })
}

# Main processing loop
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Starting download and processing...\n")
cat("=" , rep("=", 60), "=\n\n", sep = "")

total_downloaded <- 0
total_processed <- 0

for (i in seq_along(quarters)) {
  q <- quarters[[i]]
  year <- q$year
  quarter <- q$quarter
  
  cat("\n")
  cat("=" , rep("=", 60), "=\n", sep = "")
  cat("Processing Quarter", i, "of", length(quarters), ":", year, "Q", quarter, "\n")
  cat("=" , rep("=", 60), "=\n", sep = "")
  
  # Process both fixed and mobile
  for (data_type in config$data_types) {
    cat("\n--- Network Type:", toupper(data_type), "---\n")
    
    # Construct S3 prefix
    prefix <- paste0(
      config$s3_prefix,
      "type=", data_type,
      "/year=", year,
      "/quarter=", quarter,
      "/"
    )
    
    # List files in S3
    file_keys <- list_s3_files(S3_BASE, prefix)
    
    if (is.null(file_keys) || length(file_keys) == 0) {
      cat("  Skipping - no data available\n")
      next
    }
    
    # Download files
    for (key in file_keys) {
      dest_file <- here("data", "raw", basename(key))
      success <- download_s3_file(S3_BASE, key, dest_file)
      
      if (success) {
        total_downloaded <- total_downloaded + 1
      }
    }
  }
  
  cat("\n✓ Quarter", year, "Q", quarter, "download complete\n")
  
  # Call filtering script if it exists
  if (file.exists(here("scripts", "02_filter_by_country.R"))) {
    cat("\nFiltering by CIS countries...\n")
    tryCatch({
      source(here("scripts", "02_filter_by_country.R"), local = TRUE)
      total_processed <- total_processed + 1
    }, error = function(e) {
      warning("Error in filtering script:", e$message)
    })
  }
  
  # Clean up raw files to save space
  cat("\nCleaning up raw files to save disk space...\n")
  raw_files <- list.files(here("data", "raw"), pattern = "\\.parquet$", full.names = TRUE)
  if (length(raw_files) > 0) {
    file.remove(raw_files)
    cat("✓ Removed", length(raw_files), "raw Parquet files\n")
  }
  
  # Small pause to be nice to the server
  Sys.sleep(2)
}

cat("\n")
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Download Summary\n")
cat("=" , rep("=", 60), "=\n", sep = "")
cat("Total files downloaded:", total_downloaded, "\n")
cat("Total quarters processed:", total_processed, "\n")
cat("\nNext steps:\n")
cat("1. Check data/processed/ for filtered data\n")
cat("2. Run scripts/03_aggregate_data.R to create time series\n")
cat("3. Run scripts/04_azerbaijan_tiles.R for detailed Azerbaijan data\n")
