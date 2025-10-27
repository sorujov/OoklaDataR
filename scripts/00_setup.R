# =============================================================================
# OoklaDataR - Setup Script
# =============================================================================
# Purpose: Install required R packages and verify environment
# Author: OoklaDataR Project
# Date: 2025-10-27
# =============================================================================

cat("=== OoklaDataR Setup ===\n")
cat("Installing required R packages...\n\n")

# Check R version
if (getRversion() < "4.0.0") {
  stop("R version 4.0.0 or higher is required. Current version: ", getRversion())
}

cat("R version:", as.character(getRversion()), "✓\n\n")

# Function to install packages if not already installed
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    cat("Installing:", pkg, "\n")
    install.packages(pkg, dependencies = TRUE, repos = "https://cloud.r-project.org/")
  } else {
    cat("Already installed:", pkg, "✓\n")
  }
}

# Core packages
cat("\n--- Core Data Manipulation Packages ---\n")
core_packages <- c(
  "tidyverse",     # Data manipulation and visualization
  "data.table",    # Fast data processing
  "arrow",         # Read Parquet files
  "lubridate",     # Date-time handling
  "here"           # Relative paths
)

for (pkg in core_packages) {
  install_if_missing(pkg)
}

# Geospatial packages
cat("\n--- Geospatial Packages ---\n")
spatial_packages <- c(
  "sf",            # Simple Features for R
  "terra",         # Raster and vector spatial data
  "lwgeom",        # Lightweight geometry operations
  "rnaturalearth", # Natural Earth map data
  "rnaturalearthdata" # Natural Earth data
)

for (pkg in spatial_packages) {
  install_if_missing(pkg)
}

# Visualization packages
cat("\n--- Visualization Packages ---\n")
viz_packages <- c(
  "ggplot2",       # Grammar of graphics (part of tidyverse but listed for clarity)
  "tmap",          # Thematic maps
  "leaflet",       # Interactive maps
  "mapview",       # Quick interactive viewing
  "scales",        # Scale functions for visualization
  "viridis"        # Color scales
)

for (pkg in viz_packages) {
  install_if_missing(pkg)
}

# AWS and web packages
cat("\n--- AWS and Web Access Packages ---\n")
aws_packages <- c(
  "httr",          # HTTP requests
  "jsonlite",      # JSON parsing
  "curl"           # URL handling
)

for (pkg in aws_packages) {
  install_if_missing(pkg)
}

# Reporting packages
cat("\n--- Reporting Packages ---\n")
report_packages <- c(
  "rmarkdown",     # Dynamic documents
  "knitr",         # Dynamic report generation
  "kableExtra",    # Enhanced tables
  "flextable"      # Flexible tables
)

for (pkg in report_packages) {
  install_if_missing(pkg)
}

# Time series packages
cat("\n--- Time Series Packages ---\n")
ts_packages <- c(
  "zoo",           # Time series objects
  "forecast"       # Time series forecasting
)

for (pkg in ts_packages) {
  install_if_missing(pkg)
}

# Verify installations
cat("\n\n=== Verifying Installations ===\n")
all_packages <- c(core_packages, spatial_packages, viz_packages, 
                  aws_packages, report_packages, ts_packages)

failed_packages <- c()
for (pkg in all_packages) {
  if (!requireNamespace(pkg, quietly = TRUE)) {
    failed_packages <- c(failed_packages, pkg)
    cat("✗ Failed:", pkg, "\n")
  }
}

if (length(failed_packages) == 0) {
  cat("\n✓ All packages installed successfully!\n")
} else {
  cat("\n✗ Failed to install:", paste(failed_packages, collapse = ", "), "\n")
  cat("Please install these packages manually.\n")
}

# Create config file for project
cat("\n=== Creating Configuration ===\n")

config <- list(
  project_name = "OoklaDataR",
  countries = c("AM", "AZ", "BY", "KZ", "KG", "MD", "RU", "TJ", "UZ", "UA", "GE"),
  country_names = c(
    "AM" = "Armenia",
    "AZ" = "Azerbaijan", 
    "BY" = "Belarus",
    "KZ" = "Kazakhstan",
    "KG" = "Kyrgyzstan",
    "MD" = "Moldova",
    "RU" = "Russia",
    "TJ" = "Tajikistan",
    "UZ" = "Uzbekistan",
    "UA" = "Ukraine",
    "GE" = "Georgia"
  ),
  start_year = 2019,
  start_quarter = 2,
  end_year = 2025,
  end_quarter = 3,
  data_types = c("fixed", "mobile"),
  s3_bucket = "ookla-open-data",
  s3_prefix = "parquet/performance/"
)

# Save config
config_file <- here::here("config.rds")
saveRDS(config, config_file)
cat("Configuration saved to:", config_file, "\n")

# Print system info
cat("\n=== System Information ===\n")
cat("R version:", as.character(getRversion()), "\n")
cat("Platform:", R.version$platform, "\n")
cat("Working directory:", getwd(), "\n")

# Check AWS CLI (optional but helpful)
cat("\n=== Checking AWS CLI (optional) ===\n")
aws_check <- tryCatch({
  system2("aws", "--version", stdout = TRUE, stderr = TRUE)
}, error = function(e) {
  return(NULL)
})

if (is.null(aws_check)) {
  cat("AWS CLI not found. You can install it from: https://aws.amazon.com/cli/\n")
  cat("(Not required, but helpful for faster downloads)\n")
} else {
  cat("AWS CLI installed:", aws_check[1], "✓\n")
}

cat("\n=== Setup Complete! ===\n")
cat("Next steps:\n")
cat("1. Run scripts/01_download_data.R to download Ookla data\n")
cat("2. Run scripts/02_filter_by_country.R to filter by CIS countries\n")
cat("3. Run scripts/03_aggregate_data.R to create time series\n")
