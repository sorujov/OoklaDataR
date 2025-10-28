# =============================================================================
# Accurate CIS Quadkey Filtering - Optimized Download
# =============================================================================

library(tidyverse)
library(arrow)
library(sf)
library(here)
library(rnaturalearth)
library(furrr)
library(future)

cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║    Accurate CIS Quadkey Filtering - Download Only What You Need     ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

START_TIME <- Sys.time()

# Setup
n_cores <- max(1, parallel::detectCores() - 1)
plan(multisession, workers = n_cores)

config <- readRDS(here("config.rds"))
dir.create(here("data", "cache"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "processed"), recursive = TRUE, showWarnings = FALSE)
dir.create(here("data", "boundaries"), recursive = TRUE, showWarnings = FALSE)

TEST_YEAR <- 2019
TEST_QUARTER <- 2
TEST_TYPE <- "fixed"

cat("Processing:", TEST_YEAR, "Q", TEST_QUARTER, "-", TEST_TYPE, "\n\n")

# =============================================================================
# Step 1: Load Boundaries
# =============================================================================
cat("1. Loading country boundaries...\n")
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

# =============================================================================
# Step 2: Generate ACCURATE Quadkey Prefixes
# =============================================================================
cat("2. Generating accurate quadkey prefixes for CIS region...\n")

# Helper functions
latlon_to_quadkey <- function(lat, lon, zoom) {
  lat_rad <- lat * pi / 180
  n <- 2^zoom
  x <- floor((lon + 180) / 360 * n)
  y <- floor((1 - log(tan(lat_rad) + 1/cos(lat_rad)) / pi) / 2 * n)
  
  quadkey <- ""
  for (i in zoom:1) {
    digit <- 0
    mask <- bitwShiftL(1, i - 1)
    if (bitwAnd(as.integer(x), as.integer(mask)) != 0) digit <- digit + 1
    if (bitwAnd(as.integer(y), as.integer(mask)) != 0) digit <- digit + 2
    quadkey <- paste0(quadkey, digit)
  }
  return(quadkey)
}

quadkey_to_bbox <- function(quadkey) {
  zoom <- nchar(quadkey)
  x <- 0
  y <- 0
  
  for (i in 1:zoom) {
    mask <- bitwShiftL(1, zoom - i)
    digit <- as.integer(substr(quadkey, i, i))
    
    if (bitwAnd(digit, 1) != 0) x <- x + mask
    if (bitwAnd(digit, 2) != 0) y <- y + mask
  }
  
  n <- 2^zoom
  lon_min <- (x / n) * 360 - 180
  lon_max <- ((x + 1) / n) * 360 - 180
  lat_min <- atan(sinh(pi * (1 - 2 * (y + 1) / n))) * 180 / pi
  lat_max <- atan(sinh(pi * (1 - 2 * y / n))) * 180 / pi
  
  return(c(xmin = lon_min, ymin = lat_min, xmax = lon_max, ymax = lat_max))
}

# Generate dense grid of points within CIS boundaries
cat("   Generating dense point grid across CIS countries...\n")

bbox <- st_bbox(cis_countries)

# Use MUCH higher resolution for accurate coverage
# Zoom level 5 gives ~156km tiles, zoom 6 gives ~78km tiles
ZOOM_LEVEL <- 5  # Good balance between precision and number of prefixes

# Create a dense grid
lat_seq <- seq(bbox["ymin"], bbox["ymax"], by = 1)  # Every 1 degree
lon_seq <- seq(bbox["xmin"], bbox["xmax"], by = 1)

grid_points <- expand.grid(lon = lon_seq, lat = lat_seq)
grid_sf <- st_as_sf(grid_points, coords = c("lon", "lat"), crs = 4326)

# Filter points that are actually within CIS countries
cat("   Filtering grid points to CIS boundaries...\n")
points_in_cis <- st_join(grid_sf, cis_countries, join = st_intersects) %>%
  filter(!is.na(country))

# Generate quadkeys for these points
cat("   Generating quadkeys at zoom level", ZOOM_LEVEL, "...\n")
coords <- st_coordinates(points_in_cis)
quadkeys_in_cis <- mapply(latlon_to_quadkey, 
                          coords[, "Y"], 
                          coords[, "X"], 
                          MoreArgs = list(zoom = ZOOM_LEVEL))

# Get unique prefixes at zoom level 4 (broader coverage)
quadkey_prefixes_4 <- unique(substr(quadkeys_in_cis, 1, 4))

# Also keep zoom level 5 for more precision
quadkey_prefixes_5 <- unique(substr(quadkeys_in_cis, 1, 5))

# Use zoom 4 prefixes (good balance)
quadkey_prefixes <- quadkey_prefixes_4

cat("✓ Generated", length(quadkey_prefixes), "accurate quadkey prefixes\n")
cat("   Sample prefixes:", paste(head(quadkey_prefixes, 10), collapse = ", "), "\n\n")

# Verify coverage
cat("   Verifying coverage...\n")
covered_area <- 0
for (qk in head(quadkey_prefixes, 5)) {
  qk_bbox <- quadkey_to_bbox(qk)
  cat("     ", qk, ": lon[", round(qk_bbox["xmin"], 1), ",", 
      round(qk_bbox["xmax"], 1), "] lat[", 
      round(qk_bbox["ymin"], 1), ",", round(qk_bbox["ymax"], 1), "]\n")
}
cat("\n")

# =============================================================================
# Step 3: Download with Accurate Filter
# =============================================================================
cat("3. Downloading CIS tiles with accurate quadkey filter...\n")

quarter_start <- sprintf("%s-%02d-01", TEST_YEAR, ((TEST_QUARTER - 1) * 3) + 1)
s3_uri <- paste0(
  "s3://ookla-open-data/parquet/performance/type=", TEST_TYPE,
  "/year=", TEST_YEAR, "/quarter=", TEST_QUARTER, "/",
  quarter_start, "_performance_", TEST_TYPE, "_tiles.parquet"
)

download_start <- Sys.time()

ds <- open_dataset(s3_uri, format = "parquet")

cat("   Building filter for", length(quadkey_prefixes), "prefixes...\n")

# Create the filter by checking if the first N characters match any prefix
prefix_length <- nchar(quadkey_prefixes[1])

filtered_data <- ds %>%
  mutate(quadkey_prefix = substr(quadkey, 1, prefix_length)) %>%
  filter(quadkey_prefix %in% quadkey_prefixes) %>%
  select(-quadkey_prefix) %>%
  collect()

download_time <- as.numeric(difftime(Sys.time(), download_start, units = "secs"))

cat("✓ Downloaded", format(nrow(filtered_data), big.mark = ","), "tiles in", 
    round(download_time, 1), "seconds\n")
cat("  Size:", round(object.size(filtered_data) / 1024^2, 1), "MB\n\n")

# Quick validation
if (nrow(filtered_data) > 2000000) {
  warning("⚠️  Downloaded more than 2M tiles - filter may still be too broad!")
  cat("   Consider using zoom level 5 prefixes for better precision\n\n")
} else if (nrow(filtered_data) < 50000) {
  warning("⚠️  Downloaded fewer than 50k tiles - filter may be too restrictive!")
  cat("   Consider using zoom level 3 prefixes for broader coverage\n\n")
} else {
  cat("✓ Downloaded tile count looks reasonable for CIS region\n\n")
}

# =============================================================================
# Step 4: Use Existing WKT Geometry (Much Faster!)
# =============================================================================
cat("4. Converting to spatial features using existing WKT geometry...\n")

geometry_start <- Sys.time()

# The data already has 'tile' column with WKT geometry - just convert it!
filtered_sf <- st_as_sf(filtered_data, wkt = "tile", crs = 4326)
filtered_sf <- st_make_valid(filtered_sf)

geometry_time <- as.numeric(difftime(Sys.time(), geometry_start, units = "secs"))

cat("✓ Created", format(nrow(filtered_sf), big.mark = ","), "geometries in", 
    round(geometry_time, 1), "seconds\n\n")

# Clear large object from memory
rm(filtered_data)
gc()

# =============================================================================
# Step 5: Final Spatial Filter
# =============================================================================
cat("5. Refining with precise country boundaries...\n")

filter_start <- Sys.time()

cis_data <- st_join(filtered_sf, cis_countries, join = st_intersects)
cis_data <- cis_data %>% filter(!is.na(country))

filter_time <- as.numeric(difftime(Sys.time(), filter_start, units = "secs"))

cat("✓ Refined to", format(nrow(cis_data), big.mark = ","), 
    "tiles within CIS countries in", round(filter_time, 1), "seconds\n\n")

# =============================================================================
# Step 6: Save by Country (Parallel)
# =============================================================================
cat("6. Saving by country (parallelized)...\n")

country_list <- unique(cis_data$iso_a2)

save_results <- future_map(country_list, function(country_code) {
  country_data <- cis_data %>% filter(iso_a2 == country_code)
  
  if (nrow(country_data) > 0) {
    output_file <- here("data", "processed", 
                        sprintf("%s_Q%d_%s_%s.rds", 
                                TEST_YEAR, TEST_QUARTER, TEST_TYPE, country_code))
    saveRDS(country_data, output_file)
    
    return(list(country = country_code, tiles = nrow(country_data)))
  }
  return(NULL)
}, .options = furrr_options(seed = TRUE))

save_results <- save_results[!sapply(save_results, is.null)]

for (result in save_results) {
  cat("   ✓", result$country, "-", format(result$tiles, big.mark = ","), "tiles\n")
}

plan(sequential)

# =============================================================================
# Summary
# =============================================================================
cat("\n7. Summary statistics:\n\n")

country_stats <- cis_data %>%
  st_drop_geometry() %>%
  group_by(country, iso_a2) %>%
  summarise(
    tiles = n(),
    download_mbps = round(median(avg_d_kbps / 1000, na.rm = TRUE), 1),
    upload_mbps = round(median(avg_u_kbps / 1000, na.rm = TRUE), 1),
    latency_ms = round(median(avg_lat_ms, na.rm = TRUE), 0),
    .groups = "drop"
  ) %>%
  arrange(desc(tiles))

print(country_stats, n = Inf)

TOTAL_TIME <- difftime(Sys.time(), START_TIME, units = "mins")

cat("\n")
cat("╔═══════════════════════════════════════════════════════════════════════╗\n")
cat("║                     Processing Complete!                             ║\n")
cat("╚═══════════════════════════════════════════════════════════════════════╝\n\n")

cat("⏱️  Total time:", round(TOTAL_TIME, 2), "minutes\n")
cat("📥 Initial download:", format(nrow(filtered_data), big.mark = ","), "tiles\n")
cat("✅ Final CIS tiles:", format(nrow(cis_data), big.mark = ","), "tiles\n")
cat("🌍 Countries:", length(save_results), "\n")
cat("📊 Efficiency:", round(nrow(cis_data) / nrow(filtered_data) * 100, 1), "% of downloaded data used\n\n")
