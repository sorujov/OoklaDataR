# OoklaDataR - Quick Start Guide

## ✅ Setup Complete!

All scripts are ready. Here's how to use them:

## 📋 Available Scripts

| Script | Purpose | Runtime |
|--------|---------|---------|
| `00_setup.R` | ✅ Install packages (DONE) | 2-5 min |
| `01_download_data.R` | Download & filter data quarter by quarter | 2-4 hours |
| `02_filter_by_country.R` | Filter tiles by CIS countries (auto-called) | Included |
| `03_aggregate_data.R` | Create time series CSVs (auto-called) | Included |
| `04_azerbaijan_tiles.R` | Extract Azerbaijan mapping data (auto-called) | Included |
| `run_pipeline.R` | ⭐ **RUN THIS** - Complete automated pipeline | 2-4 hours |
| `test_download.R` | ✅ Test download (PASSED) | 1 min |

## 🚀 How to Start

### Option 1: Run Complete Pipeline (Recommended)
```r
source("scripts/run_pipeline.R")
```
This will:
- Download all 26 quarters (2019-2025)
- Filter by CIS countries automatically
- Create aggregated time series
- Extract Azerbaijan tiles
- Clean up raw files to save space

### Option 2: Run Step by Step
```r
# Download data
source("scripts/01_download_data.R")

# Aggregate (if needed separately)
source("scripts/03_aggregate_data.R")

# Azerbaijan tiles (if needed separately)
source("scripts/04_azerbaijan_tiles.R")
```

## 📊 Output Files

After completion, you'll have:

```
data/
├── aggregated/
│   ├── Fixed.csv                      # Fixed broadband time series
│   ├── Cellular.csv                   # Mobile network time series
│   ├── All_Statistics_Detailed.csv    # Complete statistics
│   └── Azerbaijan_TimeSeries.csv      # Azerbaijan summary
│
├── processed/
│   └── azerbaijan_tiles/              # Detailed Azerbaijan GeoPackages
│       ├── AZ_20190401_fixed_tiles.gpkg
│       ├── AZ_20190401_mobile_tiles.gpkg
│       └── ...
│
└── boundaries/
    └── cis_countries.rds              # Country boundaries
```

## 🗺️ Countries Covered

✅ **11 CIS Countries**:
- 🇦🇲 Armenia (AM)
- 🇦🇿 Azerbaijan (AZ) + detailed tiles
- 🇧🇾 Belarus (BY)
- 🇰🇿 Kazakhstan (KZ)
- 🇰🇬 Kyrgyzstan (KG)
- 🇲🇩 Moldova (MD)
- 🇷🇺 Russia (RU)
- 🇹🇯 Tajikistan (TJ)
- 🇺🇿 Uzbekistan (UZ)
- 🇺🇦 Ukraine (UA)
- 🇬🇪 Georgia (GE)

## 💾 Storage Requirements

- **Temporary**: ~10-50 GB (auto-deleted)
- **Final**: < 5 GB persistent data
- **Make sure you have**: 100 GB free space for processing

## ⏱️ Expected Runtime

- **Fast internet (100+ Mbps)**: 2-3 hours
- **Medium internet (25-100 Mbps)**: 3-4 hours
- **Slow internet (<25 Mbps)**: 4-6 hours

## 🔍 What Happens During Processing

For each quarter (26 total):
1. 📥 Download Parquet files from AWS S3 (~200-500 MB)
2. 🗺️ Filter tiles by CIS country boundaries
3. 📊 Calculate aggregate statistics
4. 🇦🇿 Extract Azerbaijan tiles for mapping
5. 🗑️ Delete raw files to save space
6. ➡️ Move to next quarter

## 📈 Using the Data

### Time Series Analysis (R)
```r
# Load aggregated data
fixed <- read.csv("data/aggregated/Fixed.csv")
cellular <- read.csv("data/aggregated/Cellular.csv")

# Filter for Azerbaijan
az_fixed <- fixed %>% filter(Country == "AZ")

# Plot speed over time
library(ggplot2)
ggplot(az_fixed, aes(x = Date, y = Download_Median_Mbps)) +
  geom_line() +
  labs(title = "Azerbaijan Fixed Broadband Speed Evolution")
```

### Mapping (R)
```r
library(sf)
library(tmap)

# Load Azerbaijan tiles
az_tiles <- st_read("data/processed/azerbaijan_tiles/AZ_20240701_mobile_tiles.gpkg")

# Create choropleth map
tm_shape(az_tiles) +
  tm_polygons("download_mbps", 
              style = "jenks",
              palette = "YlOrRd",
              title = "Download Speed (Mbps)")
```

## 🆘 Troubleshooting

### Problem: "Package not found"
```r
source("scripts/00_setup.R")  # Re-run setup
```

### Problem: "No space left on device"
- Free up at least 100 GB
- Or process fewer quarters at a time (modify config.rds)

### Problem: "Download failed"
- Check internet connection
- Re-run the script (it skips already downloaded files)

### Problem: "AWS CLI not found"
- Not needed! Scripts work without AWS CLI
- Uses R's `httr` and `arrow` packages instead

## 📝 Notes

- ✅ No AWS account needed (public data)
- ✅ No AWS CLI needed (pure R solution)
- ✅ No admin rights needed (R packages only)
- ✅ Pause-resume friendly (skips downloaded files)
- ✅ Storage-efficient (auto-cleanup)

## 🎯 Ready to Start?

Run this command in R:
```r
source("scripts/run_pipeline.R")
```

Or open RStudio and run:
```
File → Open File → scripts/run_pipeline.R → Source
```

Good luck with your research! 🚀
