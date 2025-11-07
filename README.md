# OoklaDataR

**Automated Ookla Speed Test Data Downloader for CIS Countries**

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![R Version](https://img.shields.io/badge/R-%3E%3D4.0.0-blue.svg)](https://www.r-project.org/)

## Overview

OoklaDataR is an R-based automated system for downloading and processing Ookla Speedtest Open Data for Commonwealth of Independent States (CIS) countries. This project enables scientific analysis of internet quality evolution from 2019 to 2025.

### Key Features

- ✅ **11 CIS Countries Coverage**: Armenia, Azerbaijan, Belarus, Kazakhstan, Kyrgyzstan, Moldova, Russia, Tajikistan, Uzbekistan, Ukraine, Georgia
- ✅ **Storage-Efficient**: Process and delete raw files automatically (< 5 GB final storage)
- ✅ **Dual Network Types**: Fixed broadband and mobile cellular data
- ✅ **Time Series Ready**: Monthly/quarterly aggregated data compatible with analysis tools
- ✅ **Azerbaijan Focus**: Detailed tile-level data for advanced mapping and visualization
- ✅ **Open Data**: Uses Ookla's public AWS Open Data registry

## Quick Start

### Prerequisites

- **R**: Version 4.0 or higher
- **RStudio**: Recommended for development
- **Disk Space**: 100 GB free for temporary processing
- **Internet**: Stable connection for AWS downloads

### Installation

1. **Clone the repository**:
```bash
git clone https://github.com/sorujov/OoklaDataR.git
cd OoklaDataR
```

2. **Run the setup script** (installs all required packages):
```r
source("scripts/00_setup.R")
```

This will install:
- Data manipulation: `tidyverse`, `data.table`, `arrow`, `lubridate`
- Geospatial: `sf`, `terra`, `lwgeom`, `rnaturalearth`
- Visualization: `ggplot2`, `tmap`, `leaflet`, `mapview`
- Reporting: `rmarkdown`, `knitr`, `kableExtra`

### Basic Usage

**Run the automated pipeline**
```r
# This will download, filter, and aggregate all data automatically
source("scripts/run_pipeline_auto.R")
```

**Or run steps individually**
```r
# Step 1: Download Ookla data from AWS S3 (processes quarter by quarter)
source("scripts/01_download_data.R")

# Step 2: Aggregate data (creates mean & median statistics)
source("scripts/03_aggregate_data.R")
```

**Access the processed data**
```r
# Load the main aggregated dataset (all countries, all quarters)
data <- read.csv("data/aggregated/CIS_Internet_Speed_2019-2025.csv")

# Or load individual country files
armenia_data <- read.csv("data/aggregated/AM_all_quarters.csv")

# View structure
head(data)
```

## Project Structure

```
OoklaDataR/
├── data/
│   ├── processed/        # Filtered country data by quarter (RDS files)
│   ├── aggregated/       # Aggregated CSV files (YOUR MAIN OUTPUT)
│   │   ├── CIS_Internet_Speed_2019-2025.csv  # Combined dataset
│   │   ├── AM_all_quarters.csv               # Armenia
│   │   ├── AZ_all_quarters.csv               # Azerbaijan
│   │   ├── BY_all_quarters.csv               # Belarus
│   │   ├── KZ_all_quarters.csv               # Kazakhstan
│   │   ├── KG_all_quarters.csv               # Kyrgyzstan
│   │   ├── MD_all_quarters.csv               # Moldova
│   │   ├── RU_all_quarters.csv               # Russia
│   │   ├── TJ_all_quarters.csv               # Tajikistan
│   │   ├── UZ_all_quarters.csv               # Uzbekistan
│   │   ├── UA_all_quarters.csv               # Ukraine
│   │   ├── GE_all_quarters.csv               # Georgia
│   │   └── summary_statistics.csv            # Summary stats
│   └── boundaries/       # Country shapefiles
├── scripts/
│   ├── 00_setup.R                 # Setup and package installation
│   ├── 01_download_data.R         # AWS S3 data download & filtering
│   ├── 02_filter_by_country.R     # Spatial filtering (called by 01)
│   ├── 03_aggregate_data.R        # Calculate mean & median statistics
│   ├── 04_azerbaijan_tiles.R      # Azerbaijan tile extraction
│   ├── run_pipeline_auto.R        # Automated pipeline (non-interactive)
│   └── recalculate_with_medians.R # Recalculate with median values
├── config.rds           # Project configuration
├── QUICKSTART.md        # Quick start guide
├── cleanup_project.R    # Cleanup script (already run)
└── README.md            # This file
```

## Data Source

**Ookla Speedtest Open Data**
- **Source**: AWS Registry of Open Data
- **Bucket**: `s3://ookla-open-data`
- **Format**: Apache Parquet with WKT geometries
- **Coverage**: Global, updated quarterly
- **Resolution**: Zoom level 16 tiles (~610.8m × 610.8m at equator)
- **License**: Open Data License (free for research and education)

## Output Data

### Main Aggregated Dataset
**File**: `data/aggregated/CIS_Internet_Speed_2019-2025.csv`

This is your primary output containing all 11 CIS countries from 2019 Q2 to 2025 Q3.

**Columns**:
- `download_mbps`, `upload_mbps`, `latency_ms` - **Mean values**
- `download_mbps_median`, `upload_mbps_median`, `latency_ms_median` - **Median values**
- `tile_count` - Number of geographic tiles
- `total_tests` - Total number of speed tests
- `total_devices` - Total number of unique devices
- `country_code` - ISO 2-letter country code (AM, AZ, BY, etc.)
- `country_name` - Full country name
- `year`, `quarter` - Time period
- `network_type` - "fixed" or "mobile"
- `date` - First day of quarter (YYYY-MM-DD)

**Example**:
```
download_mbps,upload_mbps,latency_ms,download_mbps_median,upload_mbps_median,latency_ms_median,
tile_count,total_tests,total_devices,country_code,country_name,year,quarter,network_type,date
15.99,14.24,22.7,14.07,11.46,12,3237,55424,18180,AM,Armenia,2019,2,fixed,2019-06-01
23.55,10.2,29,16.38,7.78,21,2479,13617,8114,AM,Armenia,2019,2,mobile,2019-06-01
```

### Individual Country Files
Each country has its own CSV file:
- `data/aggregated/AM_all_quarters.csv` - Armenia
- `data/aggregated/AZ_all_quarters.csv` - Azerbaijan
- `data/aggregated/BY_all_quarters.csv` - Belarus
- ... (one file per country)

Same format as the main file, but filtered to a single country.

### Summary Statistics
**File**: `data/aggregated/summary_statistics.csv`

Contains overall statistics across all quarters for each country and network type.

## CIS Countries Covered

| Country | ISO Code | Status |
|---------|----------|--------|
| Armenia | AM | ✓ |
| Azerbaijan | AZ | ✓ (+ detailed tiles) |
| Belarus | BY | ✓ |
| Kazakhstan | KZ | ✓ |
| Kyrgyzstan | KG | ✓ |
| Moldova | MD | ✓ |
| Russia | RU | ✓ |
| Tajikistan | TJ | ✓ |
| Uzbekistan | UZ | ✓ |
| Ukraine | UA | ✓ |
| Georgia | GE | ✓ |

## Storage Management

The project uses a **process-and-delete strategy** to minimize disk usage:

1. Download one quarter (~200-500 MB compressed)
2. Filter by CIS countries using spatial boundaries
3. Calculate mean and median statistics
4. Save processed data (RDS files)
5. **Delete raw Parquet files**
6. Repeat for next quarter

**Current storage**:
- Processed data (RDS): ~500 MB (all quarters, all countries, both network types)
- Aggregated CSVs: ~2-3 MB (your main output files)
- Total: < 1 GB

**Note**: Raw downloads are automatically deleted after processing to save space. The processed RDS files in `data/processed/` can be regenerated if needed by re-running the download pipeline.

## Citation

When using this data, please cite:

```
Speedtest by Ookla Global Fixed and Mobile Network Performance Maps.
Based on analysis by Ookla of Speedtest Intelligence® data for Q2 2019 - Q3 2025.
Provided by Ookla and accessed [Date].
Ookla trademarks used under license and reprinted with permission.
```

## Related Projects

- **Internet-Evolution**: Original analysis of internet quality in Azerbaijan (2019-2025)  
  https://github.com/sorujov/Internet-Evolution

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Contact

- **Author**: Samir Orujov
- **GitHub**: [@sorujov](https://github.com/sorujov)
- **Repository**: [OoklaDataR](https://github.com/sorujov/OoklaDataR)

## Acknowledgments

- **Ookla** for providing open access to Speedtest data
- **AWS Open Data Program** for hosting the data
- **Internet-Evolution Project** for the methodological foundation

---

**Status**: 🚧 Active Development | **Version**: 0.1.0 | **Last Updated**: October 2025