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

**Step 1: Download and process data**
```r
# Download Ookla data from AWS S3 (processes quarter by quarter)
source("scripts/01_download_data.R")

# Filter by CIS countries
source("scripts/02_filter_by_country.R")

# Generate aggregated time series
source("scripts/03_aggregate_data.R")

# Extract Azerbaijan tiles for mapping
source("scripts/04_azerbaijan_tiles.R")
```

**Step 2: Access processed data**
```r
# Load aggregated data
fixed_data <- read.csv("data/aggregated/Fixed.csv")
mobile_data <- read.csv("data/aggregated/Cellular.csv")

# View structure
head(fixed_data)
```

## Project Structure

```
OoklaDataR/
├── data/
│   ├── raw/              # Temporary downloads (auto-deleted)
│   ├── processed/        # Filtered country data
│   ├── aggregated/       # Time series CSVs
│   └── boundaries/       # Country shapefiles
├── scripts/
│   ├── 00_setup.R                 # Setup and package installation
│   ├── 01_download_data.R         # AWS S3 data download
│   ├── 02_filter_by_country.R     # Spatial filtering
│   ├── 03_aggregate_data.R        # Temporal aggregation
│   ├── 04_azerbaijan_tiles.R      # Azerbaijan tile extraction
│   └── 05_create_maps.R           # Visualization (future)
├── analysis/             # Analysis scripts
├── reports/              # RMarkdown reports
├── figures/              # Generated visualizations
├── .gitignore           # Git ignore rules
├── config.rds           # Project configuration
├── PROJECT_PLAN.md      # Detailed implementation plan
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

### Aggregated Time Series
- `data/aggregated/Fixed.csv`: Fixed broadband statistics by country/month
- `data/aggregated/Cellular.csv`: Mobile network statistics by country/month

**Format**:
```
Country, Date, Download_Mbps, Upload_Mbps, Latency_ms, Tests_Count
AZ, 2019-04-01, 45.2, 23.1, 18.5, 15234
```

### Azerbaijan Spatial Data
- `data/processed/azerbaijan_tiles/`: Tile-level data by quarter
- Format: GeoPackage with geometries
- Ready for detailed mapping and visualization

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

1. Download one quarter (~10-50 GB)
2. Filter by CIS countries
3. Aggregate statistics
4. Extract Azerbaijan tiles
5. **Delete raw files**
6. Repeat for next quarter

**Final storage**: < 5 GB persistent data

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