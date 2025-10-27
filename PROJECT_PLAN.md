# OoklaDataR Project Plan

## Objective
Create an R-based automated system to download and process Ookla speed test data (2019-2025) for CIS countries from AWS Open Data, enabling scientific analysis similar to the Internet-Evolution project.

## Scope

### Geographic Coverage
- **11 CIS Countries**: Armenia, Azerbaijan, Belarus, Kazakhstan, Kyrgyzstan, Moldova, Russia, Tajikistan, Uzbekistan, Ukraine, Georgia
- **Excluded**: Turkmenistan (not important per requirements)
- **Special Focus**: Azerbaijan (for detailed mapping visualizations)

### Time Period
- **Coverage**: 2019 Q2 - 2025 Q3 (26 quarters)
- **Granularity**: Monthly data preferred, quarterly aggregations acceptable

### Data Types
- Fixed broadband network performance
- Mobile (cellular) network performance

### Metrics
- Download Speed (Mbps)
- Upload Speed (Mbps)
- Latency (ms)

## Implementation Steps

### 1. Project Structure Setup
**File**: `scripts/00_setup.R`

Create folder structure:
```
OoklaDataR/
├── data/
│   ├── raw/              # Temporary: Downloaded Parquet files (will be deleted)
│   ├── processed/        # Country-filtered data (persistent)
│   ├── aggregated/       # Monthly/quarterly aggregates (persistent)
│   └── boundaries/       # Country shapefiles (persistent)
├── scripts/
│   ├── 00_setup.R        # Install packages, create folders
│   ├── 01_download_data.R         # AWS S3 data download
│   ├── 02_filter_by_country.R     # Spatial filtering
│   ├── 03_aggregate_data.R        # Temporal aggregation
│   ├── 04_azerbaijan_tiles.R      # Extract Azerbaijan tile details
│   └── 05_create_maps.R           # Visualization (future)
├── analysis/             # Analysis scripts (matching Internet-Evolution)
├── reports/              # RMarkdown reports
├── figures/              # Generated visualizations
├── .gitignore            # Exclude large data files
├── README.md             # Project documentation
└── PROJECT_PLAN.md       # This file
```

**R Packages to Install**:
- **Data manipulation**: `tidyverse`, `data.table`, `arrow`
- **Geospatial**: `sf`, `terra`, `lwgeom`, `rnaturalearth`
- **AWS access**: `aws.s3`, `paws` (or use AWS CLI)
- **Time series**: `lubridate`, `zoo`
- **Visualization**: `ggplot2`, `tmap`, `leaflet`, `mapview`
- **Reporting**: `rmarkdown`, `knitr`, `kableExtra`, `flextable`

### 2. Data Download Script
**File**: `scripts/01_download_data.R`

**Functionality**:
- Connect to AWS S3 bucket: `s3://ookla-open-data/parquet/performance/`
- Download quarterly Parquet files for both network types:
  - `type=fixed/year=YYYY/quarter=Q/`
  - `type=mobile/year=YYYY/quarter=Q/`
- Iterate through: 2019 Q2 → 2025 Q3 (26 quarters)
- Use AWS CLI or `aws.s3` package with no credentials (public data)

**Storage Strategy**:
- Download one quarter at a time
- Process immediately
- Delete raw Parquet after processing
- Retain only aggregated results

### 3. Spatial Filtering
**File**: `scripts/02_filter_by_country.R`

**Functionality**:
- Download CIS country boundaries:
  - Source: Natural Earth (`rnaturalearth` package) or GADM
  - Countries: AM, AZ, BY, KZ, KG, MD, RU, TJ, UZ, UA, GE
- Load Ookla tile data (Parquet files with WKT geometries)
- Convert WKT to spatial features
- Perform spatial intersection with country polygons
- Tag each tile with country code(s)
- **For Azerbaijan**: Preserve individual tile geometries and attributes for detailed mapping
- Export filtered data by country

**CIS Country List**:
| Country | ISO Code | Coverage |
|---------|----------|----------|
| Armenia | AM | Full |
| Azerbaijan | AZ | Full + detailed tiles |
| Belarus | BY | Full |
| Kazakhstan | KZ | Full |
| Kyrgyzstan | KG | Full |
| Moldova | MD | Full |
| Russia | RU | Full |
| Tajikistan | TJ | Full |
| Uzbekistan | UZ | Full |
| Ukraine | UA | Full |
| Georgia | GE | Full |

### 4. Data Aggregation Pipeline
**File**: `scripts/03_aggregate_data.R`

**Functionality**:
- Read filtered tile data by country
- Check for date/timestamp fields to determine monthly vs quarterly availability
- Aggregate statistics:
  - **By Country**: Country code
  - **By Network Type**: Fixed vs Mobile
  - **By Time Period**: Monthly (if available) or Quarterly
  - **Metrics**: Median download, median upload, median latency, mean download, mean upload, mean latency, tile count
- Output format matching Internet-Evolution:
  - `Fixed.csv`: Country, Date, Download_Mbps, Upload_Mbps, Latency_ms
  - `Cellular.csv`: Country, Date, Download_Mbps, Upload_Mbps, Latency_ms
- Save to `data/aggregated/`

### 5. Azerbaijan Tile Extraction
**File**: `scripts/04_azerbaijan_tiles.R`

**Functionality**:
- Extract all Azerbaijan tiles with full spatial information
- Preserve zoom level 16 tile geometries (610.8m × 610.8m resolution)
- Include attributes: quadkey, tile_id, lat, lon, speeds, latency
- Save as GeoPackage or Shapefile for mapping
- Organize by time period for temporal mapping
- Output to `data/processed/azerbaijan_tiles/`

**Purpose**: Enable creation of detailed choropleth maps showing:
- City-level speed variations (Baku, Ganja, Sumqayit, Mingachevir, etc.)
- Regional comparisons (Absheron, Ganja-Gazakh, Shaki-Zagatala, etc.)
- Urban vs rural connectivity
- Temporal evolution of network quality

### 6. Visualization & Mapping (Future)
**File**: `scripts/05_create_maps.R`

**Planned Features**:
- Interactive leaflet maps of Azerbaijan
- Choropleth maps by tile (color-coded by speed)
- Time series animations
- Regional aggregation maps
- Comparison maps (2019 vs 2025)
- Export as HTML, PNG, PDF

**Visualization Libraries**:
- `leaflet`: Interactive web maps
- `tmap`: Thematic maps (static and interactive)
- `ggplot2` + `sf`: Static publication-quality maps
- `mapview`: Quick exploration

## Storage Management

### Disk Space Strategy
1. **Download Phase**: ~10-50 GB per quarter (estimate)
2. **Process Phase**: Filter to CIS countries, reduce to ~1-5 GB
3. **Aggregate Phase**: Summarize to ~10-100 MB CSVs
4. **Delete Phase**: Remove raw Parquet files immediately
5. **Retain**:
   - Aggregated CSVs (~100 MB total)
   - Azerbaijan tile data (~500 MB - 2 GB)
   - Country boundaries (~10 MB)

### Processing Workflow
```
For each quarter (2019 Q2 → 2025 Q3):
  1. Download raw Parquet files → data/raw/
  2. Filter by CIS countries → data/processed/
  3. Aggregate statistics → data/aggregated/
  4. Extract Azerbaijan tiles → data/processed/azerbaijan_tiles/
  5. DELETE data/raw/ contents
  6. Continue to next quarter
```

**Total Storage Estimate**: < 5 GB persistent data

## Data Source

### Ookla Open Data
- **Source**: AWS Registry of Open Data
- **Bucket**: `s3://ookla-open-data`
- **Format**: Apache Parquet with WKT geometries (EPSG:4326)
- **Structure**: Tile-based (zoom level 16 web mercator tiles)
- **Resolution**: ~610.8m × 610.8m at equator
- **Update Frequency**: Quarterly
- **License**: Open Data License (free for research, education, policy analysis)

### Data Schema (Parquet)
Expected fields:
- `quadkey`: Tile identifier
- `tile`: Tile coordinates
- `avg_d_kbps`: Average download speed (kbps)
- `avg_u_kbps`: Average upload speed (kbps)
- `avg_lat_ms`: Average latency (ms)
- `tests`: Number of tests in tile
- `devices`: Number of unique devices
- `geometry`: WKT polygon geometry

## Output Deliverables

### 1. Aggregated Time Series Data
- `data/aggregated/Fixed.csv`: Fixed broadband stats by country/month
- `data/aggregated/Cellular.csv`: Mobile network stats by country/month
- Compatible with Internet-Evolution analysis scripts

### 2. Azerbaijan Spatial Data
- `data/processed/azerbaijan_tiles/`: Tile-level data by quarter
- Format: GeoPackage or Shapefile
- Ready for detailed mapping

### 3. Documentation
- `README.md`: Project overview, usage instructions
- `PROJECT_PLAN.md`: This comprehensive plan
- Code comments and function documentation

### 4. Analysis Scripts (Future)
- Replicate Internet-Evolution methodology:
  - `analysis/01_data_prep.R`: Data preparation
  - `analysis/02_descriptive_stats.R`: Descriptive statistics
  - `analysis/03_trend_analysis.R`: Trend analysis
  - `analysis/04_comparative_analysis.R`: Inter-country comparison

### 5. Visualizations (Future)
- Time series plots (all CIS countries)
- Comparative charts (country rankings)
- Azerbaijan detailed maps (city/region level)
- Interactive HTML dashboards

## Technical Requirements

### Software
- **R**: Version 4.0+ (4.3+ recommended)
- **RStudio**: Latest version (optional but recommended)
- **AWS CLI**: Version 2 (optional, for faster downloads)
- **Git**: For version control

### System Requirements
- **RAM**: 8 GB minimum, 16 GB recommended
- **Disk Space**: 100 GB free (for temporary processing)
- **Persistent Storage**: 5 GB for final outputs
- **Internet**: Stable connection for AWS downloads

### R Environment Management
- Use `renv` for package version control
- Document package versions in `renv.lock`
- Ensure reproducibility

## Quality Assurance

### Data Validation
1. Cross-check aggregated results with Speedtest Global Index
2. Verify temporal consistency (no unexpected gaps)
3. Validate spatial coverage (all countries represented)
4. Check for outliers and anomalies

### Code Quality
1. Use consistent R style guide (tidyverse style)
2. Add function documentation (roxygen2 format)
3. Include error handling and logging
4. Write unit tests for critical functions

### Reproducibility
1. Document all dependencies
2. Use relative paths (here package)
3. Include example workflows
4. Provide sample data for testing

## Timeline Estimate

### Phase 1: Setup (Week 1)
- Create folder structure
- Install R packages
- Download country boundaries
- Test AWS S3 access

### Phase 2: Core Development (Weeks 2-3)
- Develop download script
- Implement spatial filtering
- Build aggregation pipeline
- Test on 1-2 quarters

### Phase 3: Full Processing (Week 4)
- Process all 26 quarters (2019-2025)
- Validate outputs
- Extract Azerbaijan tiles

### Phase 4: Analysis & Mapping (Weeks 5-6)
- Replicate Internet-Evolution analysis
- Create Azerbaijan maps
- Generate visualizations
- Write reports

## Future Enhancements

### Short-term
- Automated quarterly updates (scheduled downloads)
- Interactive Shiny dashboard
- API for data access

### Long-term
- Real-time data integration
- Machine learning models (speed prediction)
- Public web portal with maps
- Integration with other data sources (infrastructure, population)

## References

1. **Ookla Open Data**: https://registry.opendata.aws/speedtest-global-performance/
2. **Internet-Evolution Project**: https://github.com/sorujov/Internet-Evolution
3. **Natural Earth Data**: https://www.naturalearthdata.com/
4. **GADM Administrative Boundaries**: https://gadm.org/

## License & Citation

### Data License
- Ookla data is provided under Open Data License
- Free use for research, education, policy analysis

### Attribution
When using this data, cite:
```
Speedtest by Ookla Global Fixed and Mobile Network Performance Maps. 
Based on analysis by Ookla of Speedtest Intelligence® data for Q2 2019 - Q3 2025. 
Provided by Ookla and accessed [Date]. 
Ookla trademarks used under license and reprinted with permission.
```

## Notes

- **Storage-efficient**: Process and delete raw files to avoid disk space issues
- **Scalable**: Pipeline can process new quarters as they become available
- **Flexible**: Monthly or quarterly aggregations depending on data availability
- **Azerbaijan-focused**: Preserves detailed tile data for advanced mapping
- **CIS-comprehensive**: Covers 11 countries (excluding only Turkmenistan)
- **Research-ready**: Output format matches Internet-Evolution for seamless analysis

---

**Project Status**: Planning Complete → Ready for Implementation
**Next Step**: Begin Phase 1 - Setup and infrastructure creation
