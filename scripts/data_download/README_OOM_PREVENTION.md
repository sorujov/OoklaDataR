# Data Download Scripts - OOM Prevention & Parallel Processing Guide

## Quick Start

### 1. Check Your System Capabilities
```r
Rscript scripts/data_download/configure_cores.R
```
This will analyze your system and recommend the optimal number of cores.

### 2. Configure Parallelization (Optional)
Edit `batch_process_all.R` line ~81:
```r
n_cores <- 1  # Change to 2, 3, or 4 based on your RAM
```

### 3. Run Processing
```r
# Standard batch (configurable parallelization)
Rscript scripts/data_download/batch_process_all.R

# Ultra-safe sequential (if you have OOM issues)
Rscript scripts/data_download/ultra_memory_safe_batch.R
```

---

## Problem
Out of Memory (OOM) errors occur when processing large Ookla datasets because:
1. **Large Parquet files**: Some quarters contain millions of tiles
2. **Spatial operations**: Converting to spatial features and intersecting with boundaries is memory-intensive
3. **Parallel processing**: Running multiple tasks simultaneously multiplies memory usage

## Solutions (from fastest to most reliable)

### 1. Standard Batch Processing (RECOMMENDED - Now with Parallelization!)
**File**: `batch_process_all.R`

**What it does**:
- **Configurable parallelization**: Set n_cores from 1 to 6+ based on your RAM
- Chunked processing for large datasets (>100k rows)
- **Automatic checkpoint/resume**: Always skips completed tasks
- Memory cleanup after each task
- Works in both sequential (n_cores=1) and parallel (n_cores>1) modes

**When to use**: Primary choice for most users

**How to configure**:
1. Check your system: `Rscript scripts/data_download/configure_cores.R`
2. Edit `batch_process_all.R` and set `n_cores` (line ~81)
3. Run: `Rscript scripts/data_download/batch_process_all.R`

**Parallelization Guide**:
| n_cores | RAM Needed | Speed | Time (est.) |
|---------|------------|-------|-------------|
| 1 | 8-16 GB | Slowest | 8-10 hours |
| 2 | 16-24 GB | Balanced | 4-6 hours |
| 3 | 24-32 GB | Fast | 2-4 hours |
| 4+ | 32+ GB | Fastest | 1-3 hours |

**Resume Capability**: ✅ YES - Works perfectly in both sequential and parallel modes

---

### 2. Ultra Memory-Safe Batch
**File**: `ultra_memory_safe_batch.R`

**What it does**:
- Processes ONE task at a time (sequential only, n_cores forced to 1)
- Aggressive garbage collection after each task
- Memory monitoring with automatic pausing if usage is too high
- Chunked processing for large datasets (>100k rows)
- Resume capability (skips completed tasks)

**When to use**: Only if you're getting OOM errors with `batch_process_all.R` at n_cores=1

**How to run**:
```r
Rscript scripts/data_download/ultra_memory_safe_batch.R
```

**Speed**: Slowest (~8-10 hours for all countries)
**Reliability**: Highest (works even on 8GB RAM systems)
**Resume Capability**: ✅ YES

---

### 3. Single Task Testing
### 3. Single Task Testing
**File**: `memory_safe_single_task.R`

**What it does**:
- Processes a single country/quarter/network combination
- Shows memory usage before and after
- Useful for debugging specific tasks that fail

**When to use**: To test if a specific task is causing OOM issues

**How to run**:
1. Edit the configuration section in the file:
```r
COUNTRY_CODE <- "AM"      # Change to your country code
YEAR <- 2024              # Change to your year
QUARTER <- 2              # Change to your quarter
NETWORK_TYPE <- "mobile"  # "fixed" or "mobile"
```

2. Run:
```r
Rscript scripts/data_download/memory_safe_single_task.R
```

**Resume Capability**: ✅ YES - Checks if task already completed

---

## Checkpoint/Resume System

### How It Works
All scripts check for existing `.rds` files in `data/processed/` before processing:
- ✅ **File exists**: Task skipped automatically
- ⏭️ **File missing**: Task will be processed
- 🔄 **Works in both sequential and parallel modes**

### Example Output
```
Country 1 of 11: Armenia
✓ Checkpoint: Skipping 25 already completed tasks
Processing 27 remaining tasks...
```

### Manual Resume
If you need to reprocess specific tasks:
1. Delete the corresponding `.rds` file(s) from `data/processed/`
2. Run the script again - it will reprocess only the deleted files

### Parallel Processing & Checkpoints
- **Before starting each country**: Checks ALL tasks for that country
- **Skips completed**: Processes only missing tasks
- **Safe interruption**: You can stop (Ctrl+C) and restart anytime
- **No duplicates**: Never reprocesses existing files

---
**File**: `memory_safe_single_task.R`

**What it does**:
- Processes a single country/quarter/network combination
- Shows memory usage before and after
- Useful for debugging specific tasks that fail

**When to use**: To test if a specific task is causing OOM issues

**How to run**:
1. Edit the configuration section in the file:
```r
COUNTRY_CODE <- "AM"      # Change to your country code
YEAR <- 2024              # Change to your year
QUARTER <- 2              # Change to your quarter
NETWORK_TYPE <- "mobile"  # "fixed" or "mobile"
```

2. Run:
```r
Rscript scripts/data_download/memory_safe_single_task.R
```

---

## Key Improvements in `process_ookla_data.R`

### Chunked Data Loading
Instead of loading all data at once with `collect()`, the function now:
1. Checks row count BEFORE collecting
2. If >100k rows, processes in chunks of 50k rows
3. Forces garbage collection after each chunk

### Chunked Spatial Operations
Large spatial conversions and intersections now happen in chunks of 25k rows to prevent memory spikes.

### Memory Cleanup
- `gc(verbose = FALSE)` called after each chunk
- Intermediate objects explicitly removed with `rm()`
- Full garbage collection between tasks

---

## How to Choose

### Step 1: Check Your System
```r
Rscript scripts/data_download/configure_cores.R
```

### Step 2: Select Approach

| RAM Available | Script | n_cores | Expected Time |
|--------------|---------|---------|---------------|
| < 12 GB | `batch_process_all.R` | 1 | 8-10 hours |
| 12-20 GB | `batch_process_all.R` | 2 | 4-6 hours |
| 20-32 GB | `batch_process_all.R` | 3 | 2-4 hours |
| 32+ GB | `batch_process_all.R` | 4-6 | 1-3 hours |
| Any (if OOM) | `ultra_memory_safe_batch.R` | (forced 1) | 8-10 hours |

### Step 3: Configure (if using parallel)
1. Open `batch_process_all.R`
2. Find line ~81: `n_cores <- 1`
3. Change to your desired value
4. Save

### Step 4: Run
```r
Rscript scripts/data_download/batch_process_all.R
```

---

## Adjusting for Your System

### If you have more RAM (16GB+):
In `batch_process_all.R`, change:
```r
n_cores <- 1  # Change to 2 or 3
```

### If you're still getting OOM errors:
In `ultra_memory_safe_batch.R`, adjust the memory threshold:
```r
MEMORY_THRESHOLD_MB <- 4000  # Lower this value (e.g., 3000)
```

In `process_ookla_data.R`, reduce chunk sizes:
```r
chunk_size <- 50000  # Change to 25000 or 10000
```

---

## Progress Monitoring

Check progress at any time:
```r
Rscript scripts/data_download/check_progress.R
```

This shows:
- Completed tasks count
- Progress percentage
- Which countries are complete
- Estimated time remaining

---

## Resume Capability

All scripts automatically skip completed tasks. If the script is interrupted:
1. Simply run it again
2. It will detect existing `.rds` files in `data/processed/`
3. Only missing tasks will be processed

---

## Troubleshooting

### Still getting OOM errors?
1. Close all other applications
2. Use `ultra_memory_safe_batch.R`
3. Further reduce chunk sizes in `process_ookla_data.R`
4. Process one country at a time manually using `memory_safe_single_task.R`

### Tasks failing for specific quarters?
- Some quarters may not have data for certain countries
- Check the error log in `logs/` directory
- These failures are normal and won't stop the batch process

### Want to test a single problematic quarter?
Use `memory_safe_single_task.R` to isolate and debug the issue.

---

## Technical Details

### Streaming vs. Loading
- **Old approach**: `collect()` loaded entire Parquet file into memory
- **New approach**: Filter is applied in Arrow before collecting, and large results are chunked

### Chunking Strategy
- Data loading: 50k row chunks
- Spatial conversion: 25k row chunks  
- Spatial intersection: 25k row chunks

These chunk sizes are conservative and work on systems with limited RAM. They can be increased on more powerful systems.

### Garbage Collection
- `gc(full = TRUE)` forces R to release memory back to the OS
- Called after every chunk and every task
- Includes brief `Sys.sleep()` pauses to let the system stabilize

---

## Files Overview

| File | Purpose | Parallelization | Speed | Reliability |
|------|---------|----------------|-------|-------------|
| `process_ookla_data.R` | Core processing function (now with chunking) | N/A | N/A | N/A |
| `ultra_memory_safe_batch.R` | Ultra-safe batch processing | None (sequential) | Slow | Highest |
| `batch_process_all.R` | Standard batch processing | Configurable (default: 1) | Medium | High |
| `memory_safe_single_task.R` | Single task testing | None | Fast | High |
| `check_progress.R` | Progress monitoring | None | Instant | N/A |

---

## Example Workflow

1. **Start with the safe approach**:
   ```r
   Rscript scripts/data_download/ultra_memory_safe_batch.R
   ```

2. **Monitor progress** (in another terminal):
   ```r
   Rscript scripts/data_download/check_progress.R
   ```

3. **If interrupted**, just run again - it will resume automatically

4. **After completion**, check aggregated data in:
   - `data/aggregated/CIS_Internet_Speed_2019-2025.csv`
   - `data/aggregated/summary_statistics.csv`
   - `data/aggregated/[COUNTRY_CODE]_all_quarters.csv`

---

## Performance Tips

1. **Run overnight**: These processes take hours
2. **Don't use your computer** during processing to avoid memory competition
3. **Disable browser/other apps** to maximize available RAM
4. **Use SSD storage** if possible for faster I/O
5. **Check progress periodically** but don't interrupt unnecessarily

---

Last updated: 2025-10-28
