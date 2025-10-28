# Quick Start Guide - Parallel Processing with Checkpoints

## TL;DR

✅ **Yes, you can run in parallel!**
✅ **Checkpoints work automatically in both sequential and parallel modes!**

## 3 Steps to Get Started

### 1️⃣ Check Your System
```powershell
Rscript scripts/data_download/configure_cores.R
```

### 2️⃣ Configure Parallelization
Edit `batch_process_all.R` (line ~81):
```r
n_cores <- 2  # Change based on your RAM (1-6)
```

**Quick Guide:**
- **8-16 GB RAM**: Use `n_cores = 1`
- **16-24 GB RAM**: Use `n_cores = 2`
- **24-32 GB RAM**: Use `n_cores = 3`
- **32+ GB RAM**: Use `n_cores = 4-6`

### 3️⃣ Run It!
```powershell
Rscript scripts/data_download/batch_process_all.R
```

---

## Checkpoint System (Built-In)

### Automatic Resume - Always Works!

✅ **The script ALWAYS checks for completed files before processing**
✅ **Works in both sequential (n_cores=1) and parallel (n_cores>1) modes**
✅ **You can safely stop and restart anytime**

### What You'll See:
```
Country 1 of 11: Armenia
✓ Checkpoint: Skipping 25 already completed tasks
Processing 27 remaining tasks...
```

### How It Works:
1. Before processing each country, script checks `data/processed/` folder
2. Lists all expected task files (e.g., `AM_2019Q2_fixed.rds`)
3. Automatically skips any files that already exist
4. Processes only missing files

### If You Want to Reprocess:
- Just delete the specific `.rds` file from `data/processed/`
- Script will automatically reprocess it on next run

---

## Parallel Processing Explained

### Sequential Mode (n_cores = 1)
- Processes ONE task at a time
- Most memory-safe
- Slowest (8-10 hours)

### Parallel Mode (n_cores = 2+)
- Processes MULTIPLE tasks simultaneously
- Faster (2-6 hours depending on cores)
- Needs more RAM

### Memory Usage Formula:
**Approximate RAM per task: 2-4 GB**
**Total RAM needed: (n_cores × 4 GB) + 4 GB overhead**

Examples:
- `n_cores = 1`: ~8 GB RAM needed
- `n_cores = 2`: ~12-16 GB RAM needed
- `n_cores = 3`: ~16-20 GB RAM needed
- `n_cores = 4`: ~20-24 GB RAM needed

---

## If You Get OOM (Out of Memory) Errors

### Quick Fix:
1. Reduce `n_cores` by 1
2. Or use: `Rscript scripts/data_download/ultra_memory_safe_batch.R`

### Why It Happens:
- Not enough RAM for parallel processing
- Other applications using memory
- Some quarters have very large datasets

### Solution:
The ultra-safe script forces sequential processing with extra memory monitoring.

---

## Monitoring Progress

### Check Progress Anytime:
```powershell
Rscript scripts/data_download/check_progress.R
```

Shows:
- How many tasks completed (e.g., 150/572)
- Progress percentage
- Which countries are done
- Estimated time remaining

### During Processing:
- You'll see real-time progress bars (in parallel mode)
- Or task-by-task completion (in sequential mode)

---

## Examples

### Example 1: Conservative Approach (Safe)
```r
# In batch_process_all.R
n_cores <- 1
```
- RAM needed: 8-16 GB
- Time: 8-10 hours
- Risk: Very low

### Example 2: Balanced Approach (Recommended)
```r
# In batch_process_all.R
n_cores <- 2
```
- RAM needed: 16-24 GB
- Time: 4-6 hours
- Risk: Low

### Example 3: Fast Approach (If You Have RAM)
```r
# In batch_process_all.R
n_cores <- 4
```
- RAM needed: 24-32 GB
- Time: 2-3 hours
- Risk: Medium (reduce if OOM errors)

---

## FAQ

### Q: Will I lose progress if I stop the script?
**A:** No! Checkpoints are saved after EACH task. Just restart the script.

### Q: Can I change n_cores mid-run?
**A:** Yes! Stop the script (Ctrl+C), change n_cores, and restart. It will resume from where it left off.

### Q: What if a specific task keeps failing?
**A:** Use `memory_safe_single_task.R` to debug that specific task. Edit the config to target the failing quarter.

### Q: Can I run multiple scripts at once?
**A:** Not recommended - they'll compete for memory and may both fail.

### Q: Does parallel mode check files correctly?
**A:** YES! The checkpoint system works identically in both modes.

### Q: How do I know which n_cores to use?
**A:** Run `configure_cores.R` - it analyzes your system and recommends a value.

---

## Summary

| Feature | Status | Notes |
|---------|--------|-------|
| Parallel Processing | ✅ YES | Configurable 1-6+ cores |
| Checkpoint/Resume | ✅ YES | Automatic, always works |
| Memory Safety | ✅ YES | Chunked processing built-in |
| Progress Monitoring | ✅ YES | Real-time updates |
| Error Recovery | ✅ YES | Auto-resumes on restart |

**Bottom Line**: Configure your `n_cores`, run the script, and let it work. If interrupted, just run again - it picks up where it left off!

---

Last updated: 2025-10-28
