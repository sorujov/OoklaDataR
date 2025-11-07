#!/bin/bash
#SBATCH --job-name=cis_ookla_all
#SBATCH --output=logs/cis_all_countries_%j.out
#SBATCH --error=logs/cis_all_countries_%j.err
#SBATCH --time=24:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=60
#SBATCH --mem=0
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=your.email@example.com

echo "========================================="
echo "Job started at: $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "CPUs allocated: $SLURM_CPUS_PER_TASK"
echo "Memory allocated: All available on node"
echo "========================================="
echo ""

# Change to project directory (corrected path)
PROJECT_DIR="/share/home/orujov/OoklaDataR"
if [ -d "$PROJECT_DIR" ]; then
  cd "$PROJECT_DIR"
else
  echo "WARNING: Project dir $PROJECT_DIR not found (pwd=$(pwd))" >&2
fi

# Activate conda environment
source ~/miniforge3/etc/profile.d/conda.sh || { echo "Failed to source conda.sh" >&2; exit 1; }
conda activate .ookladatar || { echo "Failed to activate environment .ookladatar" >&2; exit 1; }

# Check conda environment
echo "Using conda environment: .ookladatar"
which Rscript
echo ""

# Set number of cores for R to use
export SLURM_CPUS_PER_TASK=60
export OMP_NUM_THREADS=60

echo "Will use 60 cores for parallel processing"
echo ""

# Run the main R script
echo "Starting ALL CIS countries download (sequential with parallel quarters)..."
echo ""
Rscript main.R

# Check exit status
if [ $? -eq 0 ]; then
    echo ""
    echo "========================================="
    echo "SUCCESS: Job completed successfully"
    echo "Job finished at: $(date)"
    echo "========================================="
else
    echo ""
    echo "========================================="
    echo "ERROR: Job failed with exit code $?"
    echo "Job finished at: $(date)"
    echo "========================================="
    exit 1
fi
