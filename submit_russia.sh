#!/bin/bash
#SBATCH --job-name=ookla_russia
#SBATCH --output=logs/russia_%j.out
#SBATCH --error=logs/russia_%j.err
#SBATCH --time=48:00:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=20
#SBATCH --mem=128G
#SBATCH --mail-type=END,FAIL

# Russia download with batched processing (4 tasks at a time) to avoid OOM

set -euo pipefail

echo "========================================="
echo "RUSSIA Ookla Download (Batched)"
echo "Job started: $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $SLURM_NODELIST"
echo "Memory: 128GB"
echo "CPUs: 20 (4 parallel tasks at a time)"
echo "========================================="

cd /share/home/orujov/OoklaDataR

source ~/miniforge3/etc/profile.d/conda.sh
conda activate .ookladatar

export SLURM_CPUS_PER_TASK=20
export OMP_NUM_THREADS=20

mkdir -p logs data/processed/RU

echo "Starting Russia batched download..."
Rscript scripts/test_russia_batched.R

status=$?
if [ $status -eq 0 ]; then
    echo "SUCCESS: Russia data collected"
else
    echo "ERROR: Job failed (exit $status)"
fi

echo "Finished: $(date)"
