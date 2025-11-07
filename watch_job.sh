#!/bin/bash
while true; do
  clear
  echo "=== Azerbaijan Job Status ==="
  echo "Time: $(date)"
  echo ""
  squeue -u $USER -o "%.10i %.9P %.12j %.8u %.2t %.10M %.6D %.8R %.4C"
  echo ""
  echo "=== Latest Output (last 30 lines) ==="
  tail -30 /share/castor/home/orujov/OoklaDataR/logs/azerbaijan_test_68770.out 2>/dev/null || echo "Log file not ready yet..."
  echo ""
  echo "Press Ctrl+C to stop watching"
  sleep 15
done
