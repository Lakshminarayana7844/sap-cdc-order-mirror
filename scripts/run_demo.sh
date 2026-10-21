#!/usr/bin/env bash
# End-to-end demo: generate CDC events, then mirror them into Delta once.
set -euo pipefail
cd "$(dirname "$0")/.."
python src/generator/generate_cdc.py --out cdc_out
spark-submit \
  --packages io.delta:delta-spark_2.12:3.2.0 \
  src/streaming/mirror_job.py --input cdc_out --delta delta/orders --once
