#!/bin/bash

set -euo pipefail

export JAVA_HOME="/opt/homebrew/opt/openjdk@17/libexec/openjdk.jdk/Contents/Home"
export PATH="/opt/homebrew/opt/openjdk@17/bin:$PATH"
export SPARK_LOCAL_HOSTNAME="localhost"

if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <sql_file_path> <output_json_path>"
  exit 1
fi

SQL_FILE_PATH="$1"
OUTPUT_JSON_PATH="$2"

"$(dirname "$0")/.venv/bin/spark-submit" --master 'local[*]' --jars spark-logical-plan-capture_2.12-0.1.0.jar start.py "$SQL_FILE_PATH" 2>run.log
python plan_decoder.py "$OUTPUT_JSON_PATH"