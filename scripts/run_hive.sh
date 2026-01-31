#!/bin/bash
# Run Hive SQL script with date parameter
# Usage: ./run_hive.sh <sql_file> [date]

set -e

SQL_FILE=$1
DT=${2:-$(date -d "yesterday" +%Y-%m-%d)}

if [ -z "$SQL_FILE" ]; then
    echo "Usage: $0 <sql_file> [date]"
    exit 1
fi

echo "Running: $SQL_FILE with dt=$DT"
hive -f "$SQL_FILE" -hivevar dt="$DT"
