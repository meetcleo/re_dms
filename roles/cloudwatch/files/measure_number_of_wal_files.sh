#!/bin/bash
set -euo pipefail

number_of_wal_files=$(ls /re_dms/output_wal/*.wal | wc -l)
echo "number_of_wal_files:$number_of_wal_files|g" | nc -u -w0 127.0.0.1 8125
