#!/bin/bash
set -euo pipefail

echo "calculating number_of_wal_files"
number_of_wal_files=$(ls /re_dms/output_wal/*.wal | wc -l)

data="number_of_wal_files:$number_of_wal_files|g"
echo $data
echo $data | nc -u -w1 127.0.0.1 8125
echo "done calculating number_of_wal_files"

