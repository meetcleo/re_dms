#!/bin/bash
set -euo pipefail

# date at which the oldest wal file was last modified in epoch seconds
echo "calculating age of oldest wal file"
then_old=$(ls /re_dms/output_wal/*.wal -tr | head -n 1 | xargs stat -c '%Y')

# now epoch seconds
now=$(date +%s)

# oldest file created seconds ago
old_seconds_ago=$(($now-$then_old))

data="seconds_since_oldest_wal_file_modified:$old_seconds_ago|g"
echo $data
echo $data | nc -u -w1 127.0.0.1 8125
echo "done calculating age of oldest wal file"
