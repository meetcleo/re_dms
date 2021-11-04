#!/bin/bash
set -euo pipefail

# date at which the most recent wal file was last modified in epoch seconds
echo "calculating age of youngest wal file"
then_new=$(ls /re_dms/output_wal/*.wal -t | head -n 1 | xargs stat -c '%Y')

# now epoch seconds
now=$(date +%s)

# oldest file created seconds ago
newest_seconds_ago=$(($now-$then_new))

data="seconds_since_newest_wal_file_modified:$newest_seconds_ago|g"
echo $data
echo $data | nc -u -w1 127.0.0.1 8125
echo "done calculating age of youngest wal file"
