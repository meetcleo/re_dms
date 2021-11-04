#!/bin/bash
set -euo pipefail

# date at which the oldest wal file was last modified in epoch seconds
then_old=$(ls /re_dms/output_wal/*.wal -tr | head -n 1 | xargs stat -c '%Y')

# now epoch seconds
now=$(date +%s)

# oldest file created seconds ago
old_seconds_ago=$(($then_old-$now))

echo "seconds_since_oldest_wal_file_modified:$old_seconds_ago|g" | nc -u -w0 127.0.0.1 8125
