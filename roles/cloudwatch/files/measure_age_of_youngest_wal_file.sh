#!/bin/bash
set -euo pipefail

# date at which the most recent wal file was last modified in epoch seconds
then_new=$(ls /re_dms/output_wal/*.wal -tr | head -n 1 | xargs stat -c '%Y')

# now epoch seconds
now=$(date +%s)

# oldest file created seconds ago
newest_seconds_ago=$(($then_new-$now))

echo "seconds_since_newest_wal_file_modified:$newest_seconds_ago|g" | nc -u -w0 127.0.0.1 8125
