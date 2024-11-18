#!/bin/bash
set -euo pipefail
. /etc/re_dms.conf

echo "started finding production RDS replication slots lag $(date)"
cat /usr/local/bin/replication_slot_lag.sql | /usr/bin/psql -t -A -F"," "$SOURCE_CONNECTION_STRING" > /tmp/replication_slot_lags
cat /tmp/replication_slot_lags | nc -u -w1 127.0.0.1 8125
echo "finished finding production RDS replication slots lag $(date)"