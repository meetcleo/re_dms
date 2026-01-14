#!/bin/bash
set -euo pipefail
. /etc/re_dms.conf

echo "started finding production RDS dead tuple percentages $(date)"
cat /usr/local/bin/dead_tuple_percentages.sql | /usr/bin/psql -t -A -F"," "$SOURCE_CONNECTION_STRING" > /tmp/dead_tuple_percentages
cat /tmp/dead_tuple_percentages | nc -u -w1 127.0.0.1 8125
echo "finished finding production RDS dead tuple percentages $(date)"