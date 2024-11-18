#!/bin/bash
set -euo pipefail
. /etc/re_dms.conf

echo "started vacuum analyze $(date)"
/usr/local/bin/RedShift-ToolKit/VacuumAnalyzeUtility/vacuum-analyze-utility.sh -h $PG__HOST -u $PG__USER -d $PG__DBNAME -P $PG__PASSWORD -s public -o FULL -z 0
echo "finished vacuum analyze $(date)"