#!/bin/bash
set -euo pipefail

echo "calculating number_of_re_dms_processes_running"
number_of_re_dms_processes_running=$(ps -C re_dms --no-headers | wc -l)

data="number_of_re_dms_processes_running:$number_of_re_dms_processes_running|g"
echo $data
echo $data | nc -u -w1 127.0.0.1 8125
echo "done calculating number_of_re_dms_processes_running"
