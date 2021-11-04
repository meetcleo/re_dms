#!/bin/bash
set -euo pipefail

echo "calculating number_of_processes_owned_by_re_dms"
number_of_processes_owned_by_re_dms=$(ps -u re_dms --no-headers | wc -l)

data="number_of_processes_owned_by_re_dms:$number_of_processes_owned_by_re_dms|g"
echo $data
echo $data | nc -u -w1 127.0.0.1 8125
echo "done calculating number_of_processes_owned_by_re_dms"
