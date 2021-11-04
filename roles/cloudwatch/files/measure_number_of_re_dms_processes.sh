#!/bin/bash
set -euo pipefail

number_of_re_dms_processes_running=$(ps -C re_dms --no-headers | wc -l)
echo "number_of_re_dms_processes_running:$number_of_re_dms_processes_running|g" | nc -u -w0 127.0.0.1 8125
