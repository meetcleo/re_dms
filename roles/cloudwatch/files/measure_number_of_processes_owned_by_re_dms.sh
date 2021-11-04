#!/bin/bash
set -euo pipefail

number_of_processes_owned_by_re_dms=$(ps -u re_dms --no-headers | wc -l)
echo "number_of_processes_owned_by_re_dms:$number_of_processes_owned_by_re_dms|g" | nc -u -w0 127.0.0.1 8125
