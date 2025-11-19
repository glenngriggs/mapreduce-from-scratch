#!/bin/bash
#
# Word count mapper with artificial delay.
#
# Input: <text>
# Output: <word><tab><1>

# Stop on errors
set -Eeuo pipefail

# Simulate a long running job
sleep 3

# Map
cat | tr '[ \t]' '\n' | tr '[:upper:]' '[:lower:]' | awk '{print $1"\t1"}'
