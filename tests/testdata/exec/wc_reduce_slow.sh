#!/bin/bash
#
# Word count reducer with artificial delay.
#
# Input: <word><tab><count>
# Output: <word><tab><total>
#
# NOTE: this code assumes that the value is "1" for every key.

# Stop on errors
set -Eeuo pipefail

# Simulate a long running job
sleep 3

# Reduce
cat | cut -f1 | uniq -c | awk '{print $2"\t"$1}'
