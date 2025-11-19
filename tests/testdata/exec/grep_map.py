#!/usr/bin/env -S python3 -u
"""
Grep mapper.

Input: <line>
Output: <1><tab><line>
"""
import sys


def main():
    """Perform mapping."""
    # Define our query term
    if len(sys.argv) == 1:
        query = "product"
    else:
        query = sys.argv[1]

    # Loop over every line in standard in
    for line in sys.stdin:
        line = line.strip()

        if not line:
            continue

        # Print key, value pair if query present in this line
        if query in line.lower():
            print("1\t" + line)


if __name__ == "__main__":
    main()
