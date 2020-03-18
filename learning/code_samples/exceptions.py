#!/usr/bin/env python3

import argparse

parser = argparse.ArgumentParser(description='Read a file in reverse')
parser.add_argument('filename', help='File to read')
parser.add_argument('--limit', '-l', type=int, help='the number of lines to read')
parser.add_argument('--version', '-v', action='version',version='%(prog)s 2.0')

args = parser.parse_args()

try:
    f = open(args.filename)
except IOError as err:
    print("Error: file not found")
else:
    with f:
        limit = args.limit
        lines = f.readlines()
        lines.reverse()

        if limit:
            lines = lines[:limit]

        for line in lines:
            print(line.strip()[::-1])
finally:
    print("\nWe're finished!!!\n")

