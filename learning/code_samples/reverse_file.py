#!/usr/bin/env python3

import argparse

parser = argparse.ArgumentParser(description='Read a file in reverse')
parser.add_argument('filename', help='File to read')
parser.add_argument('--limit', '-l', type=int, help='the number of lines to read')
parser.add_argument('--version', '-v', action='version',version='%(prog)s 2.0')

args = parser.parse_args()

print(args.filename)
