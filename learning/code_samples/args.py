#!/usr/bin/env python3

import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('filename', help='File to read')

args = parser.parse_args()

print("First argument %s" % sys.argv[1])

