#!/usr/bin/env python3

import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('-f', '--filename', type=str, dest='file_name', help='File to read')
parser.add_argument('-l', '--line', type=int, dest='line_number', help='Line to print')

args = parser.parse_args()

try:
    with open(args.file_name) as f:
        lines=f.readlines()
        print("Line %s: %s" % (args.line_number,lines[args.line_number - 1]))
except IOError:
    print("Sorry, file not found!")
except OSError:
    print("File not found")
except IndexError:
    print("File too short, line not found")

