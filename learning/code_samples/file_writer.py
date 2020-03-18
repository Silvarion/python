#!/usr/bin/env python3

import argparse

parser=argparse.ArgumentParser(description="Simple file writer to read from keyboard and write to file")

parser.add_argument('file_name',type=str,help="the path to the file you want to write")
args = parser.parse_args()

file_name = args.file_name
try:
    with open(file_name,'w') as f:
        print("Start typing, when done, just enter an empty line")
        line = input()
        while(line):
            f.write("%s\n" % line)
            line = input()
    print("%s writing finished." % file_name)
except IOError:
    print("File not found")
