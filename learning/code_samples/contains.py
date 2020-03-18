#!/usr/bin/env python3

import argparse

parser = argparse.ArgumentParser(description='Search for words including partial word')
parser.add_argument('snippet', help='Partial (or complete) string to search for in the words file')

args = parser.parse_args()
snippet = args.snippet.lower()

words = open('/usr/share/dict/words').readlines()

print([word.strip() for word in words if snippet in word.lower()])
