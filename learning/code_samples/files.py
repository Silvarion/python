#!/usr/bin/env python3

from __future__ import print_function

xmen_file = open('xmen.txt')

# All contents in one string
print(xmen_file.read())
xmen_file.close()

xmen_file = open('xmen.txt','a')
xmen_file.writelines(['Morph\n','Nighcrawler\n'])
xmen_file.close()

# Line by line
xmen_file = open('xmen.txt')
for line in xmen_file:
    print(line, end='')

xmen_file.close()

xmen_file = open('xmen.txt','w')

with open('xmen.txt', 'a') as xmen_file:
    xmen_file.write("Professor X\n")
