#!/usr/bin/env python3

import os
import subprocess
from argparse import ArgumentParser

parser = ArgumentParser(description='kill the running process listening on a given port')
parser.add_argument('port', type=int, help='the port number to search for')

port = parser.parse_args().port

try:
    result = subprocess.check_output(["lsof", "-n","-i4TCP:%s" % port])
except subprocess.CalledProcessError as err:
    print("Error: %s" % err)
    exit(1)

for line in result.splitlines():
    if "LISTEN" in line:
        listening = line
        break

if listening:
    pid = int(listening.split()[1])
    os.kill(pid,9)
    print("Process %s killed" % pid)
else:
    print("No processess listening at port %s" % port)
    exit(1)
