#!/usr/bin/env python

from __future__ import print_function
from argparse import ArgumentParser
import os
import re
import sys
import subprocess

parser = ArgumentParser(description="Creates a new file with the right prmissions for python and adds the shebang line")
parser.add_argument('filename', type=str, help="Filename for the new file to be create, the .py extenion will be added if not found")

args = parser.parse_args()

py_filename =  args.filename
filename = args.filename

if re.compile("^[\w(.)_-]*\.py$",re.I).match(filename):
    print(".py extension found")
else:
    print("Adding .py extension")
    py_filename = filename+".py"

if os.path.isfile(filename) and filename != py_filename :
    print("ERROR: The file %s is already there..." % filename)

if os.path.isfile(py_filename):
    print("ERROR: The file %s is already there..." % py_filename)
    sys.exit(1)
else:
    try:
        with open(py_filename,'w') as fd:
            fd.write('#!/usr/bin/env python\n\n')
            fd.close()
        subprocess.call(["chmod","754",py_filename])
    except IOError as err:
        print(err)
    except subprocess.CalledProcessError as err:
        print(err)

