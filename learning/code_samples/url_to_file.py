#!/usr/bin/env python

from __future__ import print_function
from argparse import ArgumentParser
import requests

parser = ArgumentParser(description="Dumps an URL content into a text file")
parser.add_argument('-u', '--url', type=str, dest='url', help='url to dump to file')
parser.add_argument('-f', '--file', type=str, dest='filename', help='filename to dump url contents')

args = parser.parse_args()

url = args.url
filename = args.filename

try:
    url_object = requests.get(url)
    with open(filename,'w') as fd:
        fd.write(url_object.text.encode("UTF-8"))
except requests.ConnectionError as err:
    print("Error connecting")
except requests.HTTPError as err:
    print("Error in HTTP protocol")
except requests.RequestException as err:
    print("Request Exception!")
except IOError as err:
    print("IOError while using %s" % filename)
