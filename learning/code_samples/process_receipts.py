#!/usr/bin/env python3

import shutil
import glob
import json
import os

try:
    os.mkdir("./processed")
except OSError:
    print("'processed' directory already exists!")

# Get a list of recepits
receipts = glob.glob('./new/receipt-[0-9]*.json')
subtotal = 0.0

# Iterate over the receipst
for path in receipts:
    with open(path) as f:
# Read contents
        content = json.load(f)
# Calculate subtotal
        subtotal += float(content['value'])
# Move the file to the processed directory
    name = path.split('/')[-1]
    destination = "./processed/%s" % name
    shutil.move(path, destination)
    print("Moved %s to %s" % (path,destination))
# Print the subtotal of all processed receipts
print("Receipt subtotal: $%.2f" % subtotal)
