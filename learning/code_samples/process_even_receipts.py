#!/usr/bin/env python3

import shutil
import glob
import json
import os
import re
import math

try:
    os.mkdir("./processed")
except OSError:
    print("'processed' directory already exists!")

# Get a list of recepits
#receipts = glob.glob('./new/receipt-[0-9]*.json')
receipts = [f for f in glob.iglob('./new/receipt-[0-9]*.json')
                 if re.match('./new/receipt-[0-9]*[02468].json', f)]
subtotal = 0.0

# Iterate over the receipst
for path in receipts:
    with open(path) as f:
# Read contents
        content = json.load(f)
# Calculate subtotal
        subtotal += float(content['value'])
# Move the file to the processed directory
    destination = path.replace('new','processed')
    shutil.move(path, destination)
    print("Moved %s to %s" % (path,destination))
# Print the subtotal of all processed receipts
print("Receipt subtotal: $%s" % round(subtotal,2))
