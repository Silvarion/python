#!/usr/bin/env python3

import random
import os
import json

count = os.getenv("FILE_COUNT") or 100
words = [word.strip() for word in open('/usr/share/dict/words').readlines()]

try:
        os.mkdir("./new")
except OSError:
        print("'processed' directory already exists!")

for identifier in range(1, count + 1):
    amount = random.uniform(1.0, 1000.0)
    content = {
        'topic': random.choice(words),
        'value': "%.2f" % amount
    }
    with open('./new/receipt-%s.json' % identifier, 'w') as f:
        json.dump(content, f)

