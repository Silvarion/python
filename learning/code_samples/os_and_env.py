#!/usr/bin/env python3

from math import pi
import os

decimals = int(os.getenv('DIGITS').strip() or 10)

print(round(pi,decimals))
