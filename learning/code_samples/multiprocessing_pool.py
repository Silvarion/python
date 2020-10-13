#!/usr/bin/env python3

from multiprocessing import Pool
from datetime import datetime
from time import sleep
from random import random

# Global scope variables
WORKERS = 100

# Functions
def get_timestamp(number):
    sleep(random()*3)
    return f"Got {number} at {datetime.now()}"

def main():
    example_array = range(100)
    # Start workers pool
    with Pool(processes=WORKERS) as pool:
        # Ordered
        for i in pool.map(get_timestamp,example_array):
            print(i)
        # Unordered execution (sequential printing)
        # for i in pool.imap_unordered(get_timestamp,example_array):
        #     print(i)

if __name__ == "__main__":
    main()