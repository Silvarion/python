#!/usr/bin/env python3

message = input("Please enter a mesage: ")

count = int(input("Please enter the number of times to repeat it [1]: ").strip() or "1",10)

def echoit(msg, cnt):
    for iteration in range (cnt):
        print("Your message: %s" % msg)

echoit(message, count)
