#!/usr/bin/env python3

def gather_info():
    height = float(input("What is your height? (inches or meters) "))
    weight = float(input("What is your weight? (pounds or kilograms) "))
    unit = input("Are your measurements in metric or imperial units? ").lower().strip()
    return height, weight, unit

def calculate_bmi(weight, height,unit='metric'):
    if unit.startswith('i'):
        bmi = 703 * (weight / (height ** 2))
    elif unit.startswith('m'):
        bmi = (weight / (height ** 2))
    print("Your BMI is %s" % bmi)

while True:
    height, weight, unit = gather_info()
    if unit.startswith('i'):
        calculate_bmi(weight=weight, height=height, unit='imperial')
        break
    elif unit.startswith('m'):
        calculate_bmi(height=height, weight=weight)
        break
    else:
        print("Error: Unknown measurement system. Please use imperial or metric.")
