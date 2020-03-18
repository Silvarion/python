##############################
#
#   Silvarion's Python Stamps - pystamps
#
#   @author: Jesus Alejandro Sanchez Davila
#
#   This is a group of utility functions/procedures
#   that aim to help the developer simplify some common steps
#   inside their programs/scripts.
#
################

# Required by class Timestamp
import datetime
import math
# Required to add the custom module path to the resources
import sys
import os
sys.path.append(os.path.split(sys.argv[0])[0])

# Config file reader        
def readConfigFile (file_handle):
    # Return variable
    config = {}
    # Open the configuration file
    file_pointer= open(file_handle)
    # Create a list with each line
    contents = [line.rstrip('\n') for line in file_pointer]
    # Process each line separately
    for line in contents:
        key,value = line.split('=')
        config[key] = value
    return config
""" Timestamp utility class """

class Timestamp:
    # Initialization constructor
    def __init__(self, strDate = '1970-01-01', strTime = '00:00:00.000'):
        tmpArray = strDate.split('-')
        self.Year = int(tmpArray[0])
        self.Month = int(tmpArray[1])
        self.Day = int(tmpArray[2])
        tmpArray = strTime.split(':')
        self.Hour = int(tmpArray[0])
        self.Minute = int(tmpArray[1])
        self.Second = int(tmpArray[2].split('.')[0])
        if len(tmpArray[2].split('.')) > 1 :
            self.Milis = int(tmpArray[2].split('.')[1])
        else:
            self.Milis = 0
    """ Methods """
    # Get the DATE part of the timestamp as string
    def getDate(self):
        strDate = str(self.Year)
        if self.Month < 10:
            strDate += '-0' + str(self.Month)
        else:
            strDate += '-' + str(self.Month)
        if self.Day < 10:
            strDate += '-0' + str(self.Day)
        else:
            strDate += '-' + str(self.Day)
        return strDate
    # Get the DATE part of the timestamp as string
    def getTime(self):
        if self.Hour < 10:
            strTime = '0' + str(self.Hour)
        else:
            strTime = str(self.Hour)
        if self.Minute < 10:
            strTime += ':0' + str(self.Minute)
        else:
            strTime += ':' + str(self.Minute)
        if self.Second < 10:
            strTime += ':0' + str(self.Second)
        else:
            strTime += ':' + str(self.Second)
        if self.Milis < 10:
            strTime += '.00' + str(self.Milis)
        elif self.Milis < 100:
            strTime += '.0' + str(self.Milis)
        else:
            strTime += '.' + str(self.Milis)
        return strTime
    # Get the Quarter of the year in which this date belongs
    def getQuarter(self):
        if self.Month < 4:
            return 1
        elif self.Month < 7:
            return 2
        elif self.Month < 10:
            return 3
        else:
            return 4
    # Get the Day of the Week
    def getDayOfWeek(self):
        dayNumber = datetime.datetime(self.Year, self.Month, self.Day, self.Hour, self.Minute, self.Second, self.Milis).weekday()
        if dayNumber == 0:
            return "Monday"
        elif dayNumber == 1:
            return "Tuesday"
        elif dayNumber == 2:
            return "Wednesday"
        elif dayNumber == 3:
            return "Thursday"
        elif dayNumber == 4:
            return "Friday"
        elif dayNumber == 5:
            return "Saturday"
        else:
            return "Sunday"
    # Get the Day of the Month
    def getDayOfMonth(self):
        return self.Day
    # Get the Day of the Year
    def getDayOfYear(self):
        counter=1
        result=0
        while counter < self.Month:
            result += self.daysOfMonth(counter)
            counter += 1
        return counter + self.Day
    #Get the week number of the year
    def getWeekOfYear(self):
        return math.floor(self.getDayOfYear()/7)
    ''' Timestamp Conversion Methods '''
    # Get the full timestamp as Integer
    def parseInt(self):
        return int(str(self.Year).zfill(4) + str(self.Month).zfill(2) + str(self.Day).zfill(2) + str(self.Hour).zfill(2) + str(self.Minute).zfill(2) + str(self.Second).zfill(2) + str(self.Milis).zfill(3))
    # Get the full timestamp as string
    def toString(self):
        stringed = self.getDate() + ' ' + self.getTime()
        return stringed
    # Date conversion
    def toDate(self):
        dt = datetime.date(self.Year, self.Month, self.Day)
        return dt
    # Time conversion
    def toTime(self):
        dt = datetime.time(self.Hour, self.Minute, self.Second, self.Milis * 1000)
        return dt
    # Datetime conversion
    def toDatetime(self):
        dt = datetime.datetime(self.Year, self.Month, self.Day, self.Hour, self.Minute, self.Second, self.Milis * 1000)
        return dt
    ''' Timestamp Utility Methods '''
    # Leap year assertion
    def isLeapYear(self):
        if self.Year%4 != 0:
            return False
        elif self.Year%100 != 0:
            return True
        elif self.Year%400 != 0:
            return False
        else:
            return True
    # Get days in month
    def daysInMonth(self):
        if self.isLeapYear():
            februaryDays = 29
        else:
            februaryDays = 28
        daysOfMonth = [31,februaryDays,31,30,31,30,31,31,30,31,30,31]
        return daysOfMonth[self.Month - 1]
    def daysOfMonth(self,month):
        if self.isLeapYear():
            februaryDays = 29
        else:
            februaryDays = 28
        daysOfMonth = [31,februaryDays,31,30,31,30,31,31,30,31,30,31]
        return daysOfMonth[month - 1]
    ''' Timestamp Operators '''
    # Add Years
    def addYears(self, amount):
        self.Year += amount
    # Add Months
    def addMonths(self, amount):
        totalMonth = self.Month + amount
        while totalMonth > 12:
            self.addYears(1)
            totalMonth -= 12
        self.Month = totalMonth
    # Add Days
    def addDays(self, amount):
        totalDays = self.Day + amount
        while totalDays > self.daysInMonth():
            totalDays -= self.daysInMonth()
            self.addMonths(1)
        if totalDays == 0:
            self.Day = 1
        else:
            self.Day = totalDays
    # Add Hours
    def addHours(self, amount):
        totalHours = self.Hour + amount
        while totalHours > 23:
            self.addDays(1)
            totalHours -= 24
        self.Hour = totalHours
    # Add Minutes
    def addMinutes(self, amount):
        totalMinutes = self.Minute + amount
        while totalMinutes > 59:
            totalMinutes -= 60
            self.addHours(1)
        self.Minute = totalMinutes
    # Add Seconds
    def addSeconds(self, amount):
        totalSeconds = self.Second + amount
        while totalSeconds > 59:
            totalSeconds -= 60
            self.addMinutes(1)
        self.Second = totalSeconds
    # Add Miliseconds
    def addMilis(self, amount):
        totalMilis = self.Milis + amount
        while totalMilis > 999:
            totalMilis -= 1000
            self.addSeconds(1)
        self.Milis = totalMilis
    
    # Substract Miliseconds
    def subMilis(self, amount):
        totalMilis = self.Milis - amount
        while totalMilis < 0:
            totalMilis += 1000
            self.subSeconds(1)
        self.Milis = totalMilis

    # Substract Seconds
    def subSeconds(self, amount):
        totalSeconds = self.Second - amount
        while totalSeconds < 0:
            totalSeconds += 60
            self.subMinutes(1)
        self.Second = totalSeconds
        
    # Substract Minutes
    def subMinutes(self, amount):
        totalMinutes = self.Minute - amount
        while totalMinutes < 0:
            totalMinutes += 60
            self.subHours(1)
        self.Minute = totalMinutes        
        
    # Substract Hours
    def subHours(self, amount):
        totalHours = self.Hour - amount
        while totalHours < 0:
            self.subDays(1)
            totalHours += 24
        self.Hour = totalHours        

    # Substract Days
    def subDays(self, amount):
        totalDays = self.Day - amount
        while totalDays < 1:
            totalDays += self.daysInMonth() #Need to put here number of days of previous month
            self.subMonths(1) # Need to implement subMonths and subYears
        if totalDays == 0:
            self.Day = 1
        else:
            self.Day = totalDays
            
    # Substract Months
    def subMonths(self, amount):
        totalMonth = self.Month - amount
        while totalMonth < 1:
            self.subYears(1)
            totalMonth += 12
        self.Month = totalMonth
    
    # Substract Years
    def subYears(self, amount):
        self.Year -= amount    