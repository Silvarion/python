##############################
#
#   Silvarion's Python Timer - pytimer
#
#   @author: Jesus Alejandro Sanchez Davila
#
#   This is a simple timer for profiling purposes
#
################

from datetime import datetime, timedelta

class Timer:
    """Initialization"""
    def __init__(self):
        self.hours = 0
        self.minutes = 0
        self.seconds = 0
        self.total_seconds = 0.0
        self.microseconds = 0
        self.started = False
        self.stopped = False

    """String"""
    def __str__(self):
        return(f"{self.days}:{self.hours:02}:{self.minutes:02}:{self.seconds:02}.{self.microseconds}")

    def start(self):
        self.start_stamp = datetime.now()
        self.started = True
    
    def pause(self):
        self.stop_stamp = datetime.now()

    def stop(self):
        self.stop_stamp = datetime.now()
        self.stopped = True
        self.started = False
        self.calculateDelta()

    def calculateAllUnits(self, stamp: datetime):
        delta = stamp - self.start_stamp
        total_seconds = delta.total_seconds()
        days = int( delta.total_seconds() // (60*60*24))
        if days > 0:
            hours = int((delta.total_seconds() - (days*24*60*60)) % (60*60))
        else:
            hours = int(delta.total_seconds() // (60*60))
        if hours > 0:
            minutes = int((delta.total_seconds() - (days*24*60*60) - (hours*60*60)) % (60*60))
        else:
            minutes = int(delta.total_seconds() // 60)
        if minutes > 0:
            seconds = int((delta.total_seconds() - (days*24*60*60) - (hours*60*60) - (minutes*60)) % (60))
        else:
            seconds = int(delta.total_seconds())
        microseconds = (total_seconds - int( delta.total_seconds()))
        return {
            "total_seconds": total_seconds,
            "days": days,
            "hours": hours,
            "minutes": minutes,
            "seconds": seconds,
            "microseconds": microseconds
        }

    def getCurrentCount(self):
        return(self.calculateAllUnits(datetime.now()))
    
    def getCurrentCountAsString(self):
        calculated = self.calculateAllUnits(datetime.now())
        return f"{calculated["days"]:02}:{calculated["hours"]:02}:{calculated["minutes"]:02}:{calculated["seconds"]:02}"
    
    def printCurrentCount(self):
        current = self.calculateAllUnits(datetime.now())
        print(f"{current['days']:02}:{current['hours']:02}:{current['minutes']:02}:{current['seconds']:02}.{int(current['microseconds']*10000)}")

    def calculateDelta(self, stamp = None):
        if stamp is None and self.stop_stamp is not None:
            delta = self.stop_stamp - self.start_stamp
        else:
            delta = stamp - self.start_stamp
        calcs = self.calculateAllUnits(delta.total_seconds)
        self.days = calcs["days"]
        self.hours = calcs["hours"]
        self.minutes = calcs["minutes"]
        self.seconds = calcs["seconds"]
        self.microseconds = calcs["microseconds"]
        self.total_seconds = calcs["total_seconds"]
        
    def reset(self):
        self.__init__()

    