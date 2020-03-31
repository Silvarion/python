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

    def calculateAllUnits(self, delta_total_secs):
        delta = self.stop_stamp - self.start_stamp
        return {
            total_seconds = delta.total_seconds()
            days = int(self.total_seconds // (60*60*24))
            hours = int(self.total_seconds // (60*60) - (self.days * 24))
            minutes = (int(self.total_seconds // 60) - (self.days * 24) - (self.hours * 60 * 60))
            seconds = int(delta.seconds - self.minutes * 60 - (self.days * 24) - (self.hours * 60 * 60))
            microseconds = (total_seconds - int(self.total_seconds))
        }

    def getCurrentCount(self):
        return(self.calculateAllUnits((datetime.now()-self.start_stamp).total_seconds))

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

