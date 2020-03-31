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
        self.microseconds = 0
        self.started = False
        self.stopped = False

    def start(self):
        self.start_stamp = datetime.now()
        self.started = True
    
    def pause(self):
        self.stop_stamp = datetime.now()

    def stop(self):
        self.stop_stamp = datetime.now()
        self.stopped = True
        self.started = False

    def calculateAllUnits(self, delta_total_secs):
        return {
            "hours": int(delta_total_secs) // 3600,
            "minutes": int(delta_total_secs) // 60,
            "seconds": int(delta_total_secs),
            "microseconds": (float(delta_total_secs) - int(delta_total_secs)) * 1000000
        }

    def getCurrentCount(self):
        return(self.calculateAllUnits((datetime.now()-self.start_stamp).total_seconds))

    def calculateDelta(self, stamp = None):
        if stamp is None and self.stop_stamp is not None:
            delta = self.stop_stamp - self.start_stamp
        else:
            delta = stamp - self.start_stamp
        calcs = self.calculateAllUnits(delta.total_seconds)
        self.hours = calcs["hours"]
        self.minutes = calcs["minutes"]
        self.seconds = calcs["seconds"]
        self.microseconds = calcs["microseconds"]
        
    def reset(self):
        self.__init__()

