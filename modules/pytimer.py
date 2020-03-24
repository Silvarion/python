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
        self.days = 0
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

    def calculateDelta(self):
        delta = self.stop_stamp - self.start_stamp
        self.microseconds = delta.microsecond
        total_seconds = delta.total_seconds
        
        
    def reset(self):
        self.__init__()
