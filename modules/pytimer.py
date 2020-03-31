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

    def calculateDelta(self):
        delta = self.stop_stamp - self.start_stamp
        self.total_seconds = delta.total_seconds()
        self.days = int(self.total_seconds // (60*60*24))
        self.hours = int(self.total_seconds // (60*60) - (self.days * 24))
        self.minutes = int(self.total_seconds // 60 - (self.days * 24) - (self.hours * 60 * 60))
        self.seconds = int(delta.seconds - self.minutes * 60 - (self.days * 24) - (self.hours * 60 * 60))
        self.microseconds = delta.microseconds
        
    def reset(self):
        self.__init__()
