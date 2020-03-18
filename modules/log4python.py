########
#
# Filename: jasd_log4py.py
# Author: Jesus Alejandro Sanchez Davila
# Name: jasd_log4py
#
# Description: Logger utility for Python scripts
#
##########

# Imports
from datetime import datetime
from inspect import currentframe,getframeinfo


# Class Logger
class Logger(object):
    # Variables
    logweights = { 'DEBUG' : 0, 'LOG' : 10 , 'NOTICE': 20 , 'INFO' : 30 , 'WARNING' : 40 , 'ERROR' : 50 , 'CRITICAL' : 60 }

    # Constructors
    def __init__(self,loglevel = 'WARNING'):
        self.level = loglevel

    # Getters and Setters
    def get_log_level(self):
        return (self.level)

    def set_log_level(self,loglevel):
        self.level = loglevel

    # Auxiliar Print functions
    def debug(self,message):
        self.printlog('DEBUG',message)

    def log(self,message):
        self.printlog('LOG',message)

    def info(self,message):
        self.printlog('INFO',message)

    def notice(self,message):
        self.printlog('NOTICE',message)

    def warning(self,message):
        self.printlog('WARNING',message)

    def error(self,message):
        self.printlog('ERROR',message)

    def critical(self,message):
        self.printlog('CRITICAL',message)

    # Main print function
    def printlog(self,loglevel,message):
        timestamp = datetime.now()
        cf = currentframe()
        if loglevel == 'DEBUG' and self.logweights[loglevel] >= self.logweights[self.level]:
                        print("[%s][%s][File: %s][Line: %s] %s" % (timestamp,loglevel,getframeinfo(cf).filename,cf.f_back.f_lineno,message))
        else:
            if self.logweights[loglevel] >= self.logweights[self.level]:
                print("[%s][%s][File: %s][Line: %s] %s" % (timestamp,loglevel,getframeinfo(cf).filename,cf.f_back.f_lineno,message))

