#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###################
##
##  Author: Jesus Sanchez
## 
##  Filename: safe_mysqld_restart.py
##
##  Description:
##      This script is intended to restart safely a local
##  MySQL instance (only replicas. It won't restart a master)
##
##  Usage: sudo ./safe_mysqld_restart.py
##
#############################

import os
import subprocess

amIroot = os.getuid()

if amIroot != 0:
    print("[" + subprocess.run(["date", "+'%Y%m%d %H:%M:%S'"]) + "][ERROR] This script must be run as root!\n")
else:
    print("[" + subprocess.run(["date", "+'%Y%m%d %H:%M:%S'"]) + "][INFO] ROOT user detected. Proceeding.\n")
    mysqlStatus = subprocess.run(["systemctl", "status", "mysqld.service", "| grep \"Active\" | cut -d\":\" -f2 | cut -d\" \" -f2"] )
    if mysqlStatus == 'active':
        print("[" + subprocess.run(["date", "+'%Y%m%d %H:%M:%S'"]) + "][INFO] MySQL is running.\n")
    else:
        print("[" + subprocess.run(["date", "+'%Y%m%d %H:%M:%S'"]) + "][WARNING] MySQL is NOT running.\n")
