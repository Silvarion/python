#!/usr/bin/env python3
# -*- coding: utf-8 -*-

###################
##
##  Author: Jesus Sanchez
## 
##  Filename: safe_mysqld_restart.py
##
##  Description:
##      This script backups a replica database and syncs
##  the backup files to a NFS location
##
##  Usage: sudo ./mysql_dump_to_nfs.py
##
#############################

import datetime
import configparser
import json
import logging
import os
import sys

logging.basicConfig(level=logging.INFO)

def read_config_file(path):
  logger = logging.getLogger('read_config_file')
  logger.debug("Entering read_mysql_config_file")
  # Read MySQL Configuration File
  logger.info("Reading MySQL config file")
  my_cnf = configparser.ConfigParser( strict=False, empty_lines_in_values=False )
  my_cnf.readfp(open(path,'r'))
  # Create dictionary with the data needed
  mysql_cnf = {}
  mysql_cnf["data"] = my_cnf.get("mysqld","datadir")
  mysql_cnf["socket"] = my_cnf.get("mysqld","socket")
  mysql_cnf["pid-file"] = my_cnf.get("mysqld_safe","pid-file")
  # Return the dictionary
  return mysql_cnf

# MAIN ALGORYTHM ###
logger = logging.getLogger('main')
logger.info("Intializing...")
## Variables
MYSQL_CONFIG_FILE_PATH = '/etc/my.cnf'

## Read config file
config = read_config_file(MYSQL_CONFIG_FILE_PATH)

## 