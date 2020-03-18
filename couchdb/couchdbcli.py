#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mysql_wrapper.py: 
  Very simple CLI-UI client to interact with CouchDB databases using couchdblib

Requirements:
  - Python 3
  - couchbdlib
  - dialog
"""

__version__     = "1.0"
__author__      = "Jesus Alejandro Sanchez Davila"
__maintainer__  = "Jesus Alejandro Sanchez Davila"
__email__       = "jsanchez.consultant@gmail.com"
__status__      = "Alpha"

import couchdblib
from argparse import ArgumentParser
from datetime import datetime
import getpass
import json
import logging
import os
import sys
import subprocess
from logging import DEBUG
from logging import CRITICAL
from logging import ERROR
from logging import FATAL
from logging import INFO
from logging import WARNING

from dialog import Dialog
# http://pythondialog.sourceforge.net/doc/intro/intro.html

# Add local path for custom modules/files
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
  sys.path.insert(1, path)
del path


# Instantiate Logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

## Global Variables

## Functions

## General Functions

def get_db_credentials(dialog_instance):
    creds = {}
    code, creds['user'] = dialog_instance.inputbox(text='Please enter the username')
    if code == dialog_instance.OK:
        code, creds['pswd'] = dialog_instance.passwordbox(text='Please enter the password', insecure=True)
        if code == dialog_instance.OK:
            return creds
        else:
            return {"status": "error"}
    else:
        return {"status": "error"}

def get_couch(dialog_instance, credentials = None):
    code, host = dialog_instance.inputbox(text='Please enter the hostname of the CouchDB server',width=80)
    if code == dialog_instance.OK:
        code, port = dialog_instance.inputbox(text='Please enter the port',init='5984')
        if code == dialog_instance.OK:
            if credentials is None:
                answer = dialog_instance.yesno(text='Does this CouchDB require authentication?')
                logger.log(DEBUG,f"User replied {answer}")
                if answer == 'ok':
                    creds = get_db_credentials(dialog_instance)
                    return couchdblib.Server(hostname=host,port=port,username=creds['user'],password=creds['pswd'],log_level=INFO)
                else:
                    return couchdblib.Server(hostname=host,port=port,log_level=INFO)
            else:
                return couchdblib.Server(hostname=host,port=port,username=credentials['user'],password=credentials['pswd'],log_level=INFO)
        else:
            return {"status": "error"}
    else:
        return {"status": "error"}

def test_db_connect(dialog_instance, couch):
    dialog_instance.msgbox(text=json.dumps(couch.up(),indent=2))

### Server menu
def server_menu(dialog_instance):
    return dialog_instance.menu(
        text="Select your option",
        height=15,
        width=50,
        choices=[
            ("C", "CouchDB Server Configuration"),
            ("D", "Database"),
            ("T", "Document"),
            ("U", "Users"),
            ("Q", "Quit")
        ]
    )

### Database Menu
def database_menu(dialog_instance):
    return dialog_instance.menu(
        text="Select your option",
        height=15,
        width=50,
        choices=[
            ("C", "Create Database"),
            ("D", "Delete Database"),
            ("L", "List all databases"),
            ("S", "Show database info"),
            ("M", "Back to Main menu"),
            ("Q", "Quit")
        ]
    )

### Document Menu
def document_menu(dialog_instance):
    return dialog_instance.menu(
        text="Select your option",
        height=15,
        width=50,
        choices=[
            ("C", "Create Document"),
            ("D", "Delete Document"),
            ("M", "Back to Main menu"),
            ("Q", "Quit")
        ]
    )

### Users Menu
def users_menu(dialog_instance):
    return dialog_instance.menu(
        text="Select your option",
        height=15,
        width=50,
        choices=[
            ("C", "Create User"),
            ("D", "Drop User"),
            ("R", "Roles"),
            ("B", "Back to Main menu"),
            ("Q", "Quit")
        ]
    )

## Database creation drop
def create_delete_db(dialog_instance, action, couch = None, credentials = None):
    if couch is None:
        if credentials is None:
            answer = dialog_instance.yesno(text='Does this CouchDB require authentication?')
            logger.log(DEBUG,f"User replied {answer}")
            if answer == 'ok':
                creds = get_db_credentials(dialog_instance)
                couch = get_couch(dialog_instance, creds)
    couch = get_couch(dialog_instance, creds)
    if action == 'create':
        code, db  = dialog_instance.inputbox(text="Please enter the name of the database to create")
        if code == dialog_instance.OK:
            database = couchdblib.Database(couch, db)
            dialog_instance.msgbox(text=json.dumps(database.create(),indent=2))
        else:
            logger.log(DEBUG,'TODO: Implement this option')
            return code
    if action == 'delete':
        code, db = dialog_instance.inputbox(text="Please enter the name of the database to delete")
        if code == dialog_instance.OK:
            database = couchdblib.Database(couch, db)
            dialog_instance.msgbox(text=json.dumps(database.delete(),indent=2))
        else:
            logger.log(DEBUG,'TODO: Implement this option')
            return code
    else:
        return code    

## User creation/drop
def create_drop_user(dialog_instance):
    choice = ''
    code = ''
    while choice not in ['C','D','B','R','Q']:
        code, choice = users_menu(dialog_instance)
    if code == dialog_instance.OK:
        if choice == 'A':
            creds = get_db_credentials(dialog_instance)
            logger.log(DEBUG,f"Credentials >> User: {creds['user']} - Password: {creds['pswd']}")
            logger.log(DEBUG,'TODO: Implement this option')
            return code
        elif choice == 'D':
            creds = get_db_credentials(dialog_instance)
            logger.log(DEBUG,f"Credentials >> User: {creds['user']} - Password: {creds['pswd']}")
            logger.log(DEBUG,'TODO: Implement this option')
            return code
        else:
            logger.log(DEBUG,'TODO: Implement this option')
            return code
    else:
        return code    


d = Dialog(dialog="dialog",autowidgetsize=True)
d.set_background_title('CouchDB CLI GUI Tool')

######################
### Main Algorythm ###
######################
choice = ""
code = ""
creds = {}

while choice not in ['Q'] and code not in ['esc','cancel']:

    code, choice = d.menu("Select what you want to do", width=50,
        choices=[
            ("H", "Set CouchDB host to connect to"),
            ("C", "Set CouchDB Credentials"),
            ("T", "Connectivity test"),
            ("D", "Database Menu"),
            ("M", "Document Menu"),
            ("U", "User Menu"),
            ("Q", "Quit")
        ]
    )
    logger.log(DEBUG, f'code: {code} - choice: {choice}')

    if choice.upper() == 'C':
        creds = get_db_credentials(d)
    elif choice.upper() == 'H':
        if "user" in creds.keys():
            logger.log(DEBUG,"Credentials set")
            couch  = get_couch(d,creds)
        else:
            logger.log(DEBUG,"Credentials NOT set")
            couch  = get_couch(d)
    ## Database Menu START
    elif choice.upper() == 'D':
        if couch  is None:
            couch  = get_couch(d,creds)
        while choice.upper() not in ['Q','M']:
            code, choice = database_menu(d)
            if choice.upper() == 'C':
                code, db  = d.inputbox(text="Please enter the name of the database to create")
                if code == d.OK:
                    database = couchdblib.Database(couch, db)
                    d.msgbox(text=json.dumps(database.create(),indent=2))
                else:
                    logger.log(DEBUG,'TODO: Implement this option')
                choice = 'Z'
            elif choice.upper() == 'D':
                code, db = d.inputbox(text="Please enter the name of the database to delete")
                if code == d.OK:
                    database = couchdblib.Database(couch, db)
                    d.msgbox(text=json.dumps(database.delete(),indent=2))
                else:
                    logger.log(DEBUG,'TODO: Implement this option')
                choice = 'Z'
            elif choice.upper() == 'L':
                d.msgbox(text=json.dumps(couch.all_dbs(),indent=2))
                choice = 'Z'
            elif choice.upper() == 'S':
                d.msgbox(text=json.dumps(couch.dbs_info(),indent=2))
                choice = 'Z'
            elif choice.upper() == 'M':
                logger.log(DEBUG,'TODO: Going back to main menu')
    ## Database Menu END
    elif choice.upper() == 'M':
        if couch  is None:
            couch  = get_couch(d,creds)
        test_db_connect(d, couch)
    elif choice.upper() == 'T':
        if couch  is None:
            couch  = get_couch(d,creds)
        test_db_connect(d, couch)
    elif choice.upper() == 'U':
        result = users_menu(d)
        logger.log(DEBUG, f'Result {result}')
if choice in ['Q']:
    # tmp = subprocess.call('clear',shell=True)
    print('Thanks for using this tool')