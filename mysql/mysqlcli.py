#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mysqlcli.py: 
  Very simple CLI-UI client to interact with MySQL databases using mysqllib

Requirements:
  - Python 3
  - mysqllib
  - pythondialog
"""

__version__     = "1.0"
__author__      = "Jesus Alejandro Sanchez Davila"
__maintainer__  = "Jesus Alejandro Sanchez Davila"
__email__       = "jsanchez.consultant@gmail.com"
__status__      = "Alpha"

import mysqllib
from argparse import ArgumentParser
from datetime import datetime
import json
import getpass
import logging
import os
import sys
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

### Argument Parser ###
parser = ArgumentParser()
parser.add_argument('-d', '--databases', dest='dbList', nargs='+', help='List of databases to connect to')
parser.add_argument('--schema', dest='dbSchema', default='', help='Schema to connect to')
parser.add_argument('-P', '--port', dest='dbPort', default=3306, help='Port to connect to the databases')
parser.add_argument('-u', '--user', dest='dbUser', help='Username to connect to the databases')
parser.add_argument('-p', '--password', dest='askForPassword', action='store_true', help='Username to connect to the databases')
parser.add_argument("-v", "--verbosity", action="count", default=0)
# Mutually exclusive arguments
sar = parser.add_mutually_exclusive_group()
sar.add_argument('-s', '--script', dest='sqlScript', required=False, help='Path to script to run')
sar.add_argument('-c', '--command', dest='sqlCommand', required=False, help='Command to run')
sar.add_argument('--slave-status', dest='showSlaveStatus', required=False, action='store_true', help='Show Slave Status')

# Instantiate Logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

### Functions ###

## Main Menu ##
def main_menu(dialog_instance):
    code, choice = d.menu("Main Menu",
        choices=[
            ("D", "Database"),
            ("S", "Schema"),
            ("U", "User"),
            ("Q", "Quit")
        ]
    )
    return code, choice

### Databases Menu
def databases_menu(dialog_instance):
    return d.menu("Database Menu",height=18,width=50,menu_height=15,
        choices=[
            ("A", "Add target to database list"),
            ("L", "List database targets"),
            ("R", "Remove database targets"),
            ("X", "Clear targets list"),
            ("C", "Set credentials"),
            ("P", "Set default port"),
            ("S", "Show Schemas"),
            ("T", "Connection Test"),
            ("B", "Back to main menu"),
            ("Q", "Quit")
        ]
    )

## Databases Actions
def add_target(dialog_instance, db_dict):
    choice = ""
    code = ""
    while code not in ["esc","cancel"] and choice not in ["B","D","Q"]:
        code, choice = d.menu("Add target(s) Menu",
            choices=[
                ("M", "Add manually"),
                ("R", "Read from file"),
                ("D", "Back to database menu"),
                ("Q", "Quit")
            ]
        )
        if choice == 'M':
            # Show form
            code, inputs = d.form(
                text="Target database",
                elements=(
                    # (label, yl, xl, item, yi, xi, field_length, input_length)
                    ("Hostname/IP address",1,1,"host",1,30,25,40),
                    ("Port",2,1,"3306",2,30,25,4)
                )
            )
            logger.log(DEBUG, f"Inputs {inputs}")
            db_dict[inputs[0]] = inputs[1]
        elif choice == 'R':
            # Show file chooser
            code, path = d.fselect(filepath=os.path.abspath(os.path.join(os.path.dirname(__file__), '..')),height=30,width=80)
            logger.log(DEBUG, f"Selected PATH --> {path}")
            with open(path,'r') as fd:
                line = fd.readline()
                while line:
                    if line.find(':') == -1:
                        db_dict[line] = default_port
                    else:
                        data = line.split(':')
                        db_dict[data[0]] = data[1]
                    line = fd.readline()
    if code in ["esc", "cancel"]:
        code = ""

def clear_targets(dialog_instance, databases):
    databases.clear()

def list_targets(dialog_instance, databases):
    dialog_txt = ""
    logger.log(DEBUG, f"Databases dictionary keys: {databases.keys()}")
    for target in databases.keys():
        dialog_txt += f"{target}:{databases[target]}\n"
    d.scrollbox(text=dialog_txt,height=30,width=70)

def remove_target(dialog_instance, databases):
    counter = 1
    target_list = []
    for key in databases.keys():
        target_list.append((str(counter),key))
        counter += 1
    code, target_tag = dialog_instance.menu(text="Select target to remove",
        choices=target_list
    )
    if code == dialog_instance.OK:
        target, host = target_list[int(target_tag)-1]
        logger.log(DEBUG, f"{target}")
        databases.pop(host)

def read_targets_file(dialog_instance):
    d = dialog_instance
    dbList = []
    code, source = d.inputbox(
        text = "Please enter DB hostname or path to file containing a list of hostnames",
        width = 80
    )
    if code == d.OK:
        if os.path.exists(source):
            logger.log(DEBUG, 'DB List is a file and it exists')
            fd = open(source,'r')
            logger.log(DEBUG, 'Populating dbList with file contents')
            db = fd.readline()
            while db:
                dbList.append(db.strip('\n'))
                db = fd.readline()
        else:
            logger.log(DEBUG, 'Checking if the DB List is a file')
            source_list = source.replace(' ',',').split(',')
            for i in range(len(source_list)):
                logger.log(DEBUG, 'DB List has only 1 item')
                dbList.append(source_list[i])
    return dbList

def test_connection(dialog_instance, databases, creds):
    dialog_txt = ""
    for host in databases.keys():
        db = mysqllib.Database(host,databases[host])
        db.connect(username=creds['user'],password=creds['pswd'])
        if db.is_connected():
            status = "Success"
        else:
            status = "Failed"
        dialog_txt += f"{host} connection test: {status}\n"
        db.disconnect()
    dialog_instance.msgbox(text=dialog_txt,width=70,height=30)

def list_schemas(dialog_instance, databases):
    for host in databases:
        db = mysqllib.Database(host,databases[host])
        db.connect(username=creds['user'],password=creds['pswd'])
        schemas = db.get_schemas()
        db.disconnect()
        dialog_txt = "Schema name         - Character Set - Collation\n"
        dialog_txt += "=================== - ============= - ==================\n"
        for item in schemas:
            dialog_txt += item['schema_name'].ljust(20)
            dialog_txt += " - "
            dialog_txt += item['charset'].ljust(13)
            dialog_txt += " - "
            dialog_txt += f"{item['collation']}\n"
        dialog_instance.msgbox(text=dialog_txt,height=30,width=70)

## Schema Menu
def schemas_menu(dialog_instance):
    return d.menu("Schema Menu",
        choices=[
            ("C", "Create"),
            ("D", "Drop"),
            ("T", "Show Tables"),
            ("B", "Back to main menu"),
            ("Q", "Quit")
        ]
    )

## Users Menu
def users_menu(dialog_instance):
    return d.menu("User Menu",
        choices=[
            ("C", "Create"),
            ("D", "Drop"),
            ("L", "List"),
            ("G", "Show grants"),
            ("B", "Back to main menu"),
            ("Q", "Quit")
        ]
    )

## Getters and Setters ##

# Get credentials
def set_credentials(d):
    creds = {}
    creds['user'] = set_user(d)
    creds['pswd'] = set_password(d)
    return creds

def set_user(dialog_instance):
    d = dialog_instance
    code, value = d.inputbox(
        text = "Please input your DB username",
    )
    if code == d.OK:
        return value

def set_password(dialog_instance):
    code, value = d.passwordbox(
        text = "Please input your DB password",
        insecure = True
    )
    if code == d.OK:
        return value

def set_default_port(dialog_instance):
    d = dialog_instance
    code, value = d.inputbox(
        text = "Please input the default port",
    )
    if code == d.OK:
        return value
    

### Main Algorith ###
parser.parse_args()
d = Dialog(dialog="dialog")
d.set_background_title("MySQL CLI UI")
# Global Variables
creds = {
    "user": "",
    "pswd": ""
}
databases = {}
default_port = 3306
schema = ""
user = ""

choice = ""
code = ""
while code not in ['esc','cancel'] and choice != 'Q':
    code, choice = main_menu(d)
    if code == d.OK:
        logger.log(DEBUG, f"User selected {choice}")
        if choice == 'D':
            # Database menu
            while code not in ['esc','cancel'] and choice not in ['Q','B']:
                code, choice = databases_menu(d)
                logger.log(DEBUG, f"User selected {choice}")
                if choice == 'A': # Add Databases
                    add_target(d,databases)
                elif choice == 'L': # List Targets
                    list_targets(d, databases)
                elif choice == 'R': # Remove Target
                    remove_target(d, databases)
                elif choice == 'X': # Clear Targets
                    clear_targets(d, databases)
                elif choice == 'C': # Set Credentials
                    creds = set_credentials(d)
                elif choice == 'P': # Set Default port
                    default_port = set_default_port(d)
                elif choice == 'S': # Show Schemas
                    list_schemas(d,databases)
                elif choice == 'T': # Test Connection
                    if creds['user'] == "" or creds['pswd'] == "":
                        creds = set_credentials(d)
                    test_connection(d,databases,creds)
            if code in ['cancel','esc']:
                code = 'B'
        elif choice == 'S':
            # Schemas
            while code not in ['esc','cancel'] and choice not in ['Q','B']:
                code, choice = schemas_menu(d)
                logger.log(DEBUG, f"User selected {choice}")
            if code in ['cancel','esc']:
                code = 'B'
        elif choice == 'U':
            while code not in ['esc','cancel'] and choice not in ['Q','B']:
                code, choice = users_menu(d)
                logger.log(DEBUG, f"User selected {choice}")
            if code in ['cancel','esc']:
                code = 'B'
    else:
        print("Thanks for using this program")