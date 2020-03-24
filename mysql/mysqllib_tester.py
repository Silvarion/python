#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mysql_wrapper.py: 
  Script for running MySQL commands/scripts in one or more databases

Requirements:
  - Python 3
  - mysql-connector-python
"""

__version__     = "1.0"
__author__      = "Jesus Alejandro Sanchez Davila"
__maintainer__  = "Jesus Alejandro Sanchez Davila"
__email__       = "jsanchez.consultant@gmail.com"
__status__      = "Alpha"

import mysqllib
from argparse import ArgumentParser
from getpass import getpass
import json
import os
import sys

# Add current path
path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
  sys.path.insert(1, path)
del path

# Argument Parser
parser = ArgumentParser()
parser.add_argument('-d', '--databases', dest='dbList', nargs='+', required=True, help='List of databases to connect to')
parser.add_argument('--schema', dest='dbSchema', default='', help='Schema to connect to')
parser.add_argument('-P', '--port', dest='dbPort', default=3306, help='Port to connect to the databases')
parser.add_argument('-u', '--user', dest='dbUser', required=True, help='Username to connect to the databases')
parser.add_argument('-p', '--password', dest='askForPassword', action='store_true', help='Username to connect to the databases')
parser.add_argument("-v", "--verbosity", action="count", default=0)

args = parser.parse_args()

dbPswd = 'admin'
# Main algorythm
for db_host in args.dbList:
    db1 = mysqllib.Database(hostname=db_host, port=args.dbPort)
    if args.askForPassword:
        dbPswd = getpass(prompt='Please enter the password: ')
    db1.connect(username=args.dbUser,password=dbPswd)
    # print(db.is_connected())
    # print(json.dumps(db.get_schemas(),indent=2))
    # schema = mysqllib.Schema(database=db1, name='mydb')
    # print(db.get_schema('mysql'))
    # print(f"Schema info >>> Name: {schema.name} Charset: {schema.charset} Collation: {schema.collation}")
    # print(json.dumps(db.get_users(), indent=2))
    # print(json.dumps(db.get_user('jsanchez'), indent=2))
    # print(json.dumps(schema.get_tables(), indent=2))
    # print(json.dumps(schema.get_table('threads'), indent=2))
    schemas = db1.get_schemas()
    # print(schemas)
    db2 = mysqllib.Database(hostname='hekkamiahmsv1c.mylabserver.com', port=args.dbPort)
    db2.connect(username=args.dbUser,password=dbPswd)
    diff_dict = {}
    # for item in schemas:
    #     if item['schema_name'].lower() not in ['mysql','information_schema','performance_schema','test','mydb','tgops','percona']:
    #         sch = mysqllib.Schema(database=db1,name=item['schema_name'])
    #         diff_dict[item['schema_name']] = sch.compare(db2)
    # denuvo = mysqllib.Schema(db1,'uplay_denuvo')
    # print(json.dumps(denuvo.compare(database=db2),indent=2))
    print(json.dumps(diff_dict,indent=2))
    db1.disconnect()
    db2.disconnect()