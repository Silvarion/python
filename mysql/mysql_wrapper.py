#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mysql_wrapper.py: 
  Script for running MySQL commands/scripts in one or more databases

Requirements:
  - Python 3
  - mysql-connector-python
"""

__version__ = "1.0"
__author__ = "Jesus Alejandro Sanchez Davila"
__maintainer__ = "Jesus Alejandro Sanchez Davila"
__email__ = "silvarion@gmail.com"
__status__ = "Production"


#######################
### Argument Parser ###
#######################
import sys
import os
import mysql.connector
from mysql.connector import errorcode
from mysql.connector import FieldType
from argparse import ArgumentParser
from datetime import datetime
import getpass
from sys import exit
parser = ArgumentParser()
parser.add_argument('-d', '--databases', dest='dbList', nargs='+',
                    required=True, help='List of databases to connect to')
parser.add_argument('--schema', dest='dbSchema',
                    default='', help='Schema to connect to')
parser.add_argument('-P', '--port', dest='dbPort',
                    default=3306, help='Port to connect to the databases')
parser.add_argument('-u', '--user', dest='dbUser', required=True,
                    help='Username to connect to the databases')
parser.add_argument('-p', '--password', dest='askForPassword',
                    action='store_true', help='Username to connect to the databases')
parser.add_argument('--path-to-sql', dest='pathToSQL', default='',
                    help='Path to SQL scripts (Defaults to same directory)')
parser.add_argument('--create-user', dest='createUser', action='store_true',
                    help='Creates the user in all target databases.')
parser.add_argument('--drop-user', dest='dropUser', action='store_true',
                    help='Drops the user in all target databases.')
parser.add_argument("-v", "--verbosity", action="count", default=0)
# Mutually exclusive arguments
sar = parser.add_mutually_exclusive_group()
sar.add_argument('-s', '--script', dest='sqlScript',
                 required=False, help='Path to script to run')
sar.add_argument('-c', '--command', dest='sqlCommand',
                 required=False, help='Command to run')
sar.add_argument('--slave-status', dest='showSlaveStatus',
                 required=False, action='store_true', help='Show Slave Status')

path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if not path in sys.path:
    sys.path.insert(1, path)
del path

#################
### Functions ###
#################

# logger
VALID_LOG_LEVELS = {'debug', 'log', 'info',
                    'notice', 'warning', 'error', 'critical'}


def log(level, msg):
    """
    log:
      Function to print timestamp and log level along with a message
    """
    if level not in VALID_LOG_LEVELS:
        raise ValueError("results: status must be one of %r." %
                         VALID_LOG_LEVELS)

    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print('[', ts, ']', '[', level.upper(), '] ', msg, sep='')

# Connect to the database


def connect(p_user, p_password, p_host="localhost", p_port="3306", p_database="mysql"):
    """
    connect:
      Function that tries the connection to a database and returns the connection object
    """
    cnx = None
    try:
        cnx = mysql.connector.connect(
            user=p_user, password=p_password, host=p_host, database=p_database)
        if args.verbosity > 3:
            log('debug', "Connected to the " + p_database +
                " database as the " + p_user + " user")
        return cnx
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            log('critical', "Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            log('critical', "Database does not exist")
        else:
            log('critical', err)
    finally:
        if cnx:
            return cnx
        else:
            return None

# Close conections


def disconnect(cnx):
    """
    disconnect:
      Function to cleanly disconnect and dump a given connection
    """
    try:
        if cnx.is_connected():
            cnx.close()
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            log('critical', "Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            log('critical', "Database does not exist")
        else:
            log('critical', err)
    finally:
        cnx.close()

# Execute command!


def execute(cnx, sql, showSlaveStatus=False):
    """
    execute:
      Given a connection and a SQL, this function executes the SQL in the database associated with the connection
      and returns either a result set or a notification of successful run.
    """
    try:
        cursor = cnx.cursor(buffered=True, dictionary=True)
        cursor.execute("SHOW SLAVE STATUS;")
        if cursor.rowcount > 0 and (sql.find("CREATE") == 0 or sql.find("DROP") == 0 or sql.find("ALTER") == 0 or sql.find("GRANT ") == 0 or sql.find("REVOKE") == 0 or sql.find("INSERT") == 0 or sql.find("DELETE") == 0 or sql.find("UPDATE") == 0 or sql.find("IMPORT") == 0):
            log('error', 'THIS SERVER IS A SLAVE, NO DDL/DML WILL BE RUN HERE!!! ONLY SELECTS ALLOWED!')
        else:
            if args.verbosity > 3:
                log('debug', 'Got a cursor from the conection')
            cursor.execute(sql)
            if args.verbosity > 3:
                log('debug', 'Executed statement')
            if cursor.with_rows:
                if args.verbosity > 3:
                    log('debug', 'SELECT statement detected!!')
                rows = cursor.fetchall()
                # Calculate Max Length
                if args.verbosity > 3:
                    log('debug', 'Calculating Max Length for each column')
                maxLength = {}
                for column in cursor.column_names:
                    maxLength[column] = len(column)
                if args.verbosity > 2:
                    log('log', 'MaxLengths')
                    log('log', maxLength)
                for row in rows:
                    for column in cursor.column_names:
                        if len(str(row[column])) > maxLength[column]:
                            maxLength[column] = len(str(row[column]))
                if args.verbosity > 2:
                    log('log', 'MaxLengths')
                    log('log', maxLength)
                if not showSlaveStatus:
                    # Print headers
                    if args.verbosity > 3:
                        log('debug', 'Printing Headers')
                    for column in cursor.column_names:
                        print(column.ljust(maxLength[column] + 2), end="")
                    print("")
                    # Print separator
                    if args.verbosity > 3:
                        log('debug', 'Printing Separator')
                    for column in cursor.column_names:
                        for j in range(maxLength[column] + 2):
                            print("-", end="")
                    print("")
                    # Print rows
                    for row in rows:
                        for column in cursor.column_names:
                            desc = cursor.description[cursor.column_names.index(
                                column)]
                            if FieldType.get_info(desc[1]) in ['BIGINT', 'DECIMAL', 'DOUBLE', 'FLOAT', 'INT', 'LONGLONG', 'NEWDECIMAL']:
                                if args.verbosity > 3:
                                    log('debug', 'Current field is ' +
                                        FieldType.get_info(desc[1]))
                                print(str(row[column]).rjust(
                                    maxLength[column] + 2), end="")
                            else:
                                if args.verbosity > 3:
                                    log('debug', 'Current field is ' +
                                        FieldType.get_info(desc[1]))
                                print(str(row[column]).ljust(
                                    maxLength[column] + 2), end="")
                        print("")
                else:  # showSlaveStatus
                    for row in rows:
                        for column in ['Slave_IO_State', 'Master_Host', 'Master_User', 'Master_Port', 'Master_Log_File', 'Read_Master_Log_Pos', 'Relay_Log_File', 'Relay_Log_Pos', 'Relay_Master_Log_File', 'Slave_IO_Running', 'Slave_SQL_Running', 'Last_Errno', 'Skip_Counter', 'Exec_Master_Log_Pos', 'Relay_Log_Space', 'Until_Condition', 'Until_Log_Pos', 'Seconds_Behind_Master', 'Last_IO_Errno', 'Last_SQL_Errno']:
                            print(column.rjust(max(maxLength.values())) +
                                  ": " + str(row[column]))
            else:
                if args.verbosity > 3:
                    log('debug', 'Not a select statement')
                log('info',
                    f'Statement executed. {cursor.rowcount} rows affected!')
    except mysql.connector.Error as err:
        if args.verbosity > 3:
            log('debug', 'Catched exception while executing')
        log('critical', err.errno)
        log('critical', err.sqlstate)
        log('critical', err.msg)
    finally:
        if args.verbosity > 3:
            log('debug', 'Closing the cursor')
        cursor.close()


def create_user(user_type=None, user_name=None, ip_address=None, user_pswd=None, environment=None):
    log('info', 'Gathering user information')
    while not environment or environment.lower() not in ['dev', 'uat', 'prd']:
        environment = input(
            '[INPUT] Please enter the environment (dev / uat / prd): ')
    while user_type not in ['a', 'A', 'd', 'D', 'q', 'Q']:
        user_type = input(
            '[INPUT] Is the user an (a)dmin, (d)eveloper or (q)c : ')
        if user_type in ['a', 'A']:
            user_prefix = 'adm'
            filename = f'{args.pathToSQL}/{user_prefix}_role_template.sql'
        else:
            if user_type in ['d', 'D']:
                user_prefix = 'dev'
            elif user_type in ['q', 'Q']:
                user_prefix = 'qc'
            filename = f'{args.pathToSQL}/{environment}_{user_prefix}_role_template.sql'

    print(f'[NOTICE] The prefix {user_prefix} will be automatically added')
    if not user_name:
        user_name = input('[INPUT] Please enter the username: ')
    if not user_pswd:
        user_pswd = getpass.getpass(
            prompt='[INPUT] Please enter the password: ')
    if not ip_address:
        ip_address = input(
            '[INPUT] Please enter the IP address from which this user can connect: ')
    commands = f"CREATE USER '{user_prefix}_{user_name}'@'{ip_address}' IDENTIFIED BY '{user_pswd}';\n"
    with open(filename) as grants_file:
        nl = '\n'
        for line in grants_file.readlines():
            if len(line) > 1 and line.find('--') == -1:
                if user_type in ['a', 'A']:
                    if line.find('REVOKE') == -1:
                        commands += f"{line.strip(nl)} '{user_prefix}_{user_name}'@'{ip_address}' WITH GRANT OPTION;\n"
                else:
                    commands += f"{line.strip(nl)} '{user_prefix}_{user_name}'@'{ip_address}';\n"
    commands += "FLUSH PRIVILEGES;\n"
    return commands


def drop_user(user_type=None, user_name=None, ip_address=None):
    log('info', 'Gathering user information')
    while user_type not in ['a', 'A', 'd', 'D']:
        user_type = input(
            '[INPUT] Is the user a (d)eveloper or an (a)dministrator: ')
        if user_type in ['a', 'A']:
            user_prefix = 'adm'
        elif user_type in ['d', 'D']:
            user_prefix = 'dev'

    print(f'[NOTICE] The prefix {user_prefix} will be automatically added')
    if not user_name:
        user_name = input('[INPUT] Please enter the username: ')
    if not ip_address:
        ip_address = input(
            '[INPUT] Please enter the IP address from which this user can connect: ')
    command = f"DROP USER '{user_prefix}_{user_name}'@'{ip_address}';"
    return command


### Main Algorithm Start ###
args = parser.parse_args()

# Check Arguments
if args.verbosity > 0:
    log('log', 'Checking arguments')

if args.askForPassword:
    if args.verbosity > 1:
        log('log', 'Asking for password to connect to the DB')
    dbPswd = getpass.getpass(prompt='Please enter the database password: ')

if args.verbosity > 3:
    log('debug', 'Checking if the DB List is a file')
# Build DB List
dbList = []
for i in range(len(args.dbList)):
    if args.verbosity > 3:
        log('debug', 'DB List has only 1 item')
    if os.path.exists(args.dbList[i]):
        if args.verbosity > 3:
            log('debug', 'DB List is a file and it exists')
        fd = open(args.dbList[i], 'r')
        if args.verbosity > 3:
            log('debug', 'Populating dbList with file contents')
        db = fd.readline()
        while db:
            dbList.append(db)
            db = fd.readline()
    else:
        if args.verbosity > 3:
            log('debug', 'Checking if the DB List is a file')
        dbList.append(args.dbList[i])

if args.createUser:
    commands = create_user()
    tempScript = open('tempScript.tmp', 'w')
    tempScript.write(commands)
    tempScript.close()
    args.sqlScript = 'tempScript.tmp'

if args.dropUser:
    args.sqlCommand = drop_user()

# Start Crawling
for dbHost in dbList:
    print("")
    log('info', 'Connecting to ' + dbHost)
    cnx = connect(p_user=args.dbUser, p_password=dbPswd,
                  p_host=dbHost, p_port=args.dbPort, p_database=args.dbSchema)
    if cnx:
        if args.verbosity > 0:
            log('notice', 'Checking SQL')
    # If a SQL Script is provided
        if args.sqlScript:
            if args.verbosity > 0:
                log('notice', 'Executing as a script, 1 sentence at a time')
            script = open(args.sqlScript, "r")
            if args.verbosity > 3:
                log('debug', 'Reading first SQL line')
            preparedSql = script.readline().replace("\n", " ")
            while preparedSql:
                while preparedSql.find(';') == -1:
                    if args.verbosity > 3:
                        log('debug', 'Reading SQL lines until ; is found')
                    preparedSql = str(preparedSql) + \
                        script.readline().replace("\n", " ")
                    if args.verbosity > 3:
                        log('debug', 'preparedSql is: ' + preparedSql)
                if args.verbosity > 0:
                    log('log', 'Executing --> ' + preparedSql)
                execute(cnx=cnx, sql=preparedSql)
                print("")
                preparedSql = script.readline()
    # If a SQL statement/command is provided
        elif args.sqlCommand:
            preparedSql = args.sqlCommand
            if args.verbosity > 3:
                log('debug', 'Set preparedSql to ' + preparedSql)
            execute(cnx=cnx, sql=preparedSql)
    # If nothing is provided to run
        elif args.showSlaveStatus:
            log('info', 'Getting slave status for ' + dbHost)
            execute(cnx=cnx, sql="SHOW SLAVE STATUS;", showSlaveStatus=True)
        else:
            log('warning', 'No script or command was provided, this will only test a connection')
            preparedSql = "SELECT concat('Connected with user ', user(), ' to ', @@hostname) AS connection_test;"
            execute(cnx=cnx, sql=preparedSql)
        if args.verbosity > 1:
            log('log', 'Closing connection to the DB')
        disconnect(cnx)
    else:
        if args.verbosity > 0:
            log('log', f'Unable to connect to {dbHost}!')
if args.createUser:
    os.remove('tempScript.tmp')
