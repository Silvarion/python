#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
mysql_wrapper.py: 
  Module to interact with MySQL databases in a Object-Oriented way

Requirements:
  - Python 3
  - mysql-connector-python
"""

__version__     = "1.0"
__author__      = "Jesus Alejandro Sanchez Davila"
__maintainer__  = "Jesus Alejandro Sanchez Davila"
__email__       = "jsanchez.consultant@gmail.com"
__status__      = "Alpha"

import mysql.connector
from mysql.connector import errorcode
from mysql.connector import FieldType
from argparse import ArgumentParser
from datetime import datetime
from datetime import timedelta
from time import sleep
import getpass
import logging
from logging import DEBUG
from logging import CRITICAL
from logging import ERROR
from logging import FATAL
from logging import INFO
from logging import WARNING

logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()
    
# Database class
class Database:
    # Attributes
    connection = None
    
    # Creator
    def __init__(self, hostname, port=3306, database='information_schema', log_level=logging.INFO):
        self.hostname = hostname
        self.port = port
        self.schema = database
        logger.setLevel(log_level)

    # Methods
    def connect(self, username, password, schema='information_schema'):
        cnx = None
        try:
            cnx = mysql.connector.connect(user=username, password=password, host=self.hostname, database=self.schema)
            logger.log(DEBUG,f'Database {self.schema} on {self.hostname} connected')
            self.username = username
            self.password = password
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.log(CRITICAL, "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.log(CRITICAL, "Database does not exist")
            else:
                logger.log(CRITICAL, err)
        finally:
            if cnx:
                self.connection = cnx


    def disconnect(self):
        try:
            if self.connection:
                self.connection.close()
                logger.log(INFO,f'Database {self.schema} on {self.hostname} disconnected')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.log(CRITICAL, "Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.log(CRITICAL, "Database does not exist")
            else:
                logger.log(CRITICAL, err)
        finally:
            self.connection = None

    def reconnect(self):
        if "username" in dir(self) and "password" in dir(self):
            self.connect(username=self.username, password=self.password)

    def execute(self,command):
        self.reconnect()
        resultset = {}
        if self.is_connected():
            # logger.log(DEBUG, 'Database is connected. Trying to create cursor')
            try:
                cursor = self.connection.cursor(buffered=True,dictionary=True)
                # logger.log(DEBUG, 'Cursor created')
                # logger.log(DEBUG, f'command: {command}')
                sql = f"{command.strip(';')};"
                # logger.log(DEBUG, f'sql: "{sql}"')
                timer_start = datetime.now()
                cursor.execute(sql)
                timer_end = datetime.now()
                timer_elapsed = timer_end - timer_start
                # logger.log(DEBUG, 'Command executed')
                resultset = {
                    'rows': []
                }
                if command.upper().find("SELECT") == 0:
                    rows = cursor.fetchall()
                    # logger.log(DEBUG, f'Fetched {cursor.rowcount} rows')
                    columns = cursor.column_names
                    for row in rows:
                        row_dic = {}
                        for column in columns:
                            row_dic[column] = row[column]
                        resultset['rows'].append(row_dic)
                    resultset["action"] = "SELECT"
                    resultset["rowcount"] = cursor.rowcount
                    resultset["start_time"] = f"{timer_start.strftime('%Y-%m-%d %H:%M:%S')}"
                    resultset["exec_time"] = f"{timer_elapsed.total_seconds()}"
                else:
                    logger.debug(f"RESULTSET:\n{cursor}")
                    resultset["action"] = command.upper().split(" ")[0]
                    resultset["rowcount"] = cursor.rowcount
                    resultset["start_time"] = f"{timer_end.strftime('%Y-%m-%d %H:%M:%S')}"
                    resultset["exec_time"] = f"{timer_elapsed.total_seconds()}"
            except mysql.connector.Error as err:
                logger.log(WARNING, 'Catched exception while executing')
                logger.log(CRITICAL, err.errno)
                logger.log(CRITICAL, err.sqlstate)
                logger.log(CRITICAL, err.msg)
            except Exception as e:
                logger.log(WARNING, 'Catched exception while executing')
                logger.log(CRITICAL, e)
            finally:
                return resultset
        else:
            logger.log(ERROR,'Please connect first, then try again')
    
    ## Schema methods
    def load_schemas(self):
        self.schemas = self.execute('SELECT schema_name, default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata')['rows']

    def get_schemas(self):
        return self.execute('SELECT schema_name, default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata')['rows']
    
    def get_schema(self, schema_name):
        result = self.execute(f'SELECT schema_name, default_character_set_name AS charset, default_collation_name as collation FROM information_schema.schemata WHERE schema_name = \'{schema_name}\'')['rows']
        if len(result) > 0:
            logger.log(DEBUG, f'Schema {schema_name} found. Returning {result}')
            return result
        else:
            logger.log(DEBUG, f'Schema {schema_name} not found. Returning None')
            return None
    
    ## User Methods
    def create_user(self, username, host = '%', password = ''):
        result = self.execute(f"CREATE user '{username}'@'{host}' IDENTIFIED BY '{password}'")
        logger.log(DEBUG,f'{result}')
        result = self.execute(f"GRANT USAGE ON *.* TO '{username}'@'{host}'")
        logger.log(DEBUG,f'{result}')
        
    def get_user(self, user):
        return self.execute(f"SELECT user, host FROM mysql.user WHERE user = '{user}';")

    def get_users(self):
        return self.execute(f"SELECT user, host FROM mysql.user;")

    def dump(self):
        return None

    # Check if there's an active connection to the database
    def is_connected(self):
        if self.connection:
            return True
        else:
            return False

# Schema class
class Schema:
    def __init__(self, database, name):
        self.database = database
        schema = self.database.get_schema(name)
        if schema and len(schema) > 0:
            self.name = schema[0]['schema_name']
            self.charset = schema[0]['charset']
            self.collation = schema[0]['collation']
            self.tables = []
        else:
            self.name = 'NotFound'

    def load_tables(self):
        self.tables = self.get_tables()

    def get_tables(self):
        result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' ORDER by 1,2")
        tables = {}
        for row in result['rows']:
            tables[f"{row['schema_name']}.{row['table_name']}"] = {
                'schema_name': row['schema_name'],
                'table_name': row['table_name'],
                'table_type': row['table_type'],
                'table_rows': row['table_rows'],
                'avg_row_length': row['avg_row_length'],
                'max_data_length': row['max_data_length']
            }
        # logger.log(DEBUG, f"Tables is: {tables}")
        for table in tables.keys():
            tables[table]['columns'] = table.get_columns()
        return tables
    
    def get_table(self, table_name):
        result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' AND table_name = '{table_name}' ORDER by 1,2")
        table = {}
        if len(result) > 0:
            table[f"{result['schema_name']}.{result['table_name']}"] = result[0]
        # logger.log(DEBUG, f"Table is: {table}")
        table_obj = Table(self, table_name)
        table['columns'] = table_obj.get_columns()
        return table

    def compare(self, schema, gen_fix_script=False):
        # Check there is a valid connection in both databases
        logger.log(DEBUG, f'Checking connectivity to {self.database.hostname} and {schema.database.hostname}')
        if self.database.is_connected() and schema.database.is_connected():
        # Check that the schema exists in both databases
            logger.log(DEBUG, 'Creating schema objects for both databases')
            local_schema = self
            remote_schema = schema
            if remote_schema.name != 'NotFound':
                logger.log(DEBUG, f'Remote Schema is: {remote_schema.name}')
                # Get colunms definitions and compare
                local_schema.load_tables()
                # logger.log(DEBUG, f'Local Schema Tables: {local_schema.tables}')
                remote_schema.load_tables()
                # logger.log(DEBUG, f'Remote Schema Tables: {remote_schema.tables}')
                diff_dict = {
                    'differences': [],
                    'fix_commands': []
                }
                for table_entry in local_schema.tables.keys():
                    if table_entry in remote_schema.tables.keys():
                        logger.log(DEBUG, f"Checking table {table_entry}")
                        for column in local_schema.tables[table_entry]['columns'].keys():
                            if column in remote_schema.tables[table_entry]['columns'].keys():
                                # logger.log(DEBUG, f"Checking column {column}")
                                for key in local_schema.tables[table_entry]['columns'][column].keys():
                                    if key != 'ordinal_position':
                                        if local_schema.tables[table_entry]['columns'][column][key] != remote_schema.tables[table_entry]['columns'][column][key]:
                                            fix_command = f"ALTER TABLE {table_entry} MODIFY COLUMN "
                                            fix_command += f"{column} {local_schema.tables[table_entry]['columns'][column]['column_type']}"
                                            if local_schema.tables[table_entry]['columns'][column]['is_nullable'] == 'NO':
                                                fix_command += ' NOT NULL'
                                            if local_schema.tables[table_entry]['columns'][column]['column_default'] is not None:
                                                if local_schema.tables[table_entry]['columns'][column]['data_type'] == 'varchar':
                                                    fix_command += f" DEFAULT '{local_schema.tables[table_entry]['columns'][column]['column_default']}'"
                                                else:
                                                    fix_command += f" DEFAULT {local_schema.tables[table_entry]['columns'][column]['column_default']}"
                                            fix_command += ";"
                                            diff_dict['differences'].append({
                                                table_entry: {
                                                    column:{
                                                        local_schema.database.hostname: {
                                                            key: local_schema.tables[table_entry]['columns'][column][key]
                                                        },
                                                        remote_schema.database.hostname: {
                                                            key: remote_schema.tables[table_entry]['columns'][column][key]
                                                        }
                                                    }
                                                }
                                            })
                                            diff_dict['fix_commands'].append(fix_command)
                            else:
                                fix_command = f"ALTER TABLE {table_entry} ADD COLUMN "
                                fix_command += f"{column} {local_schema.tables[table_entry]['columns'][column]['column_type']}"
                                if local_schema.tables[table_entry]['columns'][column]['is_nullable'] == 'NO':
                                    fix_command += ' NOT NULL'
                                if local_schema.tables[table_entry]['columns'][column]['column_default'] is not None:
                                    if local_schema.tables[table_entry]['columns'][column]['data_type'] == 'varchar':
                                        fix_command += f" DEFAULT '{local_schema.tables[table_entry]['columns'][column]['column_default']}'"
                                    else:
                                        fix_command += f" DEFAULT {local_schema.tables[table_entry]['columns'][column]['column_default']}"
                                fix_command += ";"
                                diff_dict['differences'].append({
                                    table_entry: {
                                        column:{
                                            local_schema.database.hostname: {
                                                'column_exists': True
                                            },
                                            remote_schema.database.hostname: {
                                                'column_exists': False
                                            }
                                        }
                                    }
                                })
                                diff_dict['fix_commands'].append(fix_command)
                    else:
                        diff_dict['differences'].append({
                            self.name: {
                                local_schema.database.hostname: {
                                    'schema_exists': True
                                },
                                remote_schema.database.hostname: {
                                    'schema_exists': False
                                }
                            }
                        })
                return diff_dict
        # TO-DO: Get functions definitions and compare
        return None

# Table class
class Table:
    def __init__(self, schema, name):
        self.name = name
        self.schema = schema
        self.database = schema.database
        self.fqn = f"{self.schema.name}.{self.name}"
    
    def get_columns(self):
        # logger.log(DEBUG, f"Table is: {table_name}")
        result = self.database.execute(f"SELECT column_name, ordinal_position, column_default, is_nullable, data_type, column_type, character_set_name, collation_name FROM information_schema.columns WHERE table_schema = '{self.schema.name}' AND table_name = '{self.name}' ORDER BY ordinal_position")
        column_dict = {}
        for column in result['rows']:
            # logger.log(DEBUG, column)
            column_dict[column['column_name']] = {
                'ordinal_position': column['ordinal_position'],
                'column_default': column['column_default'],
                'is_nullable': column['is_nullable'],
                'data_type': column['data_type'],
                'column_type': column['column_type'],
                'charset': column['character_set_name'],
                'collation': column['collation_name'],
            }
        return column_dict

    def get_rowcount(self):
        logger.log(DEBUG,'Getting ROWCOUNT')
        return self.database.execute(f'SELECT count(1) AS rowcount FROM {self.fqn}')['rows'][0]

    def get_insert_statement(self, columns: list = None, values: list = None):
        if columns is None:
            columns = list(self.get_columns().keys())
        sql = f"INSERT INTO {self.fqn} ({columns})"
        for value in values:
            if type(value) == 'datetime':
                sql += f"'{value.isoformat().split('.')[0].replace('T',' ')}',"
            if type(value) == 'str':
                sql += f"{value},"
            else:
                sql += f"{value},"
        sql = sql.strip(",")
        sql += ")"
            
        return sql

    def insert(self, values: list):
        None
    
    def delete(self, rows: list, batch_size = 1, delay = 0):
        full_result = {
            "rows": []
        }
        if len(rows) == 0:
            logger.log("No IDs found!")
        if batch_size == 1:
            for row in rows:
                conditions = []
                for column in row.keys():
                    conditions.append((column,row[column]))
                for condition in conditions:
                    if type(condition[1]) is str:
                        logger.debug(f"Full command: DELETE FROM {self.fqn} WHERE {condition[0]} = '{condition[1]}'; COMMIT;")
                        result = self.database.execute(command=f"DELETE FROM {self.fqn} WHERE {condition[0]} = '{condition[1]}'; COMMIT;")
                    else:
                        logger.debug(f"Full command: DELETE FROM {self.fqn} WHERE {condition[0]} = {condition[1]}; COMMIT;")
                        result = self.database.execute(command=f"DELETE FROM {self.fqn} WHERE {condition[0]} = {condition[1]}; COMMIT;")
                    full_result["rows"].append(result)
        elif batch_size > 1:
            str_conditions = ""
            counter = 0
            for row in rows:
                conditions = []
                for column in row.keys():
                    conditions.append((column,row[column]))
                for condition in conditions:
                    str_conditions += f"{condition[1]},"
                counter+=1
                if counter == batch_size or row == rows[len(rows)-1]:
                    str_conditions = str_conditions[:(len(str_conditions)-1)]
                    cond_string = f"{condition[0]} IN ('" + str_conditions.replace(",","','") + "')"
                    full_command = f"DELETE FROM {self.fqn} WHERE {cond_string}; COMMIT;"
                    logger.debug(f"Full command: {full_command}")
                    result=self.database.execute(command=full_command)
                    counter=0
                    str_conditions = ""
                    full_result["rows"].append(result)
                if delay > 0:
                    sleep(delay)
        return(full_result)

    
    def update(self, id: list, columns: list, values: list):
        None

    def compare_data(self, table, batch_size=10000,print_to_console=False,fix_script=False,fix=False):
        column_names = ""
        if self.database.is_connected() and table.database.is_connected:
            logger.log(INFO,f'Comparing {self.fqn} on {self.database.hostname} and {table.database.hostname}')
            logger.log(DEBUG,'Connections verified, starting comparison')
            local_rowcount = self.get_rowcount()['rowcount']
            logger.log(DEBUG,f'Local Rows: {local_rowcount}')
            remote_rowcount = table.get_rowcount()['rowcount']
            logger.log(DEBUG,f'Remote Rows: {remote_rowcount}')
            if local_rowcount != remote_rowcount:
                logger.log(WARNING,f'Local and remote row counts differ!')
            processed = 0
            if fix_script:
                local_script = ""
                remote_script = ""
            while processed < local_rowcount or processed < remote_rowcount:
                local_result = self.database.execute(f'SELECT * FROM {self.schema.name}.{self.name} ORDER BY id LIMIT {processed}, {processed + batch_size}')['rows']
                # logger.log(DEBUG,local_result)
                remote_result = table.database.execute(f'SELECT * FROM {table.schema.name}.{table.name} ORDER BY id LIMIT {processed}, {processed + batch_size}')['rows']
                for index in range(batch_size):
                    if index < len(local_result) or index < len(remote_result):
                        if column_names == "":
                            for column in local_result[index].keys():
                                column_names += f"{column},"
                            column_names = column_names.strip(',')
                        if index < len(local_result):
                            local_row = local_result[index]
                        else:
                            local_row = None
                        if index < len(remote_result):
                            remote_row = local_result[index]
                        else: remote_row = None
                        if local_row is not None and remote_row is not None:
                            if  local_row != remote_row:
                                logger.debug("Difference catched")
                                if print_to_console:
                                    print(f'Conflict Local: {local_result[index]}')
                                    print(f'Conflict Remote: {remote_result[index]}')
                        else:
                            if local_row is not None:
                                if print_to_console:
                                    print(f'local: {local_result[index]}')
                                    print(f'Missing at {table.database.hostname}')
                                if fix_script:
                                    logger.debug('Adding line to remote script')
                                    values = ""
                                    for value in local_result[index].values():
                                        if type(value) == 'datetime':
                                            values += f"'{value.isoformat().split('.')[0].replace('T',' ')}',"
                                        if type(value) == 'str':
                                            values += f"{value},"
                                        else:
                                            values += f"{value},"
                                    values = values.strip(",")
                                    logger.debug(values)
                                    remote_script += f"\nINSERT INTO {self.schema.name}.{self.name} ({column_names}) VALUES ({values});"
                            if remote_row is not None:
                                if print_to_console:
                                    print(f'Missing at {self.database.hostname}')
                                    print(f'remote: {remote_result[index]}')
                                if fix_script:
                                    logger.debug('Adding line to local script')
                                    local_script += f"\nINSERT INTO {self.schema.name}.{self.name} ({remote_result[index].keys()}) VALUES ({remote_result[index].values()});"
                    else:
                        break
                logger.log(INFO,f'{processed + index} of {local_rowcount} rows processed')
                if processed % batch_size == 0 and processed != 0:
                    processed += batch_size
                else:
                    processed += batch_size - 1
            if fix_script:
                local_script += '\ncommit;'
                remote_script += '\ncommit;'
                if local_script:
                    print(f'Run on {self.database.hostname}:{local_script}')
                if remote_script:
                    print(f'Run on {table.database.hostname}:{remote_script}')
        return None
