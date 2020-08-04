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
    def __str__(self):
        return {
            "hostname": self.hostname,
            "port": self.port,
            
        }
    
    def connect(self, username, password, schema='information_schema'):
        cnx = None
        try:
            cnx = mysql.connector.connect(user=username, password=password, host=self.hostname, database=self.schema)
            logger.debug(f'Database {self.schema} on {self.hostname} connected')
            self.username = username
            self.password = password
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                logger.critical("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                logger.critical("Database does not exist")
            else:
                logger.critical(err)
        finally:
            if cnx:
                self.connection = cnx

    def get_version(self):
        if self.is_connected():
            response = self.execute(command="SELECT version()")
            self.version = response["rows"][0]["version()"]
            return self.version
        else:
            logger.error("Connect first!")

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
            # logger.debug('Database is connected. Trying to create cursor')
            try:
                cursor = self.connection.cursor(buffered=True,dictionary=True)
                # logger.debug('Cursor created')
                # logger.debug(f'command: {command}')
                sql = f"{command.strip(';')};"
                # logger.debug(f'sql: "{sql}"')
                timer_start = datetime.now()
                cursor.execute(sql)
                timer_end = datetime.now()
                timer_elapsed = timer_end - timer_start
                # logger.debug('Command executed')
                resultset = {
                    'rows': []
                }
                if command.upper().find("SELECT") == 0:
                    rows = cursor.fetchall()
                    # logger.debug(f'Fetched {cursor.rowcount} rows')
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
                elif command.upper().find("SHOW") == 0:
                    rows = cursor.fetchall()
                    logger.debug(f'Fetched {cursor.rowcount} rows')
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
            logger.debug(f'Schema {schema_name} found. Returning {result}')
            return result
        else:
            logger.debug(f'Schema {schema_name} not found. Returning None')
            return None
    
    ## User Methods
    def get_user_by_name(self, username):
        response = []
        result = self.execute(f"SELECT user, host FROM mysql.user WHERE user = '{username}'")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                response.append(User(database=self, username=row["user"].decode(), host=row["host"].decode()))
        return response

    def get_user_by_name_host(self, username, host):
        result = self.execute(f"SELECT user, host FROM mysql.user WHERE user = '{username}' AND host = '{host}'")
        if len(result["rows"]) > 0:
            return User(database=self, username=result["rows"][0]["user"].decode(), host=result["rows"][0]["host"].decode())

    # def get_users(self):
    #     return self.execute(f"SELECT user, host FROM mysql.user;")

    def dump(self):
        return None

    # Flush Privileges
    def flush_privileges(self):
        return self.execute(command = "FLUSH PRIVILEGES;")

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
            tables[f"{row['table_name']}"] = {
                'schema_name': row['schema_name'],
                'table_name': row['table_name'],
                'full_name': f"{row['schema_name']}.{row['table_name']}",
                'table_type': row['table_type'],
                'table_rows': row['table_rows'],
                'avg_row_length': row['avg_row_length'],
                'max_data_length': row['max_data_length']
            }
        # logger.debug(f"Tables is: {tables}")
        for table_name in tables.keys():
            table = Table(schema = self, name=table_name)
            tables[table_name]['columns'] = table.get_columns()
        return tables
    
    def get_table(self, table_name):
        result = self.database.execute(f"SELECT table_schema AS schema_name, table_name, table_type, table_rows, avg_row_length, max_data_length FROM information_schema.tables WHERE table_schema = '{self.name}' AND table_name = '{table_name}' ORDER by 1,2")
        table = {}
        if len(result) > 0:
            table[f"{result['table_name']}"] = result[0]
        # logger.debug(f"Table is: {table}")
        table_obj = Table(self, table_name)
        table['columns'] = table_obj.get_columns()
        return table

    def compare(self, schema, gen_fix_script=False):
        # Check there is a valid connection in both databases
        logger.debug(f'Checking connectivity to {self.database.hostname} and {schema.database.hostname}')
        if self.database.is_connected() and schema.database.is_connected():
        # Check that the schema exists in both databases
            logger.debug('Creating schema objects for both databases')
            local_schema = self
            remote_schema = schema
            if remote_schema.name != 'NotFound':
                logger.debug(f'Remote Schema is: {remote_schema.name}')
                # Get colunms definitions and compare
                local_schema.load_tables()
                # logger.debug(f'Local Schema Tables: {local_schema.tables}')
                remote_schema.load_tables()
                # logger.debug(f'Remote Schema Tables: {remote_schema.tables}')
                diff_dict = {
                    'differences': [],
                    'fix_commands': []
                }
                for table_entry in local_schema.tables.keys():
                    if table_entry in remote_schema.tables.keys():
                        logger.debug(f"Checking table {table_entry}")
                        for column in local_schema.tables[table_entry]['columns'].keys():
                            if column in remote_schema.tables[table_entry]['columns'].keys():
                                # logger.debug(f"Checking column {column}")
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
        # logger.debug(f"Table is: {table_name}")
        result = self.database.execute(f"SELECT column_name, ordinal_position, column_default, is_nullable, data_type, column_type, character_set_name, collation_name FROM information_schema.columns WHERE table_schema = '{self.schema.name}' AND table_name = '{self.name}' ORDER BY ordinal_position")
        column_dict = {}
        for column in result['rows']:
            # logger.debug(column)
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

class User:
    def __init__(self, database: Database, username, host = '%', password = None):
        result = database.execute(command=f"SELECT * FROM mysql.user WHERE user = '{username}' and host = '{host}';")
        self.database = database
        self.roles = []
        self.grants = []
        if len(result["rows"]) == 0:
            self.username = username
            self.host = host
            self.password = password
            self.exists = False
        elif len(result["rows"]) == 1:
            if type(result["rows"][0]["User"]) is bytearray:
                self.username = result["rows"][0]["User"].decode()
            else:
                self.username = result["rows"][0]["User"]
            if type(result["rows"][0]["Host"]) is bytearray:
                self.host = result["rows"][0]["Host"].decode()
            else:
                self.host = result["rows"][0]["Host"]
            if password is None:
                if type(result["rows"][0]["Password"]) is bytearray:
                    self.password = result["rows"][0]["Password"].decode()
                else:
                    self.password = result["rows"][0]["Password"]
            else:
                self.password = password
            if password is None:
                if type(result["rows"][0]["authentication_string"]) is bytearray:
                    self.auth_string = result["rows"][0]["authentication_string"].decode()
                else:
                    self.auth_string = result["rows"][0]["authentication_string"]
            else:
                self.password = password
            self.ssl_type = result["rows"][0]["ssl_type"]
            if type(result["rows"][0]["ssl_cipher"]) is bytearray:
                self.ssl_cipher = result["rows"][0]["ssl_cipher"].decode()
            else:
                self.ssl_cipher = result["rows"][0]["ssl_cipher"]
            if type(result["rows"][0]["x509_issuer"]) is bytearray:
                self.x509_issuer = result["rows"][0]["x509_issuer"].decode()
            else:
                self.x509_issuer = result["rows"][0]["x509_issuer"]
            if type(result["rows"][0]["x509_subject"]) is bytearray:
                self.x509_subject = result["rows"][0]["x509_subject"].decode()
            else:
                self.x509_subject = result["rows"][0]["x509_subject"]
            if type(result["rows"][0]["plugin"]) is bytearray:
                self.plugin = result["rows"][0]["plugin"].decode()
            else:
                self.plugin = result["rows"][0]["plugin"]
            self.select_priv = result["rows"][0]["Select_priv"]
            self.insert_priv = result["rows"][0]["Insert_priv"]
            self.update_priv = result["rows"][0]["Update_priv"]
            self.delete_priv = result["rows"][0]["Delete_priv"]
            self.create_priv = result["rows"][0]["Create_priv"]
            self.drop_priv = result["rows"][0]["Drop_priv"]
            self.reload_priv = result["rows"][0]["Reload_priv"]
            self.shutdown_priv = result["rows"][0]["Shutdown_priv"]
            self.process_priv = result["rows"][0]["Process_priv"]
            self.file_priv = result["rows"][0]["File_priv"]
            self.grant_priv = result["rows"][0]["Grant_priv"]
            self.references_priv = result["rows"][0]["References_priv"]
            self.index_priv = result["rows"][0]["Index_priv"]
            self.alter_priv = result["rows"][0]["Alter_priv"]
            self.show_db_priv = result["rows"][0]["Show_db_priv"]
            self.super_priv = result["rows"][0]["Super_priv"]
            self.create_tmp_table_priv = result["rows"][0]["Create_tmp_table_priv"]
            self.lock_tables_priv = result["rows"][0]["Lock_tables_priv"]
            self.execute_priv = result["rows"][0]["Execute_priv"]
            self.repl_slave_priv = result["rows"][0]["Repl_slave_priv"]
            self.repl_client_priv = result["rows"][0]["Repl_client_priv"]
            self.create_view_priv = result["rows"][0]["Create_view_priv"]
            self.show_view_priv = result["rows"][0]["Show_view_priv"]
            self.create_routine_priv = result["rows"][0]["Create_routine_priv"]
            self.alter_routine_priv = result["rows"][0]["Alter_routine_priv"]
            self.create_user_priv = result["rows"][0]["Create_user_priv"]
            self.event_priv = result["rows"][0]["Event_priv"]
            self.trigger_priv = result["rows"][0]["Trigger_priv"]
            self.create_tablespace_priv = result["rows"][0]["Create_tablespace_priv"]
            self.max_questions = result["rows"][0]["max_questions"]
            self.max_updates = result["rows"][0]["max_updates"]
            self.max_connections = result["rows"][0]["max_connections"]
            self.max_user_connections = result["rows"][0]["max_user_connections"]
            # Get roles
            self.roles = []
            # Get grants
            self.grants = []
            self.get_grants()
            self.exists = True
        else:
            logger.warning(f'{len(result["rows"])} results found. Please modify your search to get only 1 user.')
    
    def __str__(self):
        return {
            "username": self.username,
            "host": self.host,
            "roles": self.roles,
            "grants": self.grants
        }

    def check(self):
        response = self.database.get_user_by_name_host(username=self.username, host= self.host)
        if response.exists:
            self.exists = True

    def get_grants(self):
        result = self.database.execute(f"SHOW GRANTS FOR '{self.username}'@'{self.host}'")
        if len(result["rows"]) > 0:
            for row in result["rows"]:
                self.grants.append(row[f"Grants for {self.username}@{self.host}"])
        else:
            logger.warning("No grants found!")

    def create(self):
        response = {
            "rows": []
        }
        if self.exists:
            logger.info("User already exists, UPDATING instead")
            self.update()
        else:
            # Create user
            sql = f"CREATE USER '{self.username}'@'{self.host}' IDENTIFIED BY '{self.password}'"
            logger.debug(f"SQL is: {sql}")
            response["rows"].append(self.database.execute(sql))
            self.check()
            # Roles
            for role in self.roles:
                sql = f"GRANT {role} TO {self.username}@'{self.host}'"
                response["rows"].append(self.database.execute(sql))
            # Grants
            for grant in self.grants:
                sql = f"GRANT {grant['privilege']} ON {grant['table']} TO {self.username}@'{self.host}'"
                response["rows"].append(self.database.execute(sql))
            # Flush Privileges
            self.database.flush_privileges()
            

    def drop(self):
        if self.exists:
            # Drop user
            sql = f"DROP USER {self.username}@'{self.host}'"
            result = self.database.execute(sql)
            self.exists = False
            return result

    def update(self):
        response = {
            "rows": []
        }
        if self.exists:
            # Update password
            if self.password[0] == '*' and len(self.password) == 41:
                sql = f"SET PASSWORD FOR '{self.username}'@'{self.host}' = '{self.password}'"
            else:
                sql = f"SET PASSWORD FOR '{self.username}'@'{self.host}' = PASSWORD('{self.password}')"
            logger.debug(f"SQL is: {sql}")
            response["rows"].append(self.database.execute(sql))
            self.check()
            # Roles
            for role in self.roles:
                sql = f"GRANT {role} TO {self.username}@'{self.host}'"
                response["rows"].append(self.database.execute(sql))
            # Grants
            for grant in self.grants:
                sql = f"GRANT {grant['privilege']} ON {grant['table']} TO {self.username}@'{self.host}'"
                response["rows"].append(self.database.execute(sql))
            # Flush Privileges
            self.database.flush_privileges()
        else:
            logger.info("User doesn't exists, CREATING instead")
            self.create()