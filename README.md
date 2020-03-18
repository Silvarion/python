# Python Scripts and Modules

## Categories

### MySQL

#### mysql_wrapper.py
_Description_

This script is meant to run some command or SQL script on 1 or several MySQL databases.
It's good for pushing configuration changes or bulk DDL/DML into several databases.

_Usage_
```
mysql_wrapper.py [-h] -d DBLIST [DBLIST ...] [--schema DBSCHEMA][-P DBPORT] -u DBUSER [-p] [-v][-s SQLSCRIPT | -c SQLCOMMAND]

optional arguments:
  -h, --help            show this help message and exit
  -d DBLIST [DBLIST ...], --databases DBLIST [DBLIST ...]   List of databases to connect to
  --schema DBSCHEMA                                         Schema to connect to
  -P DBPORT, --port DBPORT                                  Port to connect to the databases
  -u DBUSER, --user DBUSER                                  Username to connect to the databases
  -p, --password                                            Username to connect to the databases
  -v, --verbosity                                           Verbosity/Log Level
  -s SQLSCRIPT, --script SQLSCRIPT                          Path to script to run
  -c SQLCOMMAND, --command SQLCOMMAND                       Command to run
```