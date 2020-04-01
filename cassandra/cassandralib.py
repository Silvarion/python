########
#
# Filename: cassandralib.py
# Author: Jesus Alejandro Sanchez Davila
# Name: cassandralib
#
# Description: Library for Cassandra DB v2.x.x
#
##########

# Imports
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
import logging
import json
import getpass

# Instantiate Logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=logging.INFO,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

class Cassandra:
    """Initialization"""
    def __init__(self,nodes: list, port: int = 9042, username = None, password = None, ssl_context = None):
        self.auth_provider = PlainTextAuthProvider(username = username, password = password)
        self.nodes = nodes
        self.port = port
        self.ssl_context = ssl_context
        self.cluster = Cluster(nodes,port,ssl_context)
        self.connected = False
    
    def connect(self, keyspace = None):
        if keyspace is None:
            self.session = self.cluster.connect()
        else:
            self.session = self.cluster.connect(keyspace = keyspace)
            self.keyspace = keyspace
        self.connected = True

    def execute(self, query = None):
        if query is not None:
            query_array = query.replace(","," ").split(" ")
            query_keys = []
            for item in query_array:
                if item.upper() != "SELECT" and len(item) > 0:
                    if item.upper() == "FROM":
                        break
                    else:
                        query_keys.append(item)
            try:
                rows = self.session.execute(query)
                result = {
                    "status": "ok",
                    "rows": []
                    }
                for row in rows:
                    dict_row = {}
                    for key in query_keys:
                        dict_row[key] = row.__getattribute__(key)
                    result["rows"].append(dict_row)
                return result
            except Exception as e:
                logger.critical(f"Critical: {e}")
                result["status"] = "error"
                result["error"] = str(e)
            finally:
                return result
        else:
            logger.warning("No query provided")
            return {
                "rows": [],
                "status": "error",
                "error": "No query provided"
            }
