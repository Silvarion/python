########
#
# Filename: couchdblib.py
# Author: Jesus Alejandro Sanchez Davila
# Name: couchdblib
#
# Description: Library for CouchDB v2.x.x
#
##########

# Imports
import http.client
from inspect import currentframe, getframeinfo
import json
import logging
from urllib3 import make_headers
import uuid
try:
    import requests
    requests_module = True
except ModuleNotFoundError as err:
    print("WARNING::Import Section: Module 'requests' not found, falling back to 'http.client'")
    requests_module = False

MASTER_LOG_LEVEL = logging.DEBUG

def get_linenumber():
    cf = currentframe()
    return cf.f_back.f_lineno

# API Endpoint Interaction
def endpoint_api(object, endpoint, headers={}, data={}, json_data={}, method='GET', admin=False, compatibility=False):
    logger = logging.getLogger('endpoint_api')
    endpoint_url = f"{object.url}{endpoint}"
    logger.debug(f"[{get_linenumber()}] Endpoint URL: {endpoint_url}")
    logger.debug(f"[{get_linenumber()}] Method: {method}")
    logger.debug(f"[{get_linenumber()}] Headers: {headers}")
    if len(headers.keys()) == 0:
        logger.debug(f"[{get_linenumber()}] Empty header detected, using default")
        default_header = True
    else:
        default_header = False
        logger.debug(f"[{get_linenumber()}] Headers: {json.dumps(headers, indent=2)}")
    if data is None:
        logger.debug(f"[{get_linenumber()}] Empty data detected")
    else:
        logger.debug(f"[{get_linenumber()}] Data: {data}")
    if json_data is None:
        logger.debug(f"[{get_linenumber()}] Empty json detected")
    else:
        logger.debug(f"[{get_linenumber()}] Data: {json.dumps(json_data, indent=2)}")

    # Set credentials
    if type(object) is Server:
        creds = (object.username,object.password)
    elif type(object) is Database:
        creds = (object.server.username,object.server.password)
    elif type(object) is Node:
        creds = (object.server.username,object.server.password)
    elif type(object) is Document:
        creds = (object.database.server.username,object.database.server.password)
    logger.debug(f"[{get_linenumber()}] Checking which module to use")
    if requests_module: ## When the requests module is found
        try:
            if admin:
                logger.debug(f"[{get_linenumber()}] Using admin mode")
                if type(object) is Server:
                    headers["Host"] = object.admin_host
                    headers["Referer"] = f"http://{object.admin_host}"
                    headers["Referer"] = object.admin_url
                elif type(object) is Database:
                    headers["Host"] = object.server.admin_host
                    headers["Referer"] = f"http://{object.server.admin_host}"
                    headers["Referer"] = object.server.admin_url
                elif type(object) is Document:
                    headers["Host"] = object.database.server.admin_host
                    headers["Referer"] = f"http://{object.database.server.admin_host}"
                    headers["Referer"] = object.database.server.admin_url
            else:
                if type(object) is Server:
                    headers["Host"] = object.couchdb_host
                    headers["Referer"] = f"http://{object.couchdb_host}"
                    headers["Referer"] = object.url
                elif type(object) is Database:
                    headers["Host"] = object.server.couchdb_host
                    headers["Referer"] = f"http://{object.server.couchdb_host}"
                    headers["Referer"] = object.server.url
                elif type(object) is Document:
                    headers["Host"] = object.database.server.couchdb_host
                    headers["Referer"] = f"http://{object.database.server.couchdb_host}"
                    # headers["Referer"] = object.database.server.url
            if default_header:
                headers["accept"] = "application/json"
            logger.debug(f"[{get_linenumber()}] Final header:\n{headers}")
            if method.upper() == 'GET':
                response = requests.get(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'PUT':
                response = requests.put(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'POST':
                response = requests.post(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            elif method.upper() == 'DELETE':
                response = requests.delete(url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
            else:
                response = requests.request(method=method.upper(), url=endpoint_url, headers=headers, auth=creds, json=json_data, data=data)
        except requests.HTTPError as he:
            logger.warning('HTTPError while trying the connection')
            logger.debug(he)
            response = {
                'status': 'error',
                'headers': {}
            }
        except Exception as e:
            logger.critical(e.__str__())
            response = {
                'status': 'error',
                'fullerror': e.__str__()
            }
        finally:            
            if requests_module:
                logger.debug(f"Crude response >>> \n{response}")  
                if "json" in dir(response):
                    return response.json()
                else:
                    return response.raw.read().decode()
            else:
                return response
    else: ## Falback to http.client module
        # Split Endpoint URL into base + path
        host_end = endpoint_url.find("/",7)
        url_base = endpoint_url[:host_end].split("/")[2]
        url_path = endpoint_url[host_end:]
        logger.debug(f"[{get_linenumber()}] URL Base: {url_base}\nURL Path: {url_path}")
        try:
            logger.debug(f"[{get_linenumber()}] Preparing request using http.client")
            http_conn = http.client.HTTPConnection(host=url_base)
            # http_conn.set_debuglevel(100)
            headers_auth = make_headers(basic_auth=f"{creds[0]}:{creds[1]}")
            if admin:
                logger.debug(f"[{get_linenumber()}] Using admin mode")
                if type(object) is Server:
                    headers["Host"]=object.admin_host
                    headers["Referer"]=f"http://{object.admin_host}"                    
                elif type(object) is Database:
                    headers["Host"]=object.server.admin_host
                    headers["Referer"]=f"http://{object.server.admin_host}"                    
                elif type(object) is Document:
                    headers["Host"]=object.database.server.admin_host
                    headers["Referer"]=f"http://{object.database.server.admin_host}"                    
            else:
                if type(object) is Server:
                    headers["Host"]=object.couchdb_host
                    headers["Referer"]=f"http://{object.couchdb_host}"                    
                elif type(object) is Database:
                    headers["Host"]=object.server.couchdb_host
                    headers["Referer"]=f"http://{object.server.couchdb_host}"                    
                elif type(object) is Document:
                    headers["Host"]=object.database.server.couchdb_host
                    headers["Referer"]=f"http://{object.database.server.couchdb_host}"                    
            if default_header:
                headers["accept"] = "application/json"
            headers["authorization"] = headers_auth["authorization"]
            logger.debug(f"[{get_linenumber()}] Final header:\n{headers}")
            logger.debug(f"[{get_linenumber()}] Connection attempt: {http_conn.connect()}")
            if data:
                http_conn.request(method=method.upper(), url=url_path, body=data, headers=headers)    
            elif json_data:
                http_conn.request(method=method.upper(), url=url_path, body=bytes(json.dumps(json_data), 'utf-8'), headers=headers)
            else:
                http_conn.request(method=method.upper(), url=url_path, headers=headers)
            response = json.loads(http_conn.getresponse().read().decode())
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            response = {
                "status": f"{response.status}",
                "reason": f"{response.reason}",
                "content": str(he)
            }
        except Exception as e:
            logger.critical(e.__str__())
            response = {
                'status': 'error',
                'fullerror': e.__str__()
            }
        finally:
            logger.debug(response)
            if requests_module:
                if response.json():
                    return response.json()
                else:
                    return response.raw.read().decode()
            else:
                return response


# Server Class - As in a CouchDB Instance/Cluster
class Server(object):
    # Initialization
    def __init__(self, hostname, port=5984, admin_port=5986, username="", password="", compatibility=False, log_level=MASTER_LOG_LEVEL):
        logger = logging.getLogger('Server::__init__')
        logging.basicConfig(level=log_level)
        logger.debug('Initializing static variables')
        self.hostname = hostname
        self.admin_port = admin_port
        self.port = str(port)
        self.username = username
        self.password = password
        self.couchdb_host = f"{self.hostname}:{self.port}"
        self.admin_host = f"{self.hostname}:{self.admin_port}"
        self.url = f'http://{self.couchdb_host}/'
        self.admin_url = f'http://{self.admin_host}/'
        self.compatible=compatibility
        try:
            response = endpoint_api(object=self, endpoint="")
            logger.debug(f"Response from connection: {response}")
            if "error" not in response.keys():
                logger.debug(f"Response:\n{json.dumps(response,indent=2)}")
                if "version" in response.keys():
                    self.version = response['version']
                if "features" in response.keys():
                    self.features = response['features']
                if "vendor" in response.keys():
                    self.vendor = response['vendor']['name']
                if "all_nodes" in response.keys():
                    self.all_nodes = self.membership()['all_nodes']
                if "version" in dir(self):
                    logger.info(f'Connected to CouchDB v{self.version} instance on {self.hostname}')
                else:
                    logger.info(f'Connected to CouchDB instance on {self.hostname}')
            else:
                logger.info(f'Error connecting to CouchDB:\n{json.dumps(response,indent=2)}')
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            response = {
                "status": "error",
                "content": str(he)
            }

    # Refresh connection
    def refresh_connection(self):
        self.__init__(hostname=self.hostname, port=self.port, admin_port=self.admin_port, username=self.username, password=self.password)

    # API Endpoint Interaction
    def endpoint(self, endpoint, headers={}, data=None, json_data=None, method='GET', admin=False):
        logger = logging.getLogger('Server::endpoint')
        logger.debug('Calling main endpoint function')
        # self.refresh_connection()
        return endpoint_api(self, endpoint=endpoint, headers=headers, data=data, json_data=json_data, method=method,admin=admin,compatibility=self.compatible)

    # Active tasks
    def active_tasks(self):
        logger = logging.getLogger('Server::active_tasks')
        logger.debug('Querying /_active_tasks')
        # self.refresh_connection()
        return self.endpoint(endpoint='_active_tasks')

    # All DBs
    def all_dbs(self):
        logger = logging.getLogger('Server::all_dbs')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_all_dbs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_all_dbs', headers=headers)

    # DBs Info
    def dbs_info(self, dbs_list = None):
        logger = logging.getLogger('Server::dbs_info')
        if dbs_list is None:
            logger.debug("No dbs_list provided")
            dbs = self.all_dbs()
            logger.debug(f"List from the server itself {dbs}")
        elif type(dbs_list) is str:
            logger.debug(f"String list provided")
            dbs = dbs_list.split(',')
            if len(dbs) == 1:
                dbs = dbs_list.split(' ')
            logger.debug(f"DBs list: {dbs}")
        else:
            dbs = dbs_list
            logger.debug(f"DBs list: {dbs}")
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        json_data = {
            'keys': dbs
        }
        logger.debug('Querying _dbs_info')
        # self.refresh_connection()
        return self.endpoint(endpoint='_dbs_info', headers=headers, json_data=json_data, method='POST')

    # Cluster Setup Status
    def cluster_setup_status(self, username, password):
        logger = logging.getLogger('Server::cluster_setup_status')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_cluster_setup')
        # self.refresh_connection()
        return self.endpoint(endpoint='_cluster_setup', headers=headers)

    def setup_cluster(self, username=None, password=None, seed_list = []):
        logger = logging.getLogger('Server::setup_cluster')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Checking cluster setup status')
        # current = self.cluster_setup_status(username,password)
        results = {
            "status": "200",
            "nodes": {}
        }
        gc_db = Database(server = self, name = "_global_changes")
        if not gc_db.exists:
            rsp = gc_db.create()
            print(rsp)
            users_db = Database(server = self, name = "_users")
            if not users_db.exists:
                rsp = users_db.create()
                print(rsp)
            repl_db = Database(server = self, name = "_replicator")
            if not repl_db.exists:
                rsp = repl_db.create()
                print(rsp)
            json_data = {
                "action": "enable_cluster",
                "bind_address": "0.0.0.0",
                "username": username,
                "password": password,
                "node_count": f"{len(seed_list)}"
            }
            print(json.dumps(json_data,indent=2))
            setup_state = self.endpoint(endpoint="_cluster_setup", headers=headers, json_data=json_data,method="POST")
            logger.debug(setup_state)
            for node in seed_list:
                if type(node) is Node:
                    json_data = {
                        "action": "add_node",
                        "host": node.name,
                        "port": self.port,
                        "username": username,
                        "password": password,
                    }
                elif type(node) is str:
                    json_data = {
                        "action": "add_node",
                        "host": node,
                        "port": 5984,
                        "username": username,
                        "password": password,
                    }
                results["nodes"][node] = self.endpoint(endpoint="", headers=headers, json_data=json_data, method="POST")
                logger.debug(results["nodes"][node])
            ## Finish the cluster setup
            setup_state = self.endpoint(endpoint="_cluster_setup", headers=headers, json_data={ "action": "finish_cluster"},method="POST")
        else:
            for node in seed_list:
                results["nodes"][node] = self.add_node(node.name.split('@')[1],port=self.port, id="couchdb")

    def create_initial_dbs(self):
        db = Database(server = self, name = "_users")
        if not db.exists:
            db.create()
        db = Database(server = self, name = "_replicator")
        if not db.exists:
            db.create()
        db = Database(server = self, name = "_global_changes")
        if not db.exists:
            db.create()

    # Server User/Admin Addition/Removal
    def add_user(self, username, password, roles = []):
        payload = {
            "name": username,
            "password": password,
            "roles": roles,
            "type": "user"
        }
        self.endpoint(endpoint=f"/_users/org.couchdb.user:{username}", json_data=payload, method="PUT")

    def delete_user(self, username):
        logger = logging.getLogger("Server::delete_user")
        users = Database(server=self, name="_users")
        to_drop = Document(database=users,doc_id=f"org.couchdb.user:{username}")
        if to_drop.exists:
            to_drop.delete()
        else:
            logger.error("User does not exist. Nothing to do")

    # Node Addition/Removal
    def add_node(self, hostname, port = 5984, id = "couchdb"):
        logger = logging.getLogger('Server::add_nodes')
        node_identifier = f"{id}@{hostname}"
        # self.refresh_connection()
        result = self.membership()
        if "all_nodes" in result.keys():            
            if node_identifier in result['all_nodes']:
                logger.warning(f"{node_identifier} is already a member of the cluster")
                response = {
                    "status": "error",
                    "errcode": "400",
                    "body": {
                        "node": node_identifier,
                        "message": "Node already registered"
                    }
                }
            else:
                response = self.endpoint(endpoint = f"_nodes/{node_identifier}", method="PUT", admin=True, data={})
        else:
            response = {
                "status": "error",
                "errcode": "500",
                "body": {
                    "node": node_identifier,
                    "message": "Error trying to get membership"
                }
            }
        return response

    def remove_node(self, hostname, port = 5984, id = "couchdb"):
        None

    # Sync all 
    def sync_all_shards(self):
        logger = logging.getLogger("Server::sync_all_shards")
        result = {
            "processed": 0,
            "rows": {}
        }
        db_list = self.all_dbs()
        logger.debug(f"dblist type is {type(db_list)}")
        if type(db_list) is list:
            for item in db_list:
                logger.info(f"Syncing shards for {item}")
                result["processed"] += 1
                db = Database(server=self, name=item)
                result["rows"][item] = db.sync_shards()
            return result
        else:
            logger.error("Invalid List!!")
            return db_list

    # Compact all
    def compact_all(self):
        result = {
            "processed": 0,
            "rows": {}
        }
        db_list = self.all_dbs()
        if "error" not in db_list.keys():
            for item in db_list:
                result["processed"] += 1
                db = Database(server=self, name=item)
                result["rows"][item] = db.compact()
            return result
        else:
            return db_list

    # DB Updates
    def db_updates(self):
        logger = logging.getLogger('Server::db_updates')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_db_updates')
        # self.refresh_connection()
        return self.endpoint(endpoint='_db_updates', headers=headers)

    # Cluster Membership
    def membership(self):
        logger = logging.getLogger('Server::membership')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_membership')
        # self.refresh_connection()
        return endpoint_api(self, endpoint='_membership', headers=headers)

    # Scheduler Jobs
    def scheduler_jobs(self):
        logger = logging.getLogger('Server::scheduler_jobs')
        headers = {
            'Accept': 'application/json'
        }
        logger.debug('Querying /_scheduler/jobs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_scheduler/jobs', headers=headers)

    # Scheduler Docs
    def scheduler_docs(self):
        logger = logging.getLogger('Server::scheduler_docs')
        headers = {
            'Accept': 'application/json'
        }
        logger.debug('Querying /_scheduler/docs')
        # self.refresh_connection()
        return self.endpoint(endpoint='_scheduler/docs', headers=headers)

    # Up
    def up(self):
        logger = logging.getLogger('Server::up')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_up')
        # self.refresh_connection()
        return self.endpoint(endpoint='_up', headers=headers)

    # UUIDs
    def uuids(self):
        logger = logging.getLogger('Server::uuids')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Querying /_uuids')
        # self.refresh_connection()
        return self.endpoint(endpoint='_uuids', headers=headers)

# Node Class
class Node(object):
    # Initialization
    def __init__(self, server, name):
        logger = logging.getLogger('Node::__init__')
        logger.info('Checking node name against cluster nodes list')
        self.name = name
        self.url = server.url + '_node/' + name + '/'
        # if server.urlopener:
        #     self.urlopener = server.urlopener
        self.server = server

    # Node Config
    def config(self, section=None, key=None, data=None, method='GET'):
        logger = logging.getLogger('Node::config')
        logger.debug("Building endpoint")
        endpoint = '_config'
        if section:
            endpoint = endpoint + '/' + section
            if key:
                endpoint = endpoint + '/' + key
            headers = {
                'Accept': 'application/json'
            }
        return endpoint_api(self, endpoint=endpoint, headers=headers, data=data, method=method.upper())

    # Node Stats
    def stats(self):
        logger = logging.getLogger('Node::stats')
        logger.debug("Building endpoint")
        endpoint = '_stats'
        return endpoint_api(self, endpoint=endpoint)

    # Node System
    def system(self):
        logger = logging.getLogger('Node::system')
        logger.debug("Building endpoint")
        endpoint = '_system'
        return endpoint_api(self, endpoint=endpoint)

# Database Class
class Database(object):
    # Initialization
    def __init__(self, server, name):
        logger = logging.getLogger('Database::__init__')
        logger.debug('Initializing Database object')
        self.server = server
        self.name = name
        self.url = f"{self.server.url}{name}/"
        # if "urlopener" in dir(server):
        #     self.urlopener = server.urlopener
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        try:
            logger.debug('Looking for the database')
            resp = endpoint_api(self, endpoint='', headers=headers)
            logger.debug('Response from server: ' + json.dumps(resp, indent=2))
        except http.client.HTTPException as he:
            logger.error('HTTPException while trying the connection')
            resp = {
                "status": "error",
                "content": str(he)
            }
        except Exception as ue:
            resp = {
                'status': 'error',
                'content': ue
            }
        logger.debug("Finishing initialization")
        if 'db_name' in resp.keys():
            logger.debug("Database found!")
            self.name = resp['db_name']
            self.exists = True
        else:
            logger.debug("Database NOT found!")
            self.name = name
            self.exists = False

    def __str__(self):
        return self.url

    # Creates a non-existent database
    def create(self):
        logger = logging.getLogger('Database::create')
        logger.debug('Cheking if DB exists')
        if self.exists:
            logger.warning('Database exists already, no need to create it')
        else:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            response = endpoint_api(self, endpoint='', headers=headers, method='PUT')
            if "ok" in response.keys():
                if response["ok"]:
                    self.exists = True
            return response

    # Deletes existing database
    def delete(self):
        logger = logging.getLogger('Database::delete')
        logger.debug('Cheking if DB exists')
        if not self.exists:
            logger.warning('Database does not exist, no need to delete it')
        else:
            headers = {
                'Accept': 'application/json',
                'Content-Type': 'application/json',
            }
            response = endpoint_api(self, endpoint='', headers=headers, method='DELETE')
            if "ok" in response.keys():
                if response["ok"]:
                    self.exists = False
            return response

    def all_docs(self):
        return endpoint_api(self, endpoint="_all_docs")

    def delete_all_docs(self):
        result = self.all_docs()
        if "rows" in result.keys():
            for row in result["rows"]:
                print(row)
                doc = Document(database = self, doc_id = row["id"])
                doc.delete()
        self.purge_all()

    # Returns the change log of the DB
    def changes(self):
        logger = logging.getLogger('Database::changes')
        logger.debug('Building endpoint')
        return endpoint_api(self, endpoint='_changes')

    # Security Data
    def get_security_data(self):
        logger = logging.getLogger("Database::set_security_data")
        resp = self.server.endpoint(endpoint=f"{self.name}/_security", method="GET")
        logger.debug(json.dumps(resp,indent=2))
        return(resp)

    def set_security_data(self, definition):
        logger = logging.getLogger("Database::set_security_data")
        resp = self.server.endpoint(endpoint=f"{self.name}/_security", json_data=definition, method="PUT")
        logger.info(json.dumps(resp,indent=2))
    
    def add_admin_user(self, username):
        logger = logging.getLogger("Database::add_admin_user")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "names" not in sec_data["admins"].keys():
            sec_data["admins"]["names"] = []
        if username not in sec_data["admins"]["names"]:
            sec_data["admins"]["names"].append(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_admin_user(self, username):
        logger = logging.getLogger("Database::remove_admin_user")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "names" not in sec_data["admins"].keys():
            sec_data["admins"]["names"] = []
        if username in sec_data["admins"]["names"]:
            sec_data["admins"]["names"].remove(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{username} is not a member of the admins group")

    def add_admin_role(self, role_name):
        logger = logging.getLogger("Database::add_admin_role")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "roles" not in sec_data["admins"].keys():
            sec_data["admins"]["roles"] = []
        if role_name not in sec_data["admins"]["roles"]:
            sec_data["admins"]["roles"].append(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_admin_role(self, role_name):
        logger = logging.getLogger("Database::remove_admin_role")
        sec_data = self.get_security_data()
        if "admins" not in sec_data.keys():
            sec_data["admins"] = {}
        if "roles" not in sec_data["admins"].keys():
            sec_data["admins"]["roles"] = []
        if role_name in sec_data["admins"]["roles"]:
            sec_data["admins"]["roles"].remove(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{role_name} is not a member of the admins group")

    def add_member_user(self, username):
        logger = logging.getLogger("Database::add_member_user")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "names" not in sec_data["members"].keys():
            sec_data["members"]["names"] = []
        if username not in sec_data["members"]["names"]:
            sec_data["members"]["names"].append(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_member_user(self, username):
        logger = logging.getLogger("Database::remove_member_user")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "names" not in sec_data["members"].keys():
            sec_data["members"]["names"] = []
        if username in sec_data["members"]["names"]:
            sec_data["members"]["names"].remove(username)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{username} is not a member of the members group")

    def add_member_role(self, role_name):
        logger = logging.getLogger("Database::add_member_role")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "roles" not in sec_data["members"].keys():
            sec_data["members"]["roles"] = []
        if role_name not in sec_data["members"]["roles"]:
            sec_data["members"]["roles"].append(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)

    def remove_member_role(self, role_name):
        logger = logging.getLogger("Database::remove_member_role")
        sec_data = self.get_security_data()
        if "members" not in sec_data.keys():
            sec_data["members"] = {}
        if "roles" not in sec_data["members"].keys():
            sec_data["members"]["roles"] = []
        if role_name in sec_data["members"]["roles"]:
            sec_data["members"]["roles"].remove(role_name)
            logger.info("Pushing updated security info")
            self.set_security_data(definition=sec_data)
        else:
            logger.error(f"{role_name} is not part of the members group")

    # Find document by ID
    def find_by_id(self, doc_id):
        logger = logging.getLogger('Database::find_by_id')
        logger.debug('Looking for the doc')
        return Document(self, doc_id=doc_id)

    # Find using the JSON query syntax
    def find(self, query = None):
        logger = logging.getLogger('Database::find')
        logger.debug('Looking for docs matching criteria')
        # self.server.refresh_connection()
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        resp = endpoint_api(self, endpoint='_find',
                            headers=headers, json_data=query, method='POST')
        return resp

    # Bulk Insert
    def bulk_create(self, docs):
        logger = logging.getLogger('Database::create_index')
        logger.debug('Creating headers')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        endpoint = '_bulk_docs'
        return endpoint_api(self, endpoint=endpoint, headers=headers, data=docs, method='POST')

    # Create Index on a Database
    def create_index(self, definition):
        logger = logging.getLogger('Database::create_index')
        logger.debug('Creating headers')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        logger.debug('Checking index definition')
        if 'index' in definition.keys():
            if 'fields' in definition.index.keys():
                if 'name' in definition.keys():
                    if 'type' in definition.keys():
                        if definition['type'] in ['json', 'text']:
                            logger.debug('Index definition looks good!')
                        else:
                            logger.debug(
                                'Wrong type in index definition, must be either json or text')
                    else:
                        logger.debug('Missing type in index definition')
                else:
                    logger.debug('Missing name in index definition')
            else:
                logger.debug('Missing fields in index definition')
        else:
            logger.debug('Missing index in index definition')
        logger.debug('Trying Index Creation')
        return endpoint_api(self, endpoint='_index/', headers=headers, data=definition, method='POST')

    # Create View in Database
    def create_view(self, name, definition):
        logger = logging.getLogger('Database::create_view')
        logger.debug('Creating headers')
        logger.debug('Checking ')
        logger.debug('Trying View Creation')
        view = Document(
            database=self,
            doc_id=f"_design/{name}",
            content=definition
        )
        return(view.create())

    # Purge deleted docs
    def purge_all(self):
        logger = logging.getLogger('Database::purge_all')
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json'
        }
        data = {
            "selector": {
                "deleted": True
            }
        }
        logger.debug("Finding deleted documents")
        changes = endpoint_api(
            object=self, endpoint='_changes', headers=headers, data=data, method='POST')
        logger.debug('Changes: ' + json.dumps(changes, indent=2))
        purge_id = uuid.uuid4().hex
        logger.debug('purge_id: ' + purge_id)
        data = {
            purge_id: []
        }
        latest_change = changes['last_seq']
        for doc in changes['results']:
            if "deleted" in doc.keys():
                if doc['deleted'] and doc['seq'] == latest_change:
                    for change in doc['changes']:
                        data[purge_id].append(change['rev'])
        purge = endpoint_api(object=self, endpoint='_purge',
                             headers=headers, data=data, method='POST')
        return purge

    # Sync Shards
    def sync_shards(self):
        return endpoint_api(object=self, endpoint="_sync_shards", method="POST")

    # Compact DB
    def compact(self):
        return endpoint_api(object=self, endpoint="_sync_shards", method="POST")

# Document Class
class Document(object):
    # Initialization
    def __init__(self, database, doc_id=None, content={}):
        logger = logging.getLogger('Document::__init__')
        logger.debug('Initializing attributes')
        self.id = doc_id
        # self.content = json.dumps(content, ensure_ascii=False)
        self.content = content
        self.database = database
        self.revision = None
        # if "urlopener" in dir(database):
        #     self.urlopener = database.urlopener
        headers = {
            'Accept': 'application/json'
        }
        if self.id:
            self.url = f"{database.url}{self.id}"
            logger.debug('Looking for the document')
            resp = endpoint_api(self, endpoint='', headers=headers)
            logger.debug('Response from server: ' + json.dumps(resp, indent=2))
            logger.debug('Setting Revision')
            if 'status' in resp.keys():
                if resp['status'] == 'error':
                    self.exists = False
            else:
                self.exists = True
                self.revision = resp['_rev']
        else:
            doc_id = uuid.uuid4().hex
            lookup = endpoint_api(
                object=self.database,
                endpoint=doc_id,
                headers=headers,
                method='GET'
            )
            if "id" in lookup.keys():
                while "error" not in lookup.keys():
                    doc_id = uuid.uuid4().hex
                    lookup = endpoint_api(
                        object=self.database,
                        endpoint=doc_id,
                        headers=headers,
                        method='GET'
                    )
            self.id = doc_id
            self.exists = False
            self.url = f"{self.database.url}{self.id}"
            self.content['_id'] = doc_id

    # Returns a string description
    def __str__(self):
        dict_json = {}
        dict_json["_id"] = self.id
        dict_json["database"] = self.database.name
        dict_json["content"] = self.content
        dict_json["revision"] = self.revision
        dict_json["url"] = self.url
        dict_json["exists"] = self.exists
        
        return json.dumps(dict_json,indent=4)

    # Checks and updates existence of a document
    def is_there(self):
        logger = logging.getLogger('Document::create')
        logger.debug("Looking for the document")
        response = endpoint_api(object = self, endpoint = "")
        if "error" in response.keys():
            self.exists = False
            return False
        elif "_id" in response.keys():
            self.exists = True
            return True

    # Creates a non-existent document
    def create(self):
        logger = logging.getLogger('Document::create')
        logger.debug(f"Cheking if document exists at {self.url}")
        self.is_there()
        if self.exists:
            logger.warning('Document exists already, no need to create it')
            return {"status": "error", "errcode": "400", "errmsg": "Document already exists!"}
        else:
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
            response = endpoint_api(self, endpoint='', headers=headers, json_data=self.content, method='PUT')
            if "rev" in response.keys():
                self.revision = response["rev"]
                self.exists = True
            # if response["ok"]:
            logger.debug(f"{json.dumps(response,indent=2)}")
            return response
    
    # Set the revision of the document
    def current_revision(self):
        logger = logging.getLogger('Document::current_revision')
        logger.debug("Getting revision set")
        self.revision = self.database.find_by_id(doc_id = self.id).revision

    # Updates existing document
    def update(self):
        logger = logging.getLogger('Document::update')
        logger.debug(f"Cheking if document exists at {self.url}")
        if self.is_there():
            self.current_revision()
        if self.exists:
            logger.debug(f"Document found!")
            headers = {
                "Accept": "application/json",
                "Content-Type": "application/json",
                'If-Match': self.revision
            }
            response = endpoint_api(self, endpoint='', headers=headers, json_data=self.content, method='PUT')
            # if response["ok"]:
            logger.debug(f"{json.dumps(response,indent=2)}")
            return response
        else:
            logger.warning('Document not found')
            return {"status": "error", "errcode": "400", "errmsg": "Document already exists!"}

    # Deletes existing document
    def delete(self):
        logger = logging.getLogger('Document::create')
        logger.debug('Cheking if document exists')
        self.current_revision()
        if self.is_there():
            if not self.exists:
                logger.warning('Document does not exist, no need to delete it')
                return {"status": "error", "errcode": "400", "errmsg": "Document doesn't exist!"}
            else:
                headers = {
                    'Accept': 'application/json',
                    'If-Match': self.revision
                }
                return endpoint_api(self, endpoint='', headers=headers, method='DELETE')