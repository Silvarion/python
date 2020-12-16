#!/usr/bin/env python3

from argparse import ArgumentParser
import json
import logging
import os.path
from PyInquirer import style_from_dict, Token, prompt
from PyInquirer import Validator, ValidationError
import pyoomysql

# Instantiate Logger
logging.basicConfig(
    format='[%(asctime)s][%(levelname)-8s] %(message)s',
    level=logging.DEBUG,
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger()

### ARGUMENTS ###
parser = ArgumentParser()

parser.add_argument("-d","--databases", dest="targets")

### MENUS ###

## Menu style
style = style_from_dict({
    Token.QuestionMark: "#E91E63 bold",
    Token.Selected: "#673AB7 bold",
    Token.Instruction: "",  # default
    Token.Answer: "#2196f3 bold",
    Token.Question: "",
})

## Main Menu
main_menu_options = [
    {
        "type": "list",
        "name": "Main Menu Option",
        "message": "Main Menu Option: ",
        "choices": [
            {
                "key": "t",
                "name": f"Manage Targets",
                "value": "targets"
            },{
                "key": "d",
                "name": "Manage Databases",
                "value": "databases"
            },{
                "key": "c",
                "name": "Set credentials to connect",
                "value": "credentials"
            },{
                "key": "a",
                "name": "Manage User/Service Accounts",
                "value": "accounts"
            },{
                "key": "r",
                "name": "Run a command or script on multiple targets",
                "value": "run_command"
            },{
                "key": "x",
                "name": "Exit",
                "value": "exit"
            }
        ],
        "filter": lambda val: val.lower()
    }
]

## Credentials Form
credential_form = [
    {
        "type": "input",
        "name": "user",
        "message": "Username"
    },{
        "type": "password",
        "name": "pswd",
        "message": "Password"
    },{
        "type": "input",
        "name": "schema",
        "message": "Database/Schema",
        "default": "information_schema"
    }
]

## Targets Menu
targets_options = [
    {
        "type": "list",
        "name": "Target Action",
        "message": f"Targets Option: ",
        "choices": [
            {
                "key": "a",
                "name": "Add Target",
                "value": "add-target"
            },{
                "key": "r",
                "name": "Remove Target",
                "value": "remove-target"
            },{
                "key": "l",
                "name": "Load Targets from file (<hostname>:<port>)",
                "value": "load-from-file"
            },{
                "key": "e",
                "name": "Load Targets based on environment",
                "value": "load-from-env"
            },{
                "key": "c",
                "name": "Clear Targets",
                "value": "clear-targets"
            },{
                "key": "b",
                "name": "Back",
                "value": "back"
            },{
                "key": "x",
                "name": "Exit",
                "value": "exit"
            }
        ],
        "filter": lambda val: val.lower()
    }
]

## Target Add Form ##
target_form = [
    {
        "type": "input",
        "name": "Hostname",
        "message": "Hostname: ",
        "filter": lambda val: val.lower()
    },
    {
        "type": "input",
        "name": "Port",
        "message": "Port: ",
        "default": "3306"
    }
]

## Target Load from file Form ##
load_targets_form = [
    {
        "type": "list",
        "name": "file_type",
        "message": "File type : ",
        "choices": [
            {
                "key": "j",
                "name": "JSON ( { hostname: <hostname>, port: <port> } )",
                "value": "json"
            },{
                "key": "p",
                "name": "Plain Text ( <hostname>:<port> ) (1 entry per line)",
                "value": "plain-text"
            }
        ]
    },{
        "type": "input",
        "name": "path",
        "message": "Path to file: "
    }
]

## Target Load from env Form ##
load_targets_env_form = [
    {
        "type": "list",
        "name": "environment",
        "message": "Environment: ",
        "choices": [
            {
                "key": "d",
                "name": "Development",
                "value": "dev"
            },{
                "key": "u",
                "name": "UAT",
                "value": "uat"
            },{
                "key": "p",
                "name": "Production",
                "value": "prd"
            },{
                "key": "o",
                "name": "Other",
                "value": "other"
            }
        ]
    },{
        "type": "input",
        "name": "path",
        "message": "Path to file: ",
        "when": lambda answer: answer.get("environment") != "other",
        "default": lambda answer: f"{answer.get('environment')}_targets.json"
    },{
        "type": "input",
        "name": "environment",
        "message": "Please specify: ",
        "when": lambda answer: answer.get("environment") == "other"
    },{
        "type": "input",
        "name": "path",
        "message": "Path to file: ",
        "when": lambda answer: answer.get("environment") == "other"
    }
]

## Databases Menu
databases_options = [
    {
        "type": "list",
        "name": "Database Action",
        "message": "Databases Option: ",
        "choices": [
            {
                "key": "c",
                "name": "Create Database/Schema",
                "value": "create-schema"
            },{
                "key": "d",
                "name": "Drop Database/Schema",
                "value": "drop-schema"
            },{
                "key": "b",
                "name": "Back",
                "value": "back"
            },{
                "key": "x",
                "name": "Exit",
                "value": "exit"
            }
        ],
        "filter": lambda val: val.lower()
    }
]

## Account Menu
account_types = [
    {
        "type": "list",
        "name": "Account Type",
        "message": "Account Type: ",
        "choices": [
            {
                "key": "a",
                "name": "Admin Account",
                "value": "admin-account"
            },{
                "key": "d",
                "name": "Developer Account",
                "value": "dev-account"
            },{
                "key": "q",
                "name": "QA/QC Account",
                "value": "qc-account"
            },{
                "key": "s",
                "name": "Service Account",
                "value": "service-account"
            },{
                "key": "o",
                "name": "Other Account",
                "value": "other-account"
            },{
                "key": "b",
                "name": "Back",
                "value": "back"
            },{
                "key": "x",
                "name": "Exit",
                "value": "exit"
            }
        ],
        "filter": lambda val: val.lower()
    }
]

# Account Actions Menu
account_actions = [
    {
        "type": "list",
        "name": "Account Action",
        "message": "Account Actions: ",
        "choices": [
            {
                "key": "c",
                "name": "Create Account",
                "value": "create-account"
            },{
                "key": "d",
                "name": "Drop Account",
                "value": "drop-account"
            },{
                "key": "m",
                "name": "Modify Account",
                "value": "modify-account"
            },{
                "key": "b",
                "name": "Back",
                "value": "back"
            },{
                "key": "x",
                "name": "Exit",
                "value": "exit"
            }
        ]
    }
]

# Run Command Menu/Form
run_command = [
    {
        "type": "list",
        "name": "run_type",
        "message": "What are you going to run?:",
        "choices": [
            {
                "key": "c",
                "name": "Command",
                "value": "command"
            },{
                "key": "s",
                "name": "Script",
                "value": "script"
            }
        ]
    },{
        "type": "input",
        "name": "command",
        "message": "Command to run: ",
        "when": lambda answer: answer.get("run_type") == "command"
    },{
        "type": "input",
        "name": "path",
        "message": "Path to script: ",
        "when": lambda answer: answer.get("run_type") == "script"
    },{
        "type": "confirm",
        "name": "run_everywhere",
        "message": "Run also in replicas?: "
    }
]

### FUNCTIONS ###
def load_targets(file_path: str, file_type: str):
    logger = logging.getLogger("load_targets")
    logger.debug("Entering load_targets)")
    targets_list = []
    try:
        if file_type.lower() == "json":
            logger.debug("Trying to open JSON file")
            with open(file_path,"r") as targets_file:
                targets_dict = json.load(targets_file)
                for host in targets_dict.keys():
                    targets_list.append({"host": host,"port":targets_dict[host]})
        else:
            logger.debug("Trying to open plan-text file")
            with open(file_path,"r") as targets_file:
                for line in targets_file.readlines():
                    splitted = line.split(":")
                    target = {splitted[0].replace("\n","") : int(("3306" if len(splitted) < 2 else splitted[1]).replace("\n",""))}
                    logger.debug(f"Adding {target}")
                    targets_list.append(target)
    # Handle Exceptions
    except Exception as err:
        print(err)
        logger.warning(err)
    logger.debug(targets_list)
    return targets_list


def print_current_info(db_list, credentials):
    print("==================================")
    print(f"Current targets: {db_list}\nCredentials: {(credentials['user'] or 'None')}/{('*'*len(credentials['pswd'] or 'None'))}")
    print("----------------------------------")

def beautify_output(payload):
    try:
        beautified = json.dumps(payload)
        return beautified
    except:
        return payload

### GLOBAL VARIABLES ###
target_db_list = []

############################################################################
#                                     START                                #
############################################################################
def main():
    answer="Nothing"
    target_db_list = []
    credentials = {
        "user": None,
        "pswd": None
    }
    while answer != "exit":
        # Show Main Menu
        print_current_info(target_db_list, credentials)
        answer = prompt(main_menu_options, style=style)["Main Menu Option"]
        if answer == "targets":
            print_current_info(target_db_list, credentials)
            # Target Menu
            while answer not in ["back", "exit"]:
                print_current_info(target_db_list, credentials)
                answer = prompt(targets_options, style=style)["Target Action"]
                # Target Add
                if answer == "add-target":
                    print_current_info(target_db_list, credentials)
                    answer = prompt(target_form, style=style)
                    target_db_list.append(
                        {
                            "host": answer["Hostname"],
                            "port": answer["Port"],
                            "vendor": "mysql"
                        }
                    )
                    answer = "back"
                # Target Removal
                elif answer == "remove-target":
                    print_current_info(target_db_list, credentials)
                    answer = prompt(target_form, style=style)
                    try:
                        target_db_list.remove(
                            {
                                "host": answer["Hostname"],
                                "port": answer["Port"],
                                "vendor": "mysql"
                            }
                        )
                    except Exception as err:
                        if str(err) == "list.remove(x): x not in list":
                            print("ERROR: Target not in list")
                        # print(f"Error: {error}")
                    answer = "back"
                # Load Targets from File
                elif answer == "load-from-file":
                    print_current_info(target_db_list, credentials)
                    answer = prompt(load_targets_form, style=style)
                    logger.debug(f"Answers: {answer['path']} | {answer['file_type']}")
                    target_db_list = load_targets(file_path = answer["path"], file_type=(answer["file_type"] or 3306))
                    answer = "back"
            # Load Targets from File
                elif answer == "load-from-env":
                    print_current_info(target_db_list, credentials)
                    answer = prompt(load_targets_env_form, style=style)
                    target_db_list = load_targets(file_path = answer["path"], file_type="json")
                    answer = "back"
            # Clear Targets
                elif answer == "clear-targets":
                    print_current_info(target_db_list, credentials)
                    target_db_list = []
                    answer = "back"
        elif answer == "databases":
        # Database/Schema Menu
            if len(target_db_list) > 0:
                while answer not in ["back", "exit"]:
                    print_current_info(target_db_list, credentials)
                    answer = prompt(databases_options, style=style)["Database Action"]
                    answer = "back"
        elif answer == "credentials":
        # Database/Schema Menu
            while answer not in ["back", "exit"]:
                print_current_info(target_db_list, credentials)
                answer = prompt(credential_form, style=style)
                credentials["user"] = answer["user"]
                credentials["pswd"] = answer["pswd"]
                credentials["schema"] = answer["schema"]
                answer = "back"
        elif answer == "accounts":
            while answer not in ["back", "exit"]:
                print_current_info(target_db_list, credentials)
                answer = prompt(account_types, style=style)["Account Type"]
                account_type = answer
                while answer not in ["back", "exit"]:
                    print_current_info(target_db_list, credentials)
                    answer = prompt(account_actions, style=style)["Account Action"]
                    if answer == "back":
                        answer = "account"
                        break
                    elif:
                        
        elif answer == "run_command":
            answer = prompt(run_command, style=style)
            if answer["run_type"] == "command":
                for target in target_db_list:
                    db = pyoomysql.Database(hostname=target["host"], port=target["port"])
                    db.connect(user=credentials["user"],password=credentials["pswd"],log_level=logging.INFO)
                    if db.is_connected():
                        if not db.is_replica() or answer["run_everywhere"]:
                            response = db.execute(command=answer["command"])
                            logger.info(f"Database response:\n{beautify_output(response)}")
                        else:
                            logger.info("Not running here since this is a replica")
                    else:
                        logger.error(f"Couldn't connect to {db.hostname}")
                answer = "back"
            elif answer["run_type"] == "script":
                if os.path.isfile(answer["path"]):
                    with open(answer["path"],"r") as fd:
                        script = fd.read()
                    for target in target_db_list:
                        db = pyoomysql.Database(hostname=target["host"], port=target["port"])
                        db.connect(user=credentials["user"],password=credentials["pswd"],log_level=logging.DEBUG)
                        if not db.is_replica() or answer["run_everywhere"]:
                            response = db.run(script=script)
                            logger.info(f"Database response:\n{beautify_output(response)}")
                        else:
                            logger.info("Not running here since this is a replica")
                else:
                    logger.error(f"File: '{answer['path']}' not found!!!")
                answer = "back"

############################################################################
#                                      END                                 #
############################################################################

if __name__ == '__main__':
    main()