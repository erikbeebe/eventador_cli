#!/usr/bin/env python3

from functools import wraps
from pprint import pprint
import base64
import hashlib
import json
import random
import requests
import sys
import readline
import signal
import socketio
import sys
import time
import yaml

from eventador.query import EventadorQuery
from eventador import parser

# configure readline support for input
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set editing-mode vi')

print("Loading configuration from config.yaml.. ", end='')
try:
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
except Exception as ex:
    print("failed to load config: {}".format(ex))
    sys.exit(1)
print("done.")


def query_repl(e):
    try:
        if e.selected_cluster:
            i = input('Eventador SQL[{}]> '.format(e.selected_cluster_name))
        else:
            i = input('Eventador SQL> ')
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")
        sys.exit(1)

    p.parse(i)


def ev_help():
    """simple help screen."""
    h = """
------------------------------------------------------------------------------
Commands:

show tables
show clusters
set cluster <clusterName>

Or enter any valid SQL query.  eg.

SELECT * FROM airplanes;
------------------------------------------------------------------------------
        """

    return h


# Initializing REPL loop

BASE_URL = config['endpoint']['urlbase']
WSS_URL = config['endpoint']['urlbase']

ev = EventadorQuery(BASE_URL, WSS_URL)

try:
    ev.login(config['auth']['username'], config['auth']['password'])
except NoCSRFTokenFound as ex:
    print("Login failed - no CSRF token retrieved.")
    sys.exit(1)

p = parser.Parser(ev)

print("Welcome to Eventador.  Enter a command, type 'help' for more information,"
      "or press ctrl-d to exit.")

while True:
    try:
        query_repl(p)
    except Exception as ex:
        print("Failed: {}".format(ex))
