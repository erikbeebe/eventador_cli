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
import socketio
import yaml

# configure readline support for input
readline.parse_and_bind('tab: complete')
readline.parse_and_bind('set editing-mode vi')

#BASE_URL = "https://eventador.cloud"
BASE_URL = "https://dev-one-use1.console.eventador.io"
WSS_URL = "https://dev-one-use1.console.eventador.io"

print("Loading configuration from config.yaml.. ", end='')
try:
    with open('config.yaml') as f:
        config = yaml.load(f, Loader=yaml.SafeLoader)
except Exception as ex:
    print("failed to load config: {}".format(ex))
    sys.exit(1)
print("done.")

sio = socketio.Client()

@sio.on('kafkacatOutput')
def receive_kafkacat_data(data):
    if 'msg' in data:
        out = json.loads(data['msg'])
        wayout = json.loads(out)
        pprint(wayout)
    else:
        print("Invalid message received in results - skipping.")

@sio.event
def receive_ws_data(data):
    print("NEW MESSAGE ARRIVED: {}".format(data))


class NoCSRFTokenFound(Exception):
    pass

class EventadorQuery(object):
    def __init__(self):
        self.session = requests.session()
        self.csrf_token = None
        self.deploymentid = None
        self.clusters = {}
        self.selected_cluster = None
        self.selected_cluster_name = None

        # SocketIO configuration
        self.sio = sio
        self.sio.eio.http = self.session

    def get_csrf_token(self):
        print("Getting CSRF token..")
        r = self.session.get(BASE_URL + '/login')
        for line in r.iter_lines():
            if b'name="csrf_token"' in line:
                self.csrf_token = str(line.split()[4]).split('=')[1].replace('>','').replace('"','').replace("'",'').strip()
                return
        raise NoCSRFTokenFound()

    def login(self, username, password):
        if not self.csrf_token:
            self.get_csrf_token()

        r = self.session.post(BASE_URL + '/login', data = {'login': username, 'password': password, 'csrf_token': self.csrf_token, 'next': ''})
        print("Login successful, user is: {}".format(username))

    def get_tables(self):
        r = self.session.get(BASE_URL + '/api/v1/sb-source/source')
        raw_sources = r.json()

        tables = []
        for table in raw_sources['data']:
            tables.append(table['table_name'])

        return tables

    def get_jobs(self):
        r = self.session.get(BASE_URL + '/api/v1/sb-source/source')
        raw_sources = r.json()

        tables = []
        for table in raw_sources['data']:
            tables.append(table['table_name'])

        return tables

    def get_clusters(self):
        r = self.session.get(BASE_URL + '/api/v1/deployments')
        raw_deployments = r.json()

        clusters = {}

        for r in raw_deployments['data']:
            name = r['deploymentname']
            clusters[name] = {'name': r['deploymentname'], 'description': r['description'], 'id': r['deploymentid']}

        self.clusters = clusters
        return clusters

    def reserve_jobid(self):
        headers = {'x-csrf-token': self.csrf_token}
        r = self.session.post(BASE_URL + '/api/v1/job/{}'.format(self.selected_cluster), json = {}, headers = headers)
        return r.json()['data']['jobid']

    def run_query(self, payload):
        headers = {'x-csrf-token': self.csrf_token}
        #r = self.session.post(BASE_URL + '/api/v1/sb-run/{}'.format(self.selected_cluster), data = json.dumps(payload, default=str), headers = headers)
        r = self.session.post(BASE_URL + '/api/v1/sb-run/{}'.format(self.selected_cluster), json = payload, headers = headers)
        print("Executed query: {}".format(r.text))
        return r.json()

    def sample_query(self, sink_id):
        self.sio.connect(WSS_URL)
        print("sink id: {}".format(sink_id))
        # need to know the sink id returned from the job start
        room_name = "SQL_RESULTS-" + hashlib.md5(str(sink_id + random.randint(0,2**31-1)).encode('utf-8')).hexdigest()
        self.sio.emit('join', {'room': room_name})

        params = {'sink_id': sink_id,
                  'room': room_name,
                  'deploymentid': self.selected_cluster}
        self.sio.emit('kafkacat_results', params)

        # need to run this every 5 seconds
        self.sio.emit('keepalive', {'sink_id': sink_id})
        self.sio.sleep(10)
        self.sio.wait()

def require_cluster(f):
    @wraps(f)
    def wrapper(*args, **kwds):
        if e.selected_cluster:
            return f(*args, **kwds)
        else:
            print("No cluster selected - pick a cluster first with SET CLUSTER")
            return None
    return wrapper

def query_repl(e):
    try:
        if e.selected_cluster:
            i = input('Eventador SQL[{}]> '.format(e.selected_cluster_name))
        else:
            i = input('Eventador SQL> ')
    except (EOFError, KeyboardInterrupt):
        print("\nGoodbye.")
        sys.exit(1)

    # remove trailing semicolon
    if i.lower().strip().endswith(';'):
        i = i.lower().strip()[:-1]

    if i.startswith('show') or i.startswith('set'):
        handle_show_commands(i)
    elif i.startswith('select'):
        handle_query(i)
    elif 'exit' == i.lower().strip():
        print("Goodbye.")
        sys.exit(1)
    elif 'help' == i.lower().strip():
        print(ev_help())
    elif i.strip() == "":
        # eg. if you press enter with no text
        pass
    else:
        print("Invalid command.  Type help for help.")

def handle_show_commands(cmd):
    cmd = cmd.lower().strip()

    if cmd == 'show tables':
        tables = e.get_tables()
        pprint(tables)
    elif cmd == 'show clusters':
        clusters = e.get_clusters()
        print("%-40s %-40s" % ('Cluster name', 'Description'))
        print("-"*80)
        for c in clusters:
            print("%-40s %-40s" % (clusters[c]['name'], clusters[c]['description']))
        print("")
    elif cmd.startswith('set cluster'):
        if not e.clusters:
            # if we haven't polled for a list of clusters yet,
            # do that now
            e.get_clusters()

        name = cmd.split()[2]
        if name in e.clusters:
            print("Selected SSB cluster: {}".format(name))
            e.selected_cluster = e.clusters[name].get('id')
            e.selected_cluster_name = name
        else:
            print("Invalid cluster.")
    elif cmd == 'show env':
        print("Current cluster: {}".format(e.selected_cluster))
    else:
        print("Invalid command.  Type help for help.")

@require_cluster
def handle_query(query):
    print("Running job with query: {}".format(query))

    jobid = e.reserve_jobid()
    print("jobid: {}".format(jobid))

    base64_encoded_query = base64.b64encode(query.encode('utf-8')).decode('utf-8')

    snapshot = {"is_create":False,
                "name":"erik_testing_mview",
                "retention":300,
                "is_recreate":True,
                "key_column_name":"",
                "is_ignore_nulls":False,
                "require_restart":False}

    payload = {'sql': base64_encoded_query,
               'source': 738,
               'sink': None,
               'job_name': 'erik_testing',
               'sb_version': '6.0.1',
               'restart_strategy': 'never',
               'restart_retry_time': 30,
               'parallelism': 1,
               'snapshot': snapshot,
               'jobid': jobid}

    result = e.run_query(payload)

    print("Waiting for results..")
    e.sample_query(result['data']['ephemeral_sink_id'])

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

e = EventadorQuery()
e.login(config['auth']['username'], config['auth']['password'])

print("Welcome to Eventador.  Enter a command, type 'help' for more information,"
      "or press ctrl-d to exit.")

while True:
    query_repl(e)
