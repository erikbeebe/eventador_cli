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

USE_PYGMENTS = False

try:
    from pygments import highlight
    from pygments.lexers import JsonLexer
    from pygments.formatters import TerminalTrueColorFormatter

    USE_PYGMENTS = True
except:
    pass


class NoCSRFTokenFound(Exception):
    pass

class EventadorQuery(object):
    def __init__(self, base_url, wss_url):
        self.session = requests.session()
        self.csrf_token = None
        self.clusters = {}
        self.selected_cluster = None
        self.selected_cluster_name = None
        self.current_query = {}
        self.room_name = None
        self.base_url = base_url
        self.wss_url = wss_url

        # SocketIO configuration
        self.sio = socketio.Client()
        self.sio.eio.http = self.session

        signal.signal(signal.SIGINT, self.signal_handler)
        self.register_handlers()

        self.stop_output = False

    # Handle user break signal
    def signal_handler(self, sig, frame):
        print("Aborting output.")
        self.stop_output = True
        self.sio.emit('leave', {'room': self.room_name})
        self.sio.disconnect()
        self.kill_query()

    def get_csrf_token(self):
        print("Getting CSRF token..")
        r = self.session.get(self.base_url + '/login')
        for line in r.iter_lines():
            if b'name="csrf_token"' in line:
                self.csrf_token = str(line.split()[4]).split('=')[1].replace('>','').replace('"','').replace("'",'').strip()
                return
        raise NoCSRFTokenFound()

    def login(self, username, password):
        if not self.csrf_token:
            self.get_csrf_token()

        r = self.session.post(self.base_url + '/login', data = {'login': username, 'password': password, 'csrf_token': self.csrf_token, 'next': ''})
        print("Login successful, user is: {}".format(username))

    def get_tables(self):
        r = self.session.get(self.base_url + '/api/v1/sb-source/source')
        raw_sources = r.json()

        tables = []
        for table in raw_sources['data']:
            tables.append(table['table_name'])

        return tables

    def get_jobs(self):
        r = self.session.get(self.base_url + '/api/v1/sb-source/source')
        raw_sources = r.json()

        tables = []
        for table in raw_sources['data']:
            tables.append(table['table_name'])

        return tables

    def get_clusters(self):
        r = self.session.get(self.base_url + '/api/v1/deployments')
        raw_deployments = r.json()

        clusters = {}

        for r in raw_deployments['data']:
            name = r['deploymentname']
            clusters[name] = {'name': r['deploymentname'], 'description': r['description'], 'id': r['deploymentid']}

        self.clusters = clusters
        return clusters

    def reserve_jobid(self):
        headers = {'x-csrf-token': self.csrf_token}
        r = self.session.post(self.base_url + '/api/v1/job/{}'.format(self.selected_cluster), json = {}, headers = headers)
        return r.json()['data']['jobid']

    def run_query(self, payload):
        headers = {'x-csrf-token': self.csrf_token}
        r = self.session.post(self.base_url + '/api/v1/sb-run/{}'.format(self.selected_cluster), json = payload, headers = headers)

        self.current_query = r.json()

        print("Executed query: {}".format(r.text))
        return r.json()

    def kill_query(self, jobid=None):
        headers = {'x-csrf-token': self.csrf_token}

        if not jobid:
            # kill currently running query
            jobid = self.current_query['data']['jobid']

        payload = {'deploymentid': self.selected_cluster, 'jobid': jobid}

        r = self.session.post(self.base_url + '/api/v1/sb_job_cancel', json = payload, headers = headers)

        print("Killed job: {}".format(r.json().get('message')))
        return r.json()

    def sample_query(self, sink_id):
        self.stop_output = False

        self.sio.connect(self.wss_url)
        print("sink id: {}".format(sink_id))
        # need to know the sink id returned from the job start
        self.room_name = "SQL_RESULTS-" + hashlib.md5(str(sink_id + random.randint(0,2**31-1)).encode('utf-8')).hexdigest()
        self.sio.emit('join', {'room': self.room_name})

        params = {'sink_id': sink_id,
                  'room': self.room_name,
                  'deploymentid': self.selected_cluster}
        self.sio.emit('kafkacat_results', params)

        # need to run this every 5 seconds
        self.sio.emit('keepalive', {'sink_id': sink_id})
        self.sio.sleep(10)
        self.sio.wait()

    def register_handlers(self):
        sio = self.sio

        @sio.on('kafkacatOutput')
        def receive_kafkacat_data(data):
            if not self.stop_output:
                if 'msg' in data:
                    out = json.loads(data['msg'])
                    wayout = json.loads(out)
                    if USE_PYGMENTS:
                        print(highlight(
                            json.dumps(wayout, indent=4, sort_keys=True),
                            lexer=JsonLexer(),
                            formatter=TerminalTrueColorFormatter(style="monokai")))
                    else:
                        # default to plain ascii output
                        pprint(wayout)
                else:
                    print("Invalid message received in results - skipping.")

        @sio.event
        def receive_ws_data(data):
            print("NEW MESSAGE ARRIVED: {}".format(data))

    def get_mv_endpoint(self):
        r = self.session.get(self.base_url + '/api/v1/snapper-urlbase/{}'.format(self.selected_cluster))
        return r.json()['data'].get('urlbase')
