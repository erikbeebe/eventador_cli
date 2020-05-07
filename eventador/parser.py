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

class Parser(object):
    def __init__(self, query_object):
        self.q = query_object

    def require_cluster(func):
        @wraps(func)
        def wrapper(self, *args, **kwds):
            if self.q.selected_cluster:
                return func(self, *args, **kwds)
            else:
                print("No cluster selected - pick a cluster first with SET CLUSTER")
                return None
        return wrapper

    @property
    def selected_cluster(self):
        return self.q.selected_cluster

    @property
    def selected_cluster_name(self):
        return self.q.selected_cluster_name

    def parse(self, i):
        # remove trailing semicolon
        if i.lower().strip().endswith(';'):
            i = i.lower().strip()[:-1]

        if i.startswith('show') or i.startswith('set'):
            self.handle_show_commands(i)
        elif i.startswith('select'):
            self.handle_query(i)
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

    def handle_show_commands(self, cmd):
        cmd = cmd.lower().strip()

        if cmd == 'show tables':
            tables = self.q.get_tables()
            pprint(tables)
        elif cmd == 'show clusters':
            clusters = self.q.get_clusters()
            print("%-40s %-40s" % ('Cluster name', 'Description'))
            print("-"*80)
            for c in clusters:
                print("%-40s %-40s" % (clusters[c]['name'], clusters[c]['description']))
            print("")
        elif cmd.startswith('set cluster'):
            if not self.q.clusters:
                # if we haven't polled for a list of clusters yet,
                # do that now
                self.q.get_clusters()

            name = cmd.split()[2]
            if name in self.q.clusters:
                print("Selected SSB cluster: {}".format(name))
                self.q.selected_cluster = self.q.clusters[name].get('id')
                self.q.selected_cluster_name = name
            else:
                print("Invalid cluster.")
            print(self.q.get_mv_endpoint())
        elif cmd == 'show env':
            print("Current cluster: {}".format(self.q.selected_cluster))
        else:
            print("Invalid command.  Type help for help.")

    @require_cluster
    def handle_query(self, query):
        print("Running job with query: {}".format(query))

        jobid = self.q.reserve_jobid()
        print("Starting job id: {}".format(jobid))

        base64_encoded_query = base64.b64encode(query.encode('utf-8')).decode('utf-8')

        job_basename = "api_testjob_" + str(int(time.time()))

        snapshot = {"is_create":False,
                    "name":job_basename + "_mview",
                    "retention":300,
                    "is_recreate":True,
                    "key_column_name":"",
                    "is_ignore_nulls":False,
                    "require_restart":False,
                    "api_key": ""
                    }

        payload = {'sql': base64_encoded_query,
                   'source': 0,
                   'sink': None,
                   'job_name': job_basename,
                   'sb_version': '6.0.1',
                   'restart_strategy': 'never',
                   'restart_retry_time': 30,
                   'parallelism': 1,
                   'snapshot': snapshot
                   }

        result = self.q.run_query(payload)

        print("Waiting for results..")
        self.q.sample_query(result['data']['ephemeral_sink_id'])
