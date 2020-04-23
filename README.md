# Eventador_cli

Example of how to use this:

First:

$ pip3 install -r requirements.txt
$ vim config.yaml

.. and set your login and password


```
python@python:/code/eventador_cli$ python3 eventador_cli.py 
Loading configuration from config.yaml.. done.
Getting CSRF token..
Login successful, user is: eventador_erik
Welcome to Eventador.  Enter a command, type 'help' for more information,or press ctrl-d to exit.
Eventador SQL> show clusters
Cluster name                             Description                             
--------------------------------------------------------------------------------
7.0.0-rc-schema_reg                                                              
601-1                                    6.0.1                                   
pre-releasee-183-600-2                   1.8.3 6.0.0 new sqlio image name        
pre-release-183-600                      Testing SSB 1.8.3-6.0.0                 
TestingNewDeploymentErik                 This is a test                          

Eventador SQL> select * from airplanes;
No cluster selected - pick a cluster first with SET CLUSTER
Eventador SQL> select * from airplanes;
No cluster selected - pick a cluster first with SET CLUSTER
Eventador SQL> set 601-1
Invalid command.  Type help for help.
Eventador SQL> set cluster 601-1
Selected SSB cluster: 601-1
Eventador SQL[601-1]> select * from airplanes;
Running job with query: select * from airplanes
jobid: 3100
Executed query: {
  "data": {
    "ephemeral_log_sink_id": 5219, 
    "ephemeral_sink_id": 5218, 
    "jobid": 3100
  }, 
  "message": "success", 
  "sql": null, 
  "ssb_version_log": "SSB version 6.0.1 selected for job.", 
  "status": "Success"
}

Waiting for results..
sink id: 5218
{'altitude': None,
 'counter': '6022',
 'eventTimestamp': '2020-04-23T02:46:27.436Z',
 'flight': '',
 'icao': 'AAEC9C',
 'lat': '',
 'lon': '',
 'msg_type': '8',
 'speed': None,
 'tailnumber': None,
 'timestamp': 1587609987,
 'timestamp_verbose': '2020-04-23 02:46:27.291684',
 'track': '',
 'vr': None}
{'altitude': None,
 'counter': '6028',
 'eventTimestamp': '2020-04-23T02:46:28.477Z',
 'flight': '',
 'icao': 'AAEC9C',
 'lat': '',
 'lon': '',
 'msg_type': '4',
 'speed': 374,
 'tailnumber': None,
 'timestamp': 1587609988,
 'timestamp_verbose': '2020-04-23 02:46:28.286786',
 'track': '295',
 'vr': 0}
```
