#!/usr/bin/env python3
"""
This is a script put together to pull Impala query statistics from the following queues
etl_app
svc_tbl
svc_tbl_dev
default
"""

__author__ = "phData, Inc"
__version__ = "1.0.0"
__license__ = "ASFv2"

import argparse
from configparser import ConfigParser
import json
import datetime
import copy
import os.path
import numpy as np
import requests
import urllib3
urllib3.disable_warnings()

def to_float_bytes(number):
    return float(number)*1024*1024*1024

def to_gi_bytes(bytes):
    return float(bytes)/(1024*1024*1024)

def main(json_file,queue_max,queue_name):

    csv_headers = ['username',
              'database',
              'startTime',
              'endTime',
              'memory_per_node_peak (GiB)',
              'queryId',
              'stats_missing',
              'stats_corrupt',
              'statement', ]

    nondefaultqueue = ['etl_app', 'svc_tbl', 'svc_tbl_dev']

    queue_max_bytes = to_float_bytes(queue_max)

    total_high_queries = 0

    with open(json_file,'r') as iQueries:
        json_data = json.load(iQueries)

    print(",".join(csv_headers))

    # loop through our queries
    for query in json_data.values():
        # use any queries with memory_per_node_peak stats
        if 'memory_per_node_peak' in query['attributes']:

            if queue_name not in nondefaultqueue:
                if float(query['attributes']['memory_per_node_peak']) > queue_max_bytes and query['user'] not in nondefaultqueue:
                    print('%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
                          (query['user'],
                           query['database'],
                           query['startTime'],
                           query['endTime'],
                           to_gi_bytes(float(query['attributes']['memory_per_node_peak'])),
                           query['queryId'],
                           query['attributes']['stats_missing'],
                           query['attributes']['stats_corrupt'],
                           '\"' + query['statement'].replace('\"', 'QQ') + '\"'))
                    total_high_queries += 1
            else:
                if float(query['attributes']['memory_per_node_peak']) > queue_max_bytes and query['user'] == queue_name:
                    print('%s,%s,%s,%s,%s,%s,%s,%s,%s' % \
                          (query['user'],
                           query['database'],
                           query['startTime'],
                           query['endTime'],
                           to_gi_bytes(float(query['attributes']['memory_per_node_peak'])),
                           query['queryId'],
                           query['attributes']['stats_missing'],
                           query['attributes']['stats_corrupt'],
                           '\"' + query['statement'].replace('\"', 'QQ') + '\"'))
                    total_high_queries += 1
    #print('\nTotal potential problem queries: ' + str(total_high_queries))

    iQueries.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--file", action="store", dest="json_file",
                        required=True, help="path to your json file")
    parser.add_argument("-m", "--qmax", action="store", dest="queue_max",
                        required=True, help="the memory max GB for the queue")
    parser.add_argument("-q", "--queue", action="store", dest="queue_name",
                        required=True, help="the queue to check")
    args = parser.parse_args()

    # Does the json file actually exist?
    if os.path.exists(args.json_file) is False:
        exit('invalid json file')

    if args.queue_name not in ['etl_app', 'svc_tbl', 'svc_tbl_dev', 'default']:
        exit('invalid queue name')

    main(args.json_file,args.queue_max,args.queue_name)
