#!/usr/bin/python
from __future__ import print_function

from kafka import KafkaConsumer
import yaml
import argparse
import imp
import json
import sys
import socket
import hashlib

# Kafka client is https://github.com/dpkp/kafka-python

parser = argparse.ArgumentParser(description='Process cache relay commands from Kafka')
parser.add_argument('--config', required=True, help='YAML configuration file')
script_args = parser.parse_args()

config_file = open(script_args.config)
config = yaml.safe_load(config_file)

if 'listeners' not in config:
    raise Exception('listeners config required')

topics = []
handlers = {}
for listener in config['listeners']:
    topics.append(listener['topic'])
    fp, pathname, description = imp.find_module(listener['handler'], ['handlers'])
    mod = imp.load_module(listener['handler'], fp, pathname, description)
    klass = getattr(mod, listener['handler'])
    handlers[listener['topic']] = klass(**listener['params'])

if 'server' in config:
    server = config['server']
else:
    server = 'localhost'

client_id = "kafka-watcher-%s-%s" % (socket.gethostname(), hashlib.md5(script_args.config).hexdigest())

print("Connecting to %s as %s" % (server, client_id))

consumer = KafkaConsumer(*topics, bootstrap_servers=server, client_id=client_id)
for msg in consumer:
    if msg.topic not in handlers:
        print("Weird, unknown topic %s" % msg.topic, file=sys.stderr)
        continue
    try:
        data = json.loads(msg.value)
    except ValueError:
        data = None
    if not data:
        print("Could not parse data, meh", file=sys.stderr)
        continue
    try:
        handlers[msg.topic].handle(msg.topic, data)
    except:
        e = sys.exc_info()
        print("Oops, something happened: " + str(e), file=sys.stderr)
